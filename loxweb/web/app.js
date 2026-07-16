// loxweb frontend: a generic, config-driven dashboard engine.
//
// It fetches a view/panel/series description from "api/views" and renders
// whatever that describes with ECharts -- no view is hardcoded. Per panel it
// builds one chart instance: mixed line/bar, up to three y-axes, stacking
// (including a scale:-1 series mirrored below zero), per-series color / interp
// / fill, and client-side "derived" series computed from an arithmetic
// expression over their sibling series.
//
// The pure logic (the expr evaluator, the bucket-grid alignment, the relative
// range parser) lives up top with no DOM or fetch dependency, so it is unit
// testable headless; the browser engine follows. Every network URL is RELATIVE
// ("api/views", "api/series?...") so the server-injected <base href> resolves
// them under any sub-path mount -- the frontend carries no base_path knowledge.
(function (global) {
  "use strict";

  // =========================================================================
  // Section 1 -- expr evaluator (pure): tokenize -> parse (AST) -> eval.
  //
  // Grammar mirrors the server-side validator: `+ - * /`, parens, unary sign,
  // numeric constants, and identifiers referencing sibling series ids. The
  // server already guarantees a valid, resolvable, acyclic expr, but the parser
  // is defensive regardless.
  // =========================================================================

  function tokenizeExpr(input) {
    var tokens = [];
    var i = 0;
    var n = input.length;
    while (i < n) {
      var c = input[i];
      if (c === " " || c === "\t" || c === "\n" || c === "\r") {
        i += 1;
      } else if (c === "+" || c === "-" || c === "*" || c === "/" || c === "(" || c === ")") {
        tokens.push({ t: c });
        i += 1;
      } else if ((c >= "0" && c <= "9") || c === ".") {
        var num = "";
        while (i < n && ((input[i] >= "0" && input[i] <= "9") || input[i] === ".")) {
          num += input[i];
          i += 1;
        }
        var v = parseFloat(num);
        if (!isFinite(v)) {
          throw new Error("invalid numeric constant '" + num + "'");
        }
        tokens.push({ t: "num", v: v });
      } else if (isIdentStart(c)) {
        var id = "";
        while (i < n && isIdentPart(input[i])) {
          id += input[i];
          i += 1;
        }
        tokens.push({ t: "id", v: id });
      } else {
        throw new Error("unexpected character '" + c + "' in expr");
      }
    }
    return tokens;
  }

  function isIdentStart(c) {
    return (c >= "a" && c <= "z") || (c >= "A" && c <= "Z") || c === "_";
  }
  function isIdentPart(c) {
    return isIdentStart(c) || (c >= "0" && c <= "9");
  }

  // Recursive-descent parser producing an AST. Precedence: + - (lowest),
  // * /, then unary sign / parens / atoms.
  function parseExpr(input) {
    var tokens = tokenizeExpr(input);
    var pos = 0;

    function peek() {
      return tokens[pos];
    }
    function eat() {
      return tokens[pos++];
    }

    function expr() {
      var node = term();
      while (peek() && (peek().t === "+" || peek().t === "-")) {
        var op = eat().t;
        node = { type: "bin", op: op, l: node, r: term() };
      }
      return node;
    }
    function term() {
      var node = factor();
      while (peek() && (peek().t === "*" || peek().t === "/")) {
        var op = eat().t;
        node = { type: "bin", op: op, l: node, r: factor() };
      }
      return node;
    }
    function factor() {
      var tok = peek();
      if (!tok) {
        throw new Error("unexpected end of expr");
      }
      if (tok.t === "+" || tok.t === "-") {
        eat();
        var inner = factor();
        return tok.t === "-" ? { type: "neg", v: inner } : inner;
      }
      if (tok.t === "(") {
        eat();
        var e = expr();
        if (!peek() || peek().t !== ")") {
          throw new Error("expected ')'");
        }
        eat();
        return e;
      }
      if (tok.t === "num") {
        eat();
        return { type: "num", v: tok.v };
      }
      if (tok.t === "id") {
        eat();
        return { type: "var", name: tok.v };
      }
      throw new Error("unexpected token in expr");
    }

    var ast = expr();
    if (pos !== tokens.length) {
      throw new Error("trailing tokens after a complete expr");
    }
    return ast;
  }

  // Collect the sibling ids an AST references, into a plain array (deduped).
  function collectRefs(ast, acc) {
    acc = acc || {};
    if (ast.type === "var") {
      acc[ast.name] = true;
    } else if (ast.type === "neg") {
      collectRefs(ast.v, acc);
    } else if (ast.type === "bin") {
      collectRefs(ast.l, acc);
      collectRefs(ast.r, acc);
    }
    return Object.keys(acc);
  }

  // Evaluate an AST against an env map {id: number}. A missing id yields
  // undefined and propagates to NaN; callers that must skip such points check
  // ref presence before evaluating (see evalDerivedGrid).
  function evalAst(ast, env) {
    switch (ast.type) {
      case "num":
        return ast.v;
      case "var":
        return env[ast.name];
      case "neg":
        return -evalAst(ast.v, env);
      case "bin": {
        var a = evalAst(ast.l, env);
        var b = evalAst(ast.r, env);
        switch (ast.op) {
          case "+":
            return a + b;
          case "-":
            return a - b;
          case "*":
            return a * b;
          case "/":
            return a / b;
        }
        return NaN;
      }
    }
    return NaN;
  }

  // Convenience: parse and evaluate in one call (used by tests).
  function evalExpr(input, env) {
    return evalAst(parseExpr(input), env);
  }

  // =========================================================================
  // Section 2 -- bucket-grid alignment (pure).
  //
  // Bound series arrive as [[t_ms, value], ...]. Derived series are evaluated
  // pointwise on the aligned grid: at each timestamp present across ALL
  // referenced siblings, evaluate the expr; timestamps where any referenced
  // series lacks a value are skipped. Values feed a Map<t, value> so a later
  // derived series can reference an earlier one (declaration order chaining).
  // =========================================================================

  function pointsToMap(points) {
    var m = new Map();
    for (var i = 0; i < points.length; i += 1) {
      m.set(points[i][0], points[i][1]);
    }
    return m;
  }

  // Sorted [[t, value], ...] from a Map<t, value>; non-finite values become
  // null so ECharts renders a gap rather than a bogus point.
  function mapToPoints(m) {
    var ts = Array.from(m.keys()).sort(function (a, b) {
      return a - b;
    });
    var out = [];
    for (var i = 0; i < ts.length; i += 1) {
      var v = m.get(ts[i]);
      out.push([ts[i], isFinite(v) ? v : null]);
    }
    return out;
  }

  // Evaluate a derived expr over the aligned grid. `gridById` is a
  // Map<id, Map<t, value>> of every sibling computed so far. Returns a fresh
  // Map<t, value> holding a value only where every referenced sibling has one.
  function evalDerivedGrid(exprStr, gridById) {
    var ast = parseExpr(exprStr);
    var refs = collectRefs(ast);
    var tset = new Set();
    var r, g, it;
    for (r = 0; r < refs.length; r += 1) {
      g = gridById.get(refs[r]);
      if (g) {
        it = g.keys();
        for (var k = it.next(); !k.done; k = it.next()) {
          tset.add(k.value);
        }
      }
    }
    var out = new Map();
    var ts = Array.from(tset);
    for (var i = 0; i < ts.length; i += 1) {
      var t = ts[i];
      var env = {};
      var ok = true;
      for (r = 0; r < refs.length; r += 1) {
        g = gridById.get(refs[r]);
        var v = g ? g.get(t) : undefined;
        if (v === undefined) {
          ok = false;
          break;
        }
        env[refs[r]] = v;
      }
      if (ok) {
        out.set(t, evalAst(ast, env));
      }
    }
    return out;
  }

  // Turn a panel's bound-series point arrays into per-series display points,
  // resolving derived series in declaration order. `boundPoints[idx]` holds the
  // raw [[t,v]] for a bound series (ignored for a derived one). Every value --
  // bound or derived -- is multiplied by its `scale`, and the scaled value is
  // what enters the shared grid, so an expr composes the values as plotted
  // (a scale:-1 sibling contributes its mirrored, negative value).
  function buildPanelSeriesData(panelSeries, boundPoints) {
    var gridById = new Map();
    var out = [];
    for (var idx = 0; idx < panelSeries.length; idx += 1) {
      var s = panelSeries[idx];
      var scale = typeof s.scale === "number" ? s.scale : 1;
      var valMap = new Map();
      if (s.uuid !== undefined && s.uuid !== null) {
        var raw = boundPoints[idx] || [];
        for (var p = 0; p < raw.length; p += 1) {
          valMap.set(raw[p][0], raw[p][1] * scale);
        }
      } else if (s.expr) {
        var base = evalDerivedGrid(s.expr, gridById);
        var bit = base.entries();
        for (var e = bit.next(); !e.done; e = bit.next()) {
          valMap.set(e.value[0], e.value[1] * scale);
        }
      }
      if (s.id) {
        gridById.set(s.id, valMap);
      }
      out.push(mapToPoints(valMap));
    }
    return out;
  }

  // =========================================================================
  // Section 3 -- range parsing (pure).
  //
  // A relative range is `now` or `now-<n><unit>` with units s m h d w M y
  // (m = minutes, M = months). It resolves against a supplied clock so it is
  // deterministic under test. Months/years use calendar arithmetic; the rest
  // are fixed multiples. Refresh is `<n><unit>` (s|m|h) or `off`.
  // =========================================================================

  var FIXED_UNIT_MS = {
    s: 1000,
    m: 60000,
    h: 3600000,
    d: 86400000,
    w: 604800000,
  };

  function parseRange(spec, nowMs) {
    var s = String(spec).trim();
    var m = /^now(?:-(\d+)([smhdwMy]))?$/.exec(s);
    if (!m) {
      return null;
    }
    var to = nowMs;
    if (m[1] === undefined) {
      return { from: to, to: to };
    }
    var count = parseInt(m[1], 10);
    var unit = m[2];
    var from;
    if (Object.prototype.hasOwnProperty.call(FIXED_UNIT_MS, unit)) {
      from = to - count * FIXED_UNIT_MS[unit];
    } else {
      var d = new Date(to);
      if (unit === "M") {
        d.setMonth(d.getMonth() - count);
      } else {
        d.setFullYear(d.getFullYear() - count);
      }
      from = d.getTime();
    }
    return { from: from, to: to };
  }

  function parseRefreshMs(spec) {
    if (spec === undefined || spec === null) {
      return null;
    }
    var s = String(spec).trim();
    if (s === "" || s === "off") {
      return null;
    }
    var m = /^(\d+)([smh])$/.exec(s);
    if (!m) {
      return null;
    }
    return parseInt(m[1], 10) * FIXED_UNIT_MS[m[2]];
  }

  // --- exports for headless unit tests; browser engine does not run in node ---
  if (typeof module !== "undefined" && module.exports) {
    module.exports = {
      tokenizeExpr: tokenizeExpr,
      parseExpr: parseExpr,
      collectRefs: collectRefs,
      evalAst: evalAst,
      evalExpr: evalExpr,
      pointsToMap: pointsToMap,
      mapToPoints: mapToPoints,
      evalDerivedGrid: evalDerivedGrid,
      buildPanelSeriesData: buildPanelSeriesData,
      parseRange: parseRange,
      parseRefreshMs: parseRefreshMs,
    };
    return;
  }

  // =========================================================================
  // Section 4 -- theme (browser). Colors come from the dataviz reference
  // palette; light/dark are selected sets, swapped on the OS preference.
  // =========================================================================

  var PALETTE = {
    light: ["#2a78d6", "#1baf7a", "#eda100", "#008300", "#4a3aa7", "#e34948", "#e87ba4", "#eb6834"],
    dark: ["#3987e5", "#199e70", "#c98500", "#008300", "#9085e9", "#e66767", "#d55181", "#d95926"],
  };
  var CHROME = {
    light: { surface: "#fcfcfb", ink: "#0b0b0b", ink2: "#52514e", muted: "#898781", grid: "#e1e0d9", axis: "#c3c2b7" },
    dark: { surface: "#1a1a19", ink: "#ffffff", ink2: "#c3c2b7", muted: "#898781", grid: "#2c2c2a", axis: "#383835" },
  };

  function themeName() {
    return global.matchMedia && global.matchMedia("(prefers-color-scheme: dark)").matches
      ? "dark"
      : "light";
  }

  // =========================================================================
  // Section 5 -- state spine: the selected view and global time range, both
  // synced to the URL query. The range is either relative (a `now-...` spec
  // re-resolved against the clock) or absolute (a fixed [from,to] ms window,
  // e.g. after a drag-zoom).
  // =========================================================================

  var DEFAULT_RANGE = "now-24h";

  var state = {
    views: [],
    viewId: null,
    range: { kind: "rel", spec: DEFAULT_RANGE },
  };

  function currentView() {
    for (var i = 0; i < state.views.length; i += 1) {
      if (state.views[i].id === state.viewId) {
        return state.views[i];
      }
    }
    return state.views[0] || null;
  }

  // Resolve the effective [from,to] ms window from the range state and clock.
  function resolveWindow() {
    if (state.range.kind === "abs") {
      return { from: state.range.from, to: state.range.to };
    }
    var w = parseRange(state.range.spec, Date.now());
    return w || parseRange(DEFAULT_RANGE, Date.now());
  }

  // The wheel/slider zoom lives inside the loaded data, so a panel fetches and
  // pins its x-axis to a window PADDED by this fraction of the span on each
  // side, while the dataZoom shows only the inner [from,to]. That inset gives a
  // scroll-out room to reveal loaded margin before it triggers a requery,
  // instead of dead-ending at the data edge; and since the inner window is set
  // by absolute value, the visible range holds steady across the redraw (see
  // buildPanelOption and onZoom).
  var ZOOM_PAD = 0.5;

  function padWindow(win) {
    var pad = (win.to - win.from) * ZOOM_PAD;
    return { from: Math.round(win.from - pad), to: Math.round(win.to + pad) };
  }

  function readUrl() {
    var params = new URLSearchParams(global.location.search);
    var view = params.get("view");
    var from = params.get("from");
    var to = params.get("to");
    var range = params.get("range");
    if (view) {
      state.viewId = view;
    }
    if (from && to && isFinite(+from) && isFinite(+to)) {
      state.range = { kind: "abs", from: +from, to: +to };
    } else if (range) {
      state.range = { kind: "rel", spec: range };
    } else {
      state.range = null; // signal: fall back to the view default
    }
  }

  function writeUrl() {
    var params = new URLSearchParams();
    if (state.viewId) {
      params.set("view", state.viewId);
    }
    if (state.range.kind === "abs") {
      params.set("from", String(state.range.from));
      params.set("to", String(state.range.to));
    } else {
      params.set("range", state.range.spec);
    }
    var base = global.location.pathname + "?" + params.toString();
    global.history.replaceState(null, "", base);
  }

  // Adopt a view's default range (URL carried none, or we switched views).
  function applyViewDefaultRange() {
    var v = currentView();
    var spec = (v && v.range) || DEFAULT_RANGE;
    state.range = { kind: "rel", spec: spec };
  }

  // =========================================================================
  // Section 6 -- data fetch, with in-flight cancellation. A drag-zoom or a
  // refresh supersedes any pending fetch: we abort the previous batch before
  // starting a new one, so a narrow zoom never races a stale wide response.
  // =========================================================================

  var inflight = null;

  function abortInflight() {
    if (inflight) {
      inflight.abort();
      inflight = null;
    }
  }

  function fetchViews() {
    return fetch("api/views").then(function (resp) {
      if (!resp.ok) {
        throw new Error("api/views HTTP " + resp.status);
      }
      return resp.json();
    });
  }

  function fetchSeries(viewId, panelIdx, seriesIdx, win, signal) {
    var url =
      "api/series?view=" +
      encodeURIComponent(viewId) +
      "&panel=" +
      panelIdx +
      "&series=" +
      seriesIdx +
      "&from=" +
      win.from +
      "&to=" +
      win.to;
    return fetch(url, { signal: signal }).then(function (resp) {
      if (!resp.ok) {
        throw new Error("api/series HTTP " + resp.status);
      }
      return resp.json();
    });
  }

  // =========================================================================
  // Section 7 -- per-panel ECharts option builder (capability -> option map).
  // =========================================================================

  var INTERP = { linear: {}, smooth: { smooth: true }, step: { step: "end" } };

  function buildPanelOption(panel, seriesData, viewWin, padWin, dbUnits, animate) {
    var theme = themeName();
    var chrome = CHROME[theme];
    var palette = PALETTE[theme];

    // axis id -> index, so a series can target one by id (unknown/none -> 0).
    var axisIndex = {};
    if (panel.axes) {
      panel.axes.forEach(function (ax, i) {
        axisIndex[ax.id] = i;
      });
    }

    // Per-series target axis and resolved unit. Unit precedence: the config
    // axis unit (an explicit override) wins, else the store's series_meta.unit
    // returned next to the points (dbUnits). Also remember the first store unit
    // seen per axis, so an axis with no config unit still gets a label.
    var seriesAxis = [];
    var seriesUnits = [];
    var axisStoreUnit = [];
    panel.series.forEach(function (s, i) {
      var yIdx = s.axis && axisIndex[s.axis] !== undefined ? axisIndex[s.axis] : 0;
      seriesAxis[i] = yIdx;
      var cfgUnit = (panel.axes && panel.axes[yIdx] && panel.axes[yIdx].unit) || "";
      var dbUnit = (dbUnits && dbUnits[i]) || "";
      seriesUnits[i] = cfgUnit || dbUnit;
      if (dbUnit && axisStoreUnit[yIdx] === undefined) {
        axisStoreUnit[yIdx] = dbUnit;
      }
    });

    // y-axes: declared axes -> yAxis[]; none declared -> one implicit axis.
    var yAxis;
    if (panel.axes && panel.axes.length) {
      yAxis = panel.axes.map(function (ax, i) {
        return {
          type: "value",
          position: ax.side || (i === 0 ? "left" : "right"),
          // Config unit is the override; else fall back to the store unit of a
          // series on this axis, else leave it unlabeled.
          name: ax.unit || axisStoreUnit[i] || "",
          min: ax.min !== undefined ? ax.min : null,
          max: ax.max !== undefined ? ax.max : null,
          nameTextStyle: { color: chrome.ink2 },
          axisLine: { lineStyle: { color: chrome.axis } },
          axisLabel: { color: chrome.muted },
          splitLine: { lineStyle: { color: chrome.grid } },
        };
      });
    } else {
      yAxis = [
        {
          type: "value",
          // The implicit axis borrows the store unit of its series.
          name: axisStoreUnit[0] || "",
          nameTextStyle: { color: chrome.ink2 },
          axisLine: { lineStyle: { color: chrome.axis } },
          axisLabel: { color: chrome.muted },
          splitLine: { lineStyle: { color: chrome.grid } },
        },
      ];
    }

    var legend = [];
    var series = panel.series.map(function (s, i) {
      var label = s.label || s.id || "series " + (i + 1);
      legend.push(label);
      var color = s.color || palette[i % palette.length];
      var yIdx = seriesAxis[i];
      var opt = {
        name: label,
        type: s.type === "bar" ? "bar" : "line",
        yAxisIndex: yIdx,
        data: seriesData[i],
        color: color,
        itemStyle: { color: color },
      };
      if (s.stack) {
        opt.stack = s.stack;
        // samesign: positive values stack up, negative (scale:-1) stack down,
        // so a mirrored series lands below zero within its group.
        opt.stackStrategy = "samesign";
      }
      if (opt.type === "line") {
        var mode = INTERP[s.interp] || INTERP.linear;
        if (mode.smooth) {
          opt.smooth = true;
        }
        if (mode.step) {
          opt.step = mode.step;
        }
        opt.showSymbol = false;
        opt.lineStyle = { width: 2, color: color };
        if (s.fill) {
          opt.areaStyle = { color: color, opacity: 0.15 };
        }
      } else {
        opt.itemStyle = { color: color, borderRadius: [3, 3, 0, 0] };
        if (s.stack) {
          opt.itemStyle.borderRadius = [2, 2, 0, 0];
        }
        // Fatter bars: narrow the gap around each time band (default 20%) and,
        // where several bar series share a band side by side, the gap between
        // them (default 30%), so grouped bars read as blocks, not thin sticks.
        opt.barCategoryGap = "10%";
        opt.barGap = "10%";
      }
      return opt;
    });

    return {
      // Animate the first paint, but snap data updates (zoom requery, refresh)
      // instantly -- a transition there just slides the graph around before it
      // settles on the new window.
      animation: animate,
      backgroundColor: "transparent",
      textStyle: { color: chrome.ink2, fontFamily: "system-ui, -apple-system, sans-serif" },
      grid: { left: 56, right: 56, top: 36, bottom: 44, containLabel: true },
      legend: {
        data: legend,
        top: 4,
        textStyle: { color: chrome.ink2 },
        inactiveColor: chrome.muted,
      },
      tooltip: {
        trigger: "axis",
        axisPointer: { type: "cross", link: [{ xAxisIndex: "all" }] },
        // Round to 3 decimals (raw f64 values otherwise print ~15 digits) and
        // append each series' unit (config axis unit, else the store default).
        formatter: function (params) {
          if (!params.length) {
            return "";
          }
          var lines = [params[0].axisValueLabel];
          params.forEach(function (p) {
            var v = Array.isArray(p.value) ? p.value[1] : p.value;
            var text;
            if (v === null || v === undefined || (typeof v === "number" && isNaN(v))) {
              text = "-";
            } else {
              text = typeof v === "number" ? String(Number(v.toFixed(3))) : String(v);
              var unit = seriesUnits[p.seriesIndex];
              if (unit) {
                text += " " + unit;
              }
            }
            lines.push(p.marker + p.seriesName + ": " + text);
          });
          return lines.join("<br/>");
        },
      },
      // Pin the axis to the padded window and show only the inner [from,to] via
      // the dataZoom below, so a zoom-out has margin to reveal and a dataZoom
      // value maps back to an exact [from,to] on requery (see onZoom).
      xAxis: {
        type: "time",
        min: padWin.from,
        max: padWin.to,
        axisLine: { lineStyle: { color: chrome.axis } },
        axisLabel: { color: chrome.muted },
        splitLine: { show: false },
      },
      yAxis: yAxis,
      dataZoom: [
        { type: "inside", xAxisIndex: 0, filterMode: "none", startValue: viewWin.from, endValue: viewWin.to },
        { type: "slider", xAxisIndex: 0, filterMode: "none", height: 18, bottom: 8, startValue: viewWin.from, endValue: viewWin.to },
      ],
      series: series,
    };
  }

  // =========================================================================
  // Section 8 -- render lifecycle: build the panel DOM, fetch bound series,
  // evaluate derived series, and set each chart's option. Charts in a view are
  // connected so the crosshair and zoom link across panels.
  // =========================================================================

  var charts = []; // live ECharts instances for the current view
  var GROUP = "loxweb";
  var suppressZoom = false; // guard programmatic setOption from re-triggering

  function clearCharts() {
    charts.forEach(function (c) {
      c.dispose();
    });
    charts = [];
  }

  function renderView() {
    var host = document.getElementById("panels");
    host.textContent = "";
    clearCharts();

    var view = currentView();
    if (!view || !view.panels || !view.panels.length) {
      var empty = document.createElement("p");
      empty.className = "empty";
      empty.textContent = view ? "This view has no panels." : "No views configured.";
      host.appendChild(empty);
      return;
    }

    var win = resolveWindow();

    view.panels.forEach(function (panel, panelIdx) {
      var card = document.createElement("section");
      card.className = "panel";
      var h = document.createElement("h2");
      h.className = "panel-title";
      h.textContent = panel.title || "Panel " + (panelIdx + 1);
      card.appendChild(h);

      var plot = document.createElement("div");
      plot.className = "plot";
      // panel.height is a relative grid-row hint; map it to pixels with a floor.
      plot.style.height = (panel.height ? Math.max(panel.height * 26, 160) : 300) + "px";
      card.appendChild(plot);
      host.appendChild(card);

      var chart = global.echarts.init(plot, null, { renderer: "canvas" });
      chart.group = GROUP;
      chart.on("datazoom", function (e) {
        onZoom(chart, e);
      });
      charts.push(chart);

      loadPanel(view, panel, panelIdx, chart, plot, win);
    });

    global.echarts.connect(GROUP);
  }

  // Fetch a panel's bound series, evaluate its derived series, and paint.
  // A failed bound fetch shows a per-panel error rather than crashing the view;
  // an aborted fetch (a superseding zoom/refresh) is ignored.
  function loadPanel(view, panel, panelIdx, chart, plot, win) {
    plot.classList.add("loading");
    var signal = inflight ? inflight.signal : undefined;
    // Fetch the padded window; the dataZoom shows only the inner `win`.
    var padWin = padWindow(win);

    var fetches = panel.series.map(function (s, seriesIdx) {
      if (s.uuid !== undefined && s.uuid !== null) {
        return fetchSeries(view.id, panelIdx, seriesIdx, padWin, signal);
      }
      return Promise.resolve(null); // derived: computed client-side
    });

    Promise.all(fetches)
      .then(function (results) {
        // Each bound fetch yields {unit, points}; a derived one is null. Split
        // into the raw point arrays buildPanelSeriesData expects and the per-
        // series store units the option builder defaults from.
        var boundPoints = results.map(function (r) {
          return r ? r.points : null;
        });
        var dbUnits = results.map(function (r) {
          return r ? r.unit : null;
        });
        var seriesData = buildPanelSeriesData(panel.series, boundPoints);
        var animate = !chart.__painted;
        chart.__painted = true;
        // Remember the extent we pinned the axis to, so onZoom can resolve a
        // zoom's percentages against the window actually on screen (a burst of
        // ticks all map against this, not against the drifting state.range).
        chart.__padWin = padWin;
        suppressZoom = true;
        chart.setOption(buildPanelOption(panel, seriesData, win, padWin, dbUnits, animate), {
          notMerge: true,
        });
        setTimeout(function () {
          suppressZoom = false;
        }, 0);
        clearPanelError(plot);
        plot.classList.remove("loading");
      })
      .catch(function (err) {
        if (err && (err.name === "AbortError" || (signal && signal.aborted))) {
          return; // superseded; the new batch owns the panel now
        }
        plot.classList.remove("loading");
        showPanelError(plot, err);
      });
  }

  function showPanelError(plot, err) {
    clearPanelError(plot);
    var box = document.createElement("div");
    box.className = "panel-error";
    box.textContent = "Failed to load data: " + (err && err.message ? err.message : String(err));
    plot.appendChild(box);
  }
  function clearPanelError(plot) {
    var old = plot.querySelector(".panel-error");
    if (old) {
      old.remove();
    }
  }

  // =========================================================================
  // Section 9 -- interaction: drag-zoom -> tier-routed requery, and refresh.
  //
  // The zoom read is the interaction risk. On a datazoom event we map the
  // reported percentages against the chart's rendered axis extent to an absolute
  // [from,to] (value bounds are the fallback), switch the global
  // range to that window, DEBOUNCE the burst (connected panels each fire an
  // event), cancel any in-flight fetches, and refetch every panel once. The
  // server tier-routes by span, so a narrower window transparently pulls a finer
  // aggregate tier.
  // =========================================================================

  var zoomTimer = null;

  function onZoom(chart, e) {
    if (suppressZoom) {
      return;
    }
    // Map the reported zoom to an absolute [from,to]. Resolve the percentages
    // against the chart's OWN rendered axis extent, which holds still through a
    // burst of wheel ticks -- state.range moves on every tick but the axis is
    // not requeried until the burst settles, so mapping against state.range
    // would drift and the requery would overshoot the live zoom.
    var axisWin = chart.__padWin || padWindow(resolveWindow());
    var axisSpan = axisWin.to - axisWin.from;
    var z = e.batch && e.batch.length ? e.batch[0] : e;
    var from;
    var to;
    if (isFinite(z.start) && isFinite(z.end)) {
      from = Math.round(axisWin.from + (axisSpan * z.start) / 100);
      to = Math.round(axisWin.from + (axisSpan * z.end) / 100);
    } else if (isFinite(z.startValue) && isFinite(z.endValue)) {
      from = Math.round(z.startValue);
      to = Math.round(z.endValue);
    } else {
      return;
    }
    if (!(to > from)) {
      return;
    }
    state.range = { kind: "abs", from: from, to: to };
    writeUrl();

    if (zoomTimer) {
      clearTimeout(zoomTimer);
    }
    zoomTimer = setTimeout(function () {
      zoomTimer = null;
      reloadAllPanels();
    }, 280);
  }

  // Refetch every panel of the current view against the current window, with a
  // single fresh AbortController so a prior batch is cancelled.
  function reloadAllPanels() {
    abortInflight();
    inflight = new AbortController();
    var view = currentView();
    if (!view) {
      return;
    }
    var win = resolveWindow();
    view.panels.forEach(function (panel, panelIdx) {
      var chart = charts[panelIdx];
      if (chart) {
        loadPanel(view, panel, panelIdx, chart, chart.getDom(), win);
      }
    });
  }

  var refreshTimer = null;

  function scheduleRefresh() {
    if (refreshTimer) {
      clearInterval(refreshTimer);
      refreshTimer = null;
    }
    if (document.hidden) {
      return; // paused while the tab is hidden
    }
    var view = currentView();
    var ms = view ? parseRefreshMs(view.refresh) : null;
    if (!ms) {
      return;
    }
    refreshTimer = setInterval(function () {
      // A relative range slides `now` forward; an absolute one refetches its
      // fixed window (still useful -- the latest tier data may have changed).
      reloadAllPanels();
    }, ms);
  }

  function onVisibility() {
    if (document.hidden) {
      if (refreshTimer) {
        clearInterval(refreshTimer);
        refreshTimer = null;
      }
    } else {
      reloadAllPanels();
      scheduleRefresh();
    }
  }

  // =========================================================================
  // Section 10 -- top bar: view tabs and range presets.
  // =========================================================================

  var RANGE_PRESETS = [
    { label: "1h", spec: "now-1h" },
    { label: "6h", spec: "now-6h" },
    { label: "24h", spec: "now-24h" },
    { label: "7d", spec: "now-7d" },
    { label: "30d", spec: "now-30d" },
    { label: "6M", spec: "now-6M" },
    { label: "1y", spec: "now-1y" },
  ];

  function renderTabs() {
    var nav = document.getElementById("views");
    nav.textContent = "";
    state.views.forEach(function (v) {
      var b = document.createElement("button");
      b.className = "tab" + (v.id === state.viewId ? " active" : "");
      b.textContent = v.title || v.id;
      b.addEventListener("click", function () {
        if (v.id === state.viewId) {
          return;
        }
        state.viewId = v.id;
        applyViewDefaultRange(); // switching a view adopts its default window
        writeUrl();
        renderTabs();
        renderRanges();
        fullRender();
      });
      nav.appendChild(b);
    });
  }

  function renderRanges() {
    var box = document.getElementById("ranges");
    box.textContent = "";
    var activeSpec = state.range.kind === "rel" ? state.range.spec : null;
    RANGE_PRESETS.forEach(function (p) {
      var b = document.createElement("button");
      b.className = "range" + (p.spec === activeSpec ? " active" : "");
      b.textContent = p.label;
      b.addEventListener("click", function () {
        state.range = { kind: "rel", spec: p.spec };
        writeUrl();
        renderRanges();
        fullRender();
      });
      box.appendChild(b);
    });
    if (state.range.kind === "abs") {
      var span = document.createElement("span");
      span.className = "range-abs";
      span.textContent =
        new Date(state.range.from).toLocaleString() + "  ->  " + new Date(state.range.to).toLocaleString();
      box.appendChild(span);
    }
  }

  // A full render owns its own fetch batch (fresh AbortController).
  function fullRender() {
    abortInflight();
    inflight = new AbortController();
    renderView();
    scheduleRefresh();
  }

  // =========================================================================
  // Section 11 -- bootstrap.
  // =========================================================================

  function boot() {
    readUrl();
    fetchViews()
      .then(function (dashboards) {
        state.views = (dashboards && dashboards.views) || [];
        if (!state.viewId || !currentView()) {
          state.viewId = state.views.length ? state.views[0].id : null;
        }
        if (!state.range) {
          applyViewDefaultRange();
        }
        writeUrl();
        renderTabs();
        renderRanges();
        fullRender();

        document.addEventListener("visibilitychange", onVisibility);
        if (global.matchMedia) {
          var mq = global.matchMedia("(prefers-color-scheme: dark)");
          var onThemeChange = function () {
            renderView();
          };
          if (mq.addEventListener) {
            mq.addEventListener("change", onThemeChange);
          } else if (mq.addListener) {
            mq.addListener(onThemeChange);
          }
        }
        global.addEventListener("resize", function () {
          charts.forEach(function (c) {
            c.resize();
          });
        });
      })
      .catch(function (err) {
        var host = document.getElementById("panels");
        if (host) {
          host.textContent = "";
          var box = document.createElement("p");
          box.className = "fatal-error";
          box.textContent = "Could not load views: " + (err && err.message ? err.message : String(err));
          host.appendChild(box);
        }
      });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})(typeof window !== "undefined" ? window : this);
