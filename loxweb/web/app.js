// Placeholder app script. The real ECharts-driven SPA lands in a later commit;
// this only proves the wiring end to end by listing the configured views.
//
// Every request uses a RELATIVE URL ("api/views", not "/api/views") so the
// <base href> the server injects resolves it under any sub-path mount. The
// frontend therefore carries no knowledge of base_path.
(function () {
  "use strict";

  fetch("api/views")
    .then(function (resp) {
      if (!resp.ok) {
        throw new Error("HTTP " + resp.status);
      }
      return resp.json();
    })
    .then(function (dashboards) {
      var list = document.getElementById("views");
      var views = (dashboards && dashboards.views) || [];
      views.forEach(function (view) {
        var item = document.createElement("li");
        item.textContent = view.title + " (" + view.id + ")";
        list.appendChild(item);
      });
      if (typeof echarts !== "undefined") {
        console.log("loxweb: ECharts " + echarts.version + " loaded");
      }
    })
    .catch(function (err) {
      console.error("loxweb: failed to load views", err);
    });
})();
