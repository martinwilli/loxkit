//! The HTTP layer: the axum router, its handlers, and the request resolver.
//!
//! Two API endpoints plus a self-contained static frontend:
//!
//! * `GET /api/views` serves the current [`Dashboards`] snapshot as JSON, so the
//!   frontend renders whatever the config describes (no view is baked into a
//!   route).
//! * `GET /api/series` takes `(view, panel, series, from, to)` — the frontend
//!   addresses a series by its array index in the `/api/views` payload — resolves
//!   it to a [`SeriesRequest`], and returns `{unit, points}`: the store's unit
//!   label (the display default) and the tier-routed points as an ECharts-native
//!   `[[t_ms, value], …]` array.
//! * `/`, `/app.js`, `/style.css`, `/echarts.min.js` are the SPA and the vendored
//!   ECharts bundle, compiled into the binary with `include_str!` and served from
//!   explicit routes — no runtime asset directory, so a bare `cargo run` and the
//!   packaged binary both work with zero asset-path config.
//!
//! **base_path.** A reverse proxy may mount loxweb under a sub-path (e.g.
//! `/app`). That is handled entirely server-side so the frontend uses only
//! relative URLs: every route is registered at its final, sub-path-prefixed
//! address, and the served `index.html` carries a `<base href="__BASE_PATH__/">`
//! tag whose placeholder the index handler substitutes. With `<base>` set, every
//! relative URL in the SPA resolves under the mount, so the frontend needs no
//! base_path knowledge.
//!
//! Routes are prefixed directly rather than via [`axum::Router::nest`]: nesting
//! at `/app` does not route a *trailing-slash* request (`/app/`) to the nested
//! `/`, yet that is exactly the mount root a browser and most proxies request.
//! Prefixing gives full control, and the SPA index is served at both `/app` and
//! `/app/` so either entry URL lands on the app.
//!
//! [`resolve_request`] is factored out **pure and DB-free** — it maps a
//! `(view, panel, series, from, to)` locator to a [`SeriesRequest`] or an
//! [`ApiError`] — so the routing logic is unit-testable offline while the
//! DB-touching [`query_series`] stays a thin call in the handler.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::Json;
use axum::Router;
use axum::extract::{Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use deadpool_postgres::Pool;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::error;
use uuid::Uuid;

use crate::dashboard::{DashboardStore, Dashboards};
use crate::query::{self, SeriesRequest};

/// The static assets, compiled into the binary so the server is self-contained.
const INDEX_HTML: &str = include_str!("../web/index.html");
const APP_JS: &str = include_str!("../web/app.js");
const STYLE_CSS: &str = include_str!("../web/style.css");
const ECHARTS_JS: &str = include_str!("../web/echarts.min.js");

/// The `__BASE_PATH__` placeholder in `index.html` that the `/` handler rewrites.
const BASE_PATH_PLACEHOLDER: &str = "__BASE_PATH__";

/// Shared handler state: the reloadable dashboards, the read pool, and the
/// normalized sub-path mount (empty or a leading-slash prefix like `/app`).
#[derive(Clone)]
struct AppState {
    store: Arc<DashboardStore>,
    pool: Pool,
    base_path: String,
}

/// Build the application router.
///
/// `base_path` is the configured sub-path mount; a trailing slash is trimmed, so
/// `""`, `"/"` and `"/app/"` all behave. Every route is registered at its final,
/// prefixed address; at the root mount the SPA index is `/`, under a sub-path it
/// is served at both `/app` and `/app/`.
pub fn router(store: Arc<DashboardStore>, pool: Pool, base_path: &str) -> Router {
    let base = base_path.trim_end_matches('/').to_owned();
    let state = AppState {
        store,
        pool,
        base_path: base.clone(),
    };

    let prefixed = |suffix: &str| format!("{base}{suffix}");
    let mut router = Router::new()
        .route(&prefixed("/app.js"), get(app_js))
        .route(&prefixed("/style.css"), get(style_css))
        .route(&prefixed("/echarts.min.js"), get(echarts_js))
        .route(&prefixed("/api/views"), get(get_views))
        .route(&prefixed("/api/series"), get(get_series));

    router = if base.is_empty() {
        router.route("/", get(index))
    } else {
        router.route(&base, get(index)).route(&prefixed("/"), get(index))
    };

    router.with_state(state)
}

// --- static assets ---

/// Serve `index.html`, substituting the configured base_path into its `<base>`.
async fn index(State(state): State<AppState>) -> impl IntoResponse {
    let html = INDEX_HTML.replace(BASE_PATH_PLACEHOLDER, &state.base_path);
    (
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        html,
    )
}

async fn app_js() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "text/javascript")], APP_JS)
}

async fn style_css() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "text/css")], STYLE_CSS)
}

async fn echarts_js() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "text/javascript")], ECHARTS_JS)
}

// --- API ---

/// `GET /api/views`: the current dashboards snapshot as JSON.
async fn get_views(State(state): State<AppState>) -> Json<Dashboards> {
    // Clone the snapshot out of the Arc: serializing an `Arc<T>` would need
    // serde's `rc` feature, and the config is small enough that a clone per
    // (rare) fetch is cheaper than pulling that in.
    Json((*state.store.current()).clone())
}

/// Query parameters for `GET /api/series`.
#[derive(Debug, Deserialize)]
struct SeriesQuery {
    /// View id.
    view: String,
    /// Panel index within the view.
    panel: usize,
    /// Series index within the panel.
    series: usize,
    /// Window start, Unix epoch milliseconds (JS-native).
    from: i64,
    /// Window end, Unix epoch milliseconds.
    to: i64,
}

/// The `GET /api/series` response: the store's unit label for the series (from
/// `series_meta`, omitted when unset) plus the ECharts-native `[[t_ms, value], …]`
/// points. The frontend uses `unit` as the display default, overridable by a
/// config axis unit.
#[derive(Debug, Serialize)]
struct SeriesResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    unit: Option<String>,
    points: Vec<(i64, f64)>,
}

/// `GET /api/series`: resolve the locator and return the series' unit plus the
/// tier-routed points as an ECharts-native `[[t_ms, value], …]` array. A resolve
/// error maps to its status; a query error is logged and surfaces as a 500.
async fn get_series(
    State(state): State<AppState>,
    Query(q): Query<SeriesQuery>,
) -> Result<Json<SeriesResponse>, ApiError> {
    // Resolve against a snapshot, then drop it before the await so a concurrent
    // reload never blocks on this request.
    let req = {
        let dash = state.store.current();
        resolve_request(&dash, &q.view, q.panel, q.series, q.from, q.to)?
    };
    let result = query::query_series(&state.pool, &req).await.map_err(|e| {
        // `{e:#}` prints the whole anyhow context chain on one line; plain
        // Display stops at the top context and hides the real cause (the db
        // error), which is exactly what we need in the log.
        let cause = format!("{e:#}");
        error!(error = %cause, uuid = %req.uuid, "series query failed");
        ApiError::Internal
    })?;
    let points = result
        .points
        .iter()
        .map(|p| (systemtime_to_ms(p.t), p.v))
        .collect();
    Ok(Json(SeriesResponse {
        unit: result.unit,
        points,
    }))
}

/// Locate a series by `(view, panel, series)` and build its [`SeriesRequest`].
///
/// Pure and DB-free (unit-tested offline): an unknown view/panel/series is a 404;
/// a derived series (an `expr`, no `uuid`) is a 400 because the client evaluates
/// those locally and must not query the store for them; a malformed `uuid` string
/// is a 400. The `sub` comes from [`Series::resolved_sub`](crate::dashboard::Series::resolved_sub),
/// the `agg` from the series, and the calendar bucket from the panel falling back
/// to the view.
fn resolve_request(
    dash: &Dashboards,
    view: &str,
    panel: usize,
    series: usize,
    from_ms: i64,
    to_ms: i64,
) -> Result<SeriesRequest, ApiError> {
    let view = dash
        .views
        .iter()
        .find(|v| v.id == view)
        .ok_or_else(|| ApiError::NotFound(format!("unknown view '{view}'")))?;
    let panel_ref = view.panels.get(panel).ok_or_else(|| {
        ApiError::NotFound(format!("panel {panel} out of range in view '{}'", view.id))
    })?;
    let series_ref = panel_ref.series.get(series).ok_or_else(|| {
        ApiError::NotFound(format!(
            "series {series} out of range in panel {panel} of view '{}'",
            view.id
        ))
    })?;

    // A derived series is evaluated client-side over its siblings; there is
    // nothing to query, so the client must not reach this endpoint for one.
    let Some(uuid_str) = series_ref.uuid.as_deref() else {
        return Err(ApiError::BadRequest(
            "series is derived (expr); evaluate it client-side, not via /api/series".to_owned(),
        ));
    };
    let uuid = Uuid::parse_str(uuid_str)
        .map_err(|e| ApiError::BadRequest(format!("invalid series uuid '{uuid_str}': {e}")))?;

    Ok(SeriesRequest {
        uuid,
        sub: series_ref.resolved_sub().to_owned(),
        agg: series_ref.agg,
        gapfill: series_ref.gapfill,
        kind: series_ref.kind,
        from: ms_to_systemtime(from_ms),
        to: ms_to_systemtime(to_ms),
        bucket: panel_ref.bucket.or(view.bucket),
    })
}

/// Unix epoch milliseconds to a [`SystemTime`]. A negative value (pre-epoch, not
/// expected from JS) clamps to the epoch.
fn ms_to_systemtime(ms: i64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(ms.max(0) as u64)
}

/// A [`SystemTime`] back to Unix epoch milliseconds for the JSON response.
fn systemtime_to_ms(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// An API error, mapped to an HTTP status and a short `{error: "..."}` body.
#[derive(Debug)]
enum ApiError {
    /// Unknown view/panel/series locator → 404.
    NotFound(String),
    /// A malformed request: a derived-series query or a bad uuid → 400.
    BadRequest(String),
    /// A query failure; the detail is logged, not leaked to the client → 500.
    Internal,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ApiError::NotFound(m) => (StatusCode::NOT_FOUND, m),
            ApiError::BadRequest(m) => (StatusCode::BAD_REQUEST, m),
            ApiError::Internal => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal error".to_owned(),
            ),
        };
        (status, Json(json!({ "error": message }))).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicU64, Ordering};

    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use loxtap::config::TimescaleConfig;
    use tower::ServiceExt as _;

    use crate::dashboard::{Agg, Bucket};

    /// A dashboards set exercising the resolver's branches: a bound and a derived
    /// series in one panel, a view-level calendar bucket, and a bad-uuid series.
    const DASH: &str = r#"
views:
  - id: main
    title: Main View
    panels:
      - title: P0
        series:
          - {id: a, label: A, uuid: "deadbeef-0000-0000-0000-000000000001", sub: total, agg: avg}
          - {id: d, label: D, expr: "a + 1"}
  - id: yearly
    title: Yearly View
    bucket: month
    panels:
      - title: PY
        series:
          - {label: Draw, uuid: "deadbeef-0000-0000-0000-000000000002", agg: delta}
  - id: bad
    title: Bad View
    panels:
      - title: PB
        series:
          - {label: X, uuid: "not-a-uuid"}
"#;

    /// Build a store by round-tripping YAML through the commit-2 file loader
    /// (there is no from-string constructor; a throwaway temp file is enough).
    fn store_from(yaml: &str) -> Arc<DashboardStore> {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("loxweb-http-test-{}-{n}.yaml", std::process::id()));
        std::fs::write(&path, yaml).expect("write temp dashboards");
        let store = DashboardStore::load(&path).expect("load temp dashboards");
        std::fs::remove_file(&path).ok();
        store
    }

    /// A lazy pool over an unreachable host. Never connected by the tests below,
    /// which all resolve or fail before any DB call.
    fn dummy_pool() -> Pool {
        query::build_pool(&TimescaleConfig {
            host: "db.invalid".to_owned(),
            port: 5432,
            user: "loxweb_ro".to_owned(),
            password: "x".to_owned(),
            dbname: "loxtap".to_owned(),
            sslmode: None,
            timezone: None,
        })
        .expect("build lazy pool")
    }

    fn app(base_path: &str) -> Router {
        router(store_from(DASH), dummy_pool(), base_path)
    }

    async fn body_string(resp: Response) -> String {
        let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    async fn get(app: Router, uri: &str) -> Response {
        app.oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap()
    }

    // --- /api/views ---

    #[tokio::test]
    async fn views_endpoint_returns_the_configured_views() {
        let resp = get(app(""), "/api/views").await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_string(resp).await;
        assert!(body.contains("Main View"), "body: {body}");
        assert!(body.contains("Yearly View"), "body: {body}");
        // The terse-JSON contract: an absent optional never renders as null.
        assert!(!body.contains("null"), "body: {body}");
    }

    // --- static assets ---

    #[tokio::test]
    async fn index_serves_html_with_root_base_href() {
        let resp = get(app(""), "/").await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/html; charset=utf-8"
        );
        let body = body_string(resp).await;
        assert!(body.contains(r#"<base href="/" />"#), "body: {body}");
        assert!(!body.contains(BASE_PATH_PLACEHOLDER), "placeholder left in: {body}");
    }

    #[tokio::test]
    async fn index_substitutes_a_nonempty_base_path() {
        let resp = get(app("/app"), "/app/").await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_string(resp).await;
        assert!(body.contains(r#"<base href="/app/" />"#), "body: {body}");
    }

    #[tokio::test]
    async fn index_is_served_at_both_bare_and_trailing_slash_mount() {
        // Either entry URL lands on the SPA, whichever the proxy forwards.
        assert_eq!(get(app("/app"), "/app").await.status(), StatusCode::OK);
        assert_eq!(get(app("/app"), "/app/").await.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn echarts_asset_is_served() {
        let resp = get(app(""), "/echarts.min.js").await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/javascript"
        );
        let body = body_string(resp).await;
        assert!(body.len() > 100_000, "echarts bundle looks too small");
    }

    // --- base_path scoping ---

    #[tokio::test]
    async fn base_path_scopes_all_routes() {
        let resp = get(app("/app"), "/app/api/views").await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn root_routes_are_absent_under_a_base_path() {
        let resp = get(app("/app"), "/api/views").await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn empty_base_path_serves_at_root() {
        let resp = get(app(""), "/api/views").await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    // --- resolve_request (pure) ---

    fn dashboards() -> Arc<Dashboards> {
        store_from(DASH).current()
    }

    #[test]
    fn resolve_binds_a_series() {
        let dash = dashboards();
        let req = resolve_request(&dash, "main", 0, 0, 1_000, 2_000).unwrap();
        assert_eq!(
            req.uuid,
            Uuid::parse_str("deadbeef-0000-0000-0000-000000000001").unwrap()
        );
        assert_eq!(req.sub, "total");
        assert_eq!(req.agg, Some(Agg::Avg));
        assert_eq!(req.bucket, None);
        assert_eq!(req.from, UNIX_EPOCH + Duration::from_millis(1_000));
        assert_eq!(req.to, UNIX_EPOCH + Duration::from_millis(2_000));
    }

    #[test]
    fn resolve_inherits_the_view_bucket_and_default_sub() {
        let dash = dashboards();
        // The yearly view carries bucket: month; its panel omits one, so the
        // request inherits it via panel.bucket.or(view.bucket).
        let req = resolve_request(&dash, "yearly", 0, 0, 0, 1).unwrap();
        assert_eq!(req.bucket, Some(Bucket::Month));
        // sub omitted on the series -> the default value channel.
        assert_eq!(req.sub, "value");
        assert_eq!(req.agg, Some(Agg::Delta));
    }

    #[test]
    fn resolve_rejects_a_derived_series() {
        let dash = dashboards();
        let err = resolve_request(&dash, "main", 0, 1, 0, 1).unwrap_err();
        assert!(matches!(err, ApiError::BadRequest(_)), "got {err:?}");
    }

    #[test]
    fn resolve_rejects_a_bad_uuid() {
        let dash = dashboards();
        let err = resolve_request(&dash, "bad", 0, 0, 0, 1).unwrap_err();
        assert!(matches!(err, ApiError::BadRequest(_)), "got {err:?}");
    }

    #[test]
    fn resolve_unknown_view_panel_series_is_not_found() {
        let dash = dashboards();
        assert!(matches!(
            resolve_request(&dash, "nope", 0, 0, 0, 1).unwrap_err(),
            ApiError::NotFound(_)
        ));
        assert!(matches!(
            resolve_request(&dash, "main", 9, 0, 0, 1).unwrap_err(),
            ApiError::NotFound(_)
        ));
        assert!(matches!(
            resolve_request(&dash, "main", 0, 9, 0, 1).unwrap_err(),
            ApiError::NotFound(_)
        ));
    }

    // --- /api/series over the router: the derived-series 400 errors before any
    // DB call, so it is exercisable offline; the happy path needs a live store. ---

    #[tokio::test]
    async fn series_endpoint_rejects_a_derived_locator() {
        let resp = get(app(""), "/api/series?view=main&panel=0&series=1&from=0&to=1000").await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body = body_string(resp).await;
        assert!(body.contains("error"), "body: {body}");
        assert!(body.contains("derived"), "body: {body}");
    }
}
