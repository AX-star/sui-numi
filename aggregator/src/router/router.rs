// Router HTTP API implementation
// This file provides HTTP endpoints for order routing and execution
//
// Numan Thabit 2025 Nov

use crate::venues::adapter::LimitReq;
use axum::{
    body::Body,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{Html, Json, Response},
    routing::{get, post},
    Router as AxumRouter,
};
use prometheus::{Encoder, TextEncoder};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{field, info_span};

use super::{ExecutionEngine, RouteSelector};
use crate::control::{AdmissionControl, CircuitBreakers};
use crate::metrics::{REQ_ERRORS, REQ_LATENCY};
use crate::router::execution::{ExecutionResult, ExecutionStats};
use crate::router::routes::RouteSelection;
use crate::router::selector::LatencyStats;
use crate::router::validation::validate_limit_order;
use anyhow::{Context, Result};

/// High-level Router that ties selection and execution together
pub struct Router {
    selector: Arc<RouteSelector>,
    executor: Arc<ExecutionEngine>,
    admission: Option<Arc<AdmissionControl>>,
    breakers: Option<Arc<CircuitBreakers>>,
    idempotency: Arc<RwLock<HashMap<String, IdemEntry>>>,
    idem_ttl: Duration,
}

impl Router {
    pub fn new(selector: Arc<RouteSelector>, executor: Arc<ExecutionEngine>) -> Self {
        Self {
            selector,
            executor,
            admission: None,
            breakers: None,
            idempotency: Arc::new(RwLock::new(HashMap::new())),
            idem_ttl: Duration::from_secs(300),
        }
    }

    /// Set admission control and circuit breakers
    pub fn with_control(
        mut self,
        admission: Arc<AdmissionControl>,
        breakers: Arc<CircuitBreakers>,
    ) -> Self {
        self.admission = Some(admission);
        self.breakers = Some(breakers);
        self
    }

    /// Get access to the route selector (for operations like updating latency estimates)
    pub fn selector(&self) -> &Arc<RouteSelector> {
        &self.selector
    }

    /// Get access to the execution engine (for operations like setting sponsorship)
    pub fn executor(&self) -> &Arc<ExecutionEngine> {
        &self.executor
    }

    async fn idem_get(&self, key: &str) -> Option<LimitOrderResponse> {
        let guard = self.idempotency.read().await;
        if let Some(entry) = guard.get(key) {
            if entry.at.elapsed() < self.idem_ttl {
                return Some(entry.response.clone());
            }
        }
        None
    }

    async fn idem_put(&self, key: String, response: LimitOrderResponse) {
        let mut guard = self.idempotency.write().await;
        guard.insert(
            key,
            IdemEntry {
                at: Instant::now(),
                response,
            },
        );
    }

    /// Route a single DeepBook limit order request and execute it
    pub async fn execute_limit_order(&self, req: &LimitReq) -> Result<ExecutionResult> {
        // 1. Acquire admission control permit
        let _permit = if let Some(admission) = &self.admission {
            Some(admission.acquire().await)
        } else {
            None
        };

        // 2. Pre-trade validation
        if let Some(adapter) = self.selector.deepbook_adapter() {
            let validation = validate_limit_order(adapter, req).await?;
            validation
                .into_result()
                .context("pre-trade validation failed")?;
        }

        // 3. Select route
        let sel = self.selector.select_route(req).await?;
        let best = sel.best_plan().clone();
        let uses_shared = best.uses_shared_objects;

        // 4. Check circuit breaker for route class
        let route_class = format!("{:?}", best.route);
        if let Some(breakers) = &self.breakers {
            if breakers.is_open(&route_class).await {
                anyhow::bail!("circuit breaker open for route class: {}", route_class);
            }
        }

        // 5. Execute route
        let result = match self.executor.execute(&best).await {
            Ok(result) => {
                // Record success in circuit breaker
                if let Some(breakers) = &self.breakers {
                    breakers.record_success(&route_class).await;
                }
                // Record latency observation for adaptive updates
                self.selector
                    .record_latency(result.effects_time_ms, uses_shared)
                    .await;
                Ok(result)
            }
            Err(e) => {
                // Record failure in circuit breaker
                if let Some(breakers) = &self.breakers {
                    breakers.record_failure(&route_class).await;
                }
                // Execution failed - this is already tracked in ExecutionEngine stats
                Err(e)
            }
        };

        result
    }

    /// Select route without executing (for quote/preview)
    pub async fn select_route(&self, req: &LimitReq) -> Result<RouteSelection> {
        self.selector.select_route(req).await
    }
}

#[derive(Clone)]
struct IdemEntry {
    at: Instant,
    response: LimitOrderResponse,
}

#[derive(Debug, Deserialize)]
pub struct LimitOrderRequest {
    pub pool: String,
    pub price: f64,
    pub quantity: f64,
    pub is_bid: bool,
    pub client_order_id: String,
    pub pay_with_deep: Option<bool>,
    pub expiration_ms: Option<u64>,
}

#[derive(Debug, Serialize, Clone)]
pub struct LimitOrderResponse {
    pub digest: String,
    pub effects_time_ms: f64,
    pub checkpoint_time_ms: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct RouteQuoteResponse {
    pub plan: RoutePlanResponse,
    pub alternatives: Vec<RoutePlanResponse>,
}

#[derive(Debug, Serialize)]
pub struct RoutePlanResponse {
    pub route_type: String,
    pub total_cost: f64,
    pub l2_price: f64,
    pub slippage: f64,
    pub gas_cost: f64,
    pub latency_penalty: f64,
    pub risk_factor: f64,
    pub expected_latency_ms: u64,
    pub uses_shared_objects: bool,
    pub estimated_gas: u64,
}

#[derive(Debug, Serialize)]
pub struct ApiError {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

/// Create the HTTP router with API endpoints
pub fn create_api_router(router: Arc<Router>) -> AxumRouter {
    AxumRouter::new()
        .route("/health", get(health_check))
        .route("/openapi.json", get(openapi_json))
        .route("/docs", get(swagger_ui))
        .route("/metrics", get(metrics_endpoint))
        .route("/api/v1/quote", post(quote_route))
        .route("/api/v1/order", post(execute_order))
        .route("/api/v1/stats", get(get_stats))
        .route("/api/v1/latency", get(get_latency_stats))
        .route("/api/v1/latency", post(update_latency))
        .with_state(router)
}

/// Health check endpoint
async fn health_check() -> StatusCode {
    StatusCode::OK
}

/// Serve OpenAPI spec from a bundled JSON file
async fn openapi_json() -> Result<Json<serde_json::Value>, (StatusCode, Json<ApiError>)> {
    let spec_str = include_str!("../../openapi/openapi.json");
    let json: serde_json::Value = serde_json::from_str(spec_str).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError {
                code: "OPENAPI_PARSE".to_string(),
                message: format!("failed to parse openapi spec: {}", e),
                details: None,
            }),
        )
    })?;
    Ok(Json(json))
}

/// Serve Swagger UI backed by /openapi.json
async fn swagger_ui() -> Html<&'static str> {
    static HTML: &str = r#"<!doctype html>
<html>
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Ultra Aggregator API Docs</title>
    <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
  </head>
  <body>
    <div id="swagger-ui"></div>
    <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js" crossorigin></script>
    <script>
      window.onload = () => {
        window.ui = SwaggerUIBundle({
          url: '/openapi.json',
          dom_id: '#swagger-ui',
        });
      };
    </script>
  </body>
</html>"#;
    Html(HTML)
}

/// Prometheus metrics endpoint
async fn metrics_endpoint() -> Response {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();

    let status = match encoder.encode(&metric_families, &mut buffer) {
        Ok(_) => StatusCode::OK,
        Err(err) => {
            buffer = format!("metrics encoding error: {err}").into_bytes();
            StatusCode::INTERNAL_SERVER_ERROR
        }
    };

    Response::builder()
        .status(status)
        .header(axum::http::header::CONTENT_TYPE, encoder.format_type())
        .body(Body::from(buffer))
        .unwrap_or_else(|_| {
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from("failed to build metrics response"))
                .unwrap()
        })
}

fn validate_limit_order_req(req: &LimitOrderRequest) -> Result<(), ApiError> {
    if req.pool.trim().is_empty() {
        return Err(ApiError {
            code: "VALIDATION".to_string(),
            message: "pool must not be empty".to_string(),
            details: None,
        });
    }
    if !(req.price.is_finite() && req.price > 0.0) {
        return Err(ApiError {
            code: "VALIDATION".to_string(),
            message: "price must be a positive finite number".to_string(),
            details: None,
        });
    }
    if !(req.quantity.is_finite() && req.quantity > 0.0) {
        return Err(ApiError {
            code: "VALIDATION".to_string(),
            message: "quantity must be a positive finite number".to_string(),
            details: None,
        });
    }
    if req.client_order_id.trim().is_empty() || req.client_order_id.parse::<u64>().is_err() {
        return Err(ApiError {
            code: "VALIDATION".to_string(),
            message: "client_order_id must be a non-empty u64 string".to_string(),
            details: None,
        });
    }
    Ok(())
}

/// Quote route endpoint - returns route selection without executing
async fn quote_route(
    State(router): State<Arc<Router>>,
    Json(req): Json<LimitOrderRequest>,
) -> Result<Json<RouteQuoteResponse>, (StatusCode, Json<ApiError>)> {
    let span = info_span!(
        "http.quote_route",
        pool = %req.pool,
        is_bid = req.is_bid,
        client_order_id = %req.client_order_id
    );
    let _enter = span.enter();
    let _timer = REQ_LATENCY
        .with_label_values(&["http", "quote"])
        .start_timer();
    if let Err(e) = validate_limit_order_req(&req) {
        REQ_ERRORS.with_label_values(&["http", "quote"]).inc();
        return Err((StatusCode::BAD_REQUEST, Json(e)));
    }
    let limit_req = LimitReq {
        pool: req.pool,
        price: req.price,
        quantity: req.quantity,
        is_bid: req.is_bid,
        client_order_id: req.client_order_id,
        pay_with_deep: req.pay_with_deep.unwrap_or(false),
        expiration_ms: req.expiration_ms,
    };

    let selection = router.select_route(&limit_req).await.map_err(|e| {
        REQ_ERRORS.with_label_values(&["http", "quote"]).inc();
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError {
                code: "QUOTE_ERROR".to_string(),
                message: e.to_string(),
                details: None,
            }),
        )
    })?;

    let plan_response = RoutePlanResponse {
        route_type: format!("{:?}", selection.plan.route),
        total_cost: selection.plan.score.total_cost,
        l2_price: selection.plan.score.l2_price,
        slippage: selection.plan.score.slippage,
        gas_cost: selection.plan.score.gas_cost,
        latency_penalty: selection.plan.score.latency_penalty,
        risk_factor: selection.plan.score.risk_factor,
        expected_latency_ms: selection.plan.expected_latency_ms,
        uses_shared_objects: selection.plan.uses_shared_objects,
        estimated_gas: selection.plan.estimated_gas,
    };

    let alternatives: Vec<RoutePlanResponse> = selection
        .alternatives
        .into_iter()
        .map(|plan| RoutePlanResponse {
            route_type: format!("{:?}", plan.route),
            total_cost: plan.score.total_cost,
            l2_price: plan.score.l2_price,
            slippage: plan.score.slippage,
            gas_cost: plan.score.gas_cost,
            latency_penalty: plan.score.latency_penalty,
            risk_factor: plan.score.risk_factor,
            expected_latency_ms: plan.expected_latency_ms,
            uses_shared_objects: plan.uses_shared_objects,
            estimated_gas: plan.estimated_gas,
        })
        .collect();

    Ok(Json(RouteQuoteResponse {
        plan: plan_response,
        alternatives,
    }))
}

/// Execute order endpoint - routes and executes the order
async fn execute_order(
    State(router): State<Arc<Router>>,
    headers: HeaderMap,
    Json(req): Json<LimitOrderRequest>,
) -> Result<Json<LimitOrderResponse>, (StatusCode, Json<ApiError>)> {
    let span = info_span!(
        "http.execute_order",
        pool = %req.pool,
        is_bid = req.is_bid,
        client_order_id = %req.client_order_id,
        idempotency_key = field::Empty
    );
    let _enter = span.enter();
    let _timer = REQ_LATENCY
        .with_label_values(&["http", "order"])
        .start_timer();
    if let Err(e) = validate_limit_order_req(&req) {
        REQ_ERRORS.with_label_values(&["http", "order"]).inc();
        return Err((StatusCode::BAD_REQUEST, Json(e)));
    }
    let idem_key = headers
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    if let Some(idem) = &idem_key {
        span.record("idempotency_key", idem.as_str());
    }
    if let Some(ref key) = idem_key {
        if let Some(resp) = router.idem_get(key).await {
            return Ok(Json(resp));
        }
    }
    let limit_req = LimitReq {
        pool: req.pool,
        price: req.price,
        quantity: req.quantity,
        is_bid: req.is_bid,
        client_order_id: req.client_order_id,
        pay_with_deep: req.pay_with_deep.unwrap_or(false),
        expiration_ms: req.expiration_ms,
    };

    let result = router.execute_limit_order(&limit_req).await.map_err(|e| {
        REQ_ERRORS.with_label_values(&["http", "order"]).inc();
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError {
                code: "ORDER_ERROR".to_string(),
                message: e.to_string(),
                details: None,
            }),
        )
    })?;

    let response = LimitOrderResponse {
        digest: result.digest,
        effects_time_ms: result.effects_time_ms,
        checkpoint_time_ms: result.checkpoint_time_ms,
    };
    if let Some(key) = idem_key {
        router.idem_put(key, response.clone()).await;
    }
    Ok(Json(response))
}

#[derive(Debug, Serialize)]
pub struct StatsResponse {
    pub execution: ExecutionStats,
    pub latency: LatencyStats,
}

/// Get execution and latency statistics
async fn get_stats(
    State(router): State<Arc<Router>>,
) -> Result<Json<StatsResponse>, (StatusCode, Json<ApiError>)> {
    let execution_stats = router.executor().get_stats();
    let latency_stats = router.selector().get_latency_stats().await;

    Ok(Json(StatsResponse {
        execution: execution_stats,
        latency: latency_stats,
    }))
}

/// Get latency statistics
async fn get_latency_stats(
    State(router): State<Arc<Router>>,
) -> Result<Json<LatencyStats>, (StatusCode, Json<ApiError>)> {
    let stats = router.selector().get_latency_stats().await;
    Ok(Json(stats))
}

#[derive(Debug, Deserialize)]
pub struct UpdateLatencyRequest {
    pub base_latency_ms: Option<u64>,
    pub shared_latency_ms: Option<u64>,
}

/// Update latency estimates manually
async fn update_latency(
    State(router): State<Arc<Router>>,
    Json(req): Json<UpdateLatencyRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ApiError>)> {
    let selector = router.selector();
    let (current_base, current_shared) = selector.get_latency_estimates();

    let new_base = req.base_latency_ms.unwrap_or(current_base);
    let new_shared = req.shared_latency_ms.unwrap_or(current_shared);

    selector.update_latency_estimates(new_base, new_shared);

    Ok(Json(serde_json::json!({
        "base_latency_ms": new_base,
        "shared_latency_ms": new_shared,
        "previous_base_latency_ms": current_base,
        "previous_shared_latency_ms": current_shared,
    })))
}
