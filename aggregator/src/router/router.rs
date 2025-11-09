// Router HTTP API implementation
// This file provides HTTP endpoints for order routing and execution
//
// Numan Thabit 2025 Nov

use crate::venues::adapter::LimitReq;
use axum::{
    extract::State,
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router as AxumRouter,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use super::{RouteSelector, ExecutionEngine};
use crate::router::routes::RouteSelection;
use crate::router::execution::{ExecutionResult, ExecutionStats};
use crate::router::selector::LatencyStats;
use crate::control::{AdmissionControl, CircuitBreakers};
use crate::router::validation::validate_limit_order;
use anyhow::{Result, Context};
use tracing::{warn, error};

/// High-level Router that ties selection and execution together
pub struct Router {
    selector: Arc<RouteSelector>,
    executor: Arc<ExecutionEngine>,
    admission: Option<Arc<AdmissionControl>>,
    breakers: Option<Arc<CircuitBreakers>>,
}

impl Router {
    pub fn new(selector: Arc<RouteSelector>, executor: Arc<ExecutionEngine>) -> Self {
        Self { 
            selector, 
            executor,
            admission: None,
            breakers: None,
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
            validation.into_result()
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
                self.selector.record_latency(result.effects_time_ms, uses_shared).await;
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

#[derive(Debug, Serialize)]
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
pub struct ErrorResponse {
    pub error: String,
}

/// Create the HTTP router with API endpoints
pub fn create_api_router(router: Arc<Router>) -> AxumRouter {
    AxumRouter::new()
        .route("/health", get(health_check))
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

/// Quote route endpoint - returns route selection without executing
async fn quote_route(
    State(router): State<Arc<Router>>,
    Json(req): Json<LimitOrderRequest>,
) -> Result<Json<RouteQuoteResponse>, (StatusCode, Json<ErrorResponse>)> {
    let limit_req = LimitReq {
        pool: req.pool,
        price: req.price,
        quantity: req.quantity,
        is_bid: req.is_bid,
        client_order_id: req.client_order_id,
        pay_with_deep: req.pay_with_deep.unwrap_or(false),
        expiration_ms: req.expiration_ms,
    };

    let selection = router
        .select_route(&limit_req)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: e.to_string(),
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
    Json(req): Json<LimitOrderRequest>,
) -> Result<Json<LimitOrderResponse>, (StatusCode, Json<ErrorResponse>)> {
    let limit_req = LimitReq {
        pool: req.pool,
        price: req.price,
        quantity: req.quantity,
        is_bid: req.is_bid,
        client_order_id: req.client_order_id,
        pay_with_deep: req.pay_with_deep.unwrap_or(false),
        expiration_ms: req.expiration_ms,
    };

    let result = router
        .execute_limit_order(&limit_req)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: e.to_string(),
                }),
            )
        })?;

    Ok(Json(LimitOrderResponse {
        digest: result.digest,
        effects_time_ms: result.effects_time_ms,
        checkpoint_time_ms: result.checkpoint_time_ms,
    }))
}

#[derive(Debug, Serialize)]
pub struct StatsResponse {
    pub execution: ExecutionStats,
    pub latency: LatencyStats,
}

/// Get execution and latency statistics
async fn get_stats(
    State(router): State<Arc<Router>>,
) -> Result<Json<StatsResponse>, (StatusCode, Json<ErrorResponse>)> {
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
) -> Result<Json<LatencyStats>, (StatusCode, Json<ErrorResponse>)> {
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
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
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

