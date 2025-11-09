// Route types and route selection logic
// This file defines route types, route plans, and scoring mechanisms
// for selecting optimal execution paths
//
// Numan Thabit 2025 Nov

use crate::venues::adapter::LimitReq;

/// Represents a route strategy that can be compiled into a PTB
#[derive(Debug, Clone)]
pub enum Route {
    /// Single-leg order on DeepBook
    DeepBookSingle(LimitReq),
    /// Multi-venue split route (e.g., DeepBook + AMM)
    MultiVenueSplit {
        deepbook: Option<LimitReq>,
        // Future: AMM routes will be added here when AMM adapters are implemented
        // amm_swap: Option<AmmSwapReq>,
    },
    /// Cancel and replace chain
    CancelReplace {
        cancel_digest: String,
        replace: LimitReq,
    },
    /// Flash-loan backed arbitrage (future)
    FlashLoanArb {
        // TODO: Define flash loan route structure
    },
}

/// Route plan with execution metadata
#[derive(Debug, Clone)]
pub struct RoutePlan {
    pub route: Route,
    pub score: RouteScore,
    /// Expected execution latency in milliseconds
    pub expected_latency_ms: u64,
    /// Whether this route uses shared objects (requires consensus)
    pub uses_shared_objects: bool,
    /// Estimated gas cost
    pub estimated_gas: u64,
}

/// Route scoring based on price-of-execution
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct RouteScore {
    /// Total cost including L2 price, slippage, gas, and latency penalty
    pub total_cost: f64,
    /// Base L2 price from the venue
    pub l2_price: f64,
    /// Expected fill slippage
    pub slippage: f64,
    /// Gas cost in quote units
    pub gas_cost: f64,
    /// Latency penalty (higher for shared-object routes)
    pub latency_penalty: f64,
    /// Venue failure risk factor
    pub risk_factor: f64,
}

impl RouteScore {
    /// Calculate total cost from components
    pub fn new(
        l2_price: f64,
        slippage: f64,
        gas_cost: f64,
        latency_penalty: f64,
        risk_factor: f64,
    ) -> Self {
        let total_cost = l2_price + slippage + gas_cost + latency_penalty + risk_factor;
        Self {
            total_cost,
            l2_price,
            slippage,
            gas_cost,
            latency_penalty,
            risk_factor,
        }
    }

    /// Calculate latency penalty based on route type and expected latency
    pub fn latency_penalty_for_route(
        uses_shared_objects: bool,
        expected_latency_ms: u64,
        base_latency_ms: u64,
    ) -> f64 {
        if uses_shared_objects {
            // Shared-object routes pay higher penalty
            // Penalty scales with excess latency over fast-path baseline
            let excess_ms = expected_latency_ms.saturating_sub(base_latency_ms);
            // Convert to cost: assume ~0.01% per 100ms excess latency
            (excess_ms as f64 / 100.0) * 0.0001
        } else {
            // Fast-path routes have minimal penalty
            0.0
        }
    }
}

impl RoutePlan {
    /// Create a route plan for a DeepBook single-leg order
    pub fn deepbook_single(
        req: LimitReq,
        l2_price: f64,
        slippage: f64,
        gas_cost: f64,
        expected_latency_ms: u64,
        base_latency_ms: u64,
        risk_factor: f64,
    ) -> Self {
        // DeepBook uses shared BalanceManager, so it requires consensus
        let uses_shared_objects = true;
        let latency_penalty = RouteScore::latency_penalty_for_route(
            uses_shared_objects,
            expected_latency_ms,
            base_latency_ms,
        );

        let score = RouteScore::new(l2_price, slippage, gas_cost, latency_penalty, risk_factor);

        Self {
            route: Route::DeepBookSingle(req),
            score,
            expected_latency_ms,
            uses_shared_objects,
            estimated_gas: 10_000_000, // Default estimate, should be refined
        }
    }

    /// Compare route plans - lower total_cost is better
    pub fn compare(&self, other: &Self) -> std::cmp::Ordering {
        self.score
            .total_cost
            .partial_cmp(&other.score.total_cost)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

/// Route selection result
#[derive(Debug)]
pub struct RouteSelection {
    pub plan: RoutePlan,
    pub alternatives: Vec<RoutePlan>,
}

impl RouteSelection {
    pub fn best_plan(&self) -> &RoutePlan {
        &self.plan
    }
}

