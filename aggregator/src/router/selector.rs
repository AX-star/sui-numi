// Route selector - chooses optimal routes based on price-of-execution
//  implements route selection logic that considers L2 price,
// slippage, gas, latency penalties, and venue failure risk
//
// Numan Thabit 2025 Nov

use crate::router::routes::{RoutePlan, RouteSelection};
use crate::venues::adapter::{DeepBookAdapter, LimitReq};
use anyhow::{Context, Result};
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use std::collections::VecDeque;
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Route selector that evaluates and selects optimal execution paths
pub struct RouteSelector {
    deepbook: Option<Arc<DeepBookAdapter>>,
    /// Base latency for fast-path routes (owned objects) in milliseconds
    base_latency_ms: AtomicU64,
    /// Current expected latency for shared-object routes
    shared_object_latency_ms: AtomicU64,
    /// Recent latency observations for owned-object routes (for adaptive updates)
    owned_latency_samples: Arc<RwLock<VecDeque<f64>>>,
    /// Recent latency observations for shared-object routes (for adaptive updates)
    shared_latency_samples: Arc<RwLock<VecDeque<f64>>>,
    /// Maximum number of samples to keep for latency tracking
    max_samples: usize,
    /// EWMA alpha for latency updates (0.0-1.0, higher = more weight to recent observations)
    latency_alpha: f64,
}

impl RouteSelector {
    pub fn new(
        deepbook: Option<Arc<DeepBookAdapter>>,
        base_latency_ms: u64,
        shared_object_latency_ms: u64,
    ) -> Self {
        Self {
            deepbook,
            base_latency_ms: AtomicU64::new(base_latency_ms),
            shared_object_latency_ms: AtomicU64::new(shared_object_latency_ms),
            owned_latency_samples: Arc::new(RwLock::new(VecDeque::new())),
            shared_latency_samples: Arc::new(RwLock::new(VecDeque::new())),
            max_samples: 100,
            latency_alpha: 0.1, // 10% weight to new observations
        }
    }

    /// Get the DeepBook adapter if available
    pub fn deepbook_adapter(&self) -> Option<&Arc<DeepBookAdapter>> {
        self.deepbook.as_ref()
    }

    /// Record an observed execution latency
    /// This is called after execution completes to update latency estimates
    pub async fn record_latency(&self, latency_ms: f64, uses_shared_objects: bool) {
        let samples = if uses_shared_objects {
            &self.shared_latency_samples
        } else {
            &self.owned_latency_samples
        };

        let mut samples = samples.write().await;
        samples.push_back(latency_ms);
        
        // Keep only recent samples
        while samples.len() > self.max_samples {
            samples.pop_front();
        }

        // Update estimate using EWMA if we have enough samples
        if samples.len() >= 10 {
            let current_estimate = if uses_shared_objects {
                self.shared_object_latency_ms.load(Ordering::Relaxed) as f64
            } else {
                self.base_latency_ms.load(Ordering::Relaxed) as f64
            };

            // Calculate average of recent samples
            let recent_avg: f64 = samples.iter().sum::<f64>() / samples.len() as f64;
            
            // Update using EWMA
            let new_estimate = (self.latency_alpha * recent_avg) + ((1.0 - self.latency_alpha) * current_estimate);
            
            if uses_shared_objects {
                self.shared_object_latency_ms.store(new_estimate as u64, Ordering::Relaxed);
            } else {
                self.base_latency_ms.store(new_estimate as u64, Ordering::Relaxed);
            }

            debug!(
                latency_ms = latency_ms,
                uses_shared = uses_shared_objects,
                new_estimate = new_estimate as u64,
                samples = samples.len(),
                "updated latency estimate from observation"
            );
        }
    }

    /// Get current latency estimates
    pub fn get_latency_estimates(&self) -> (u64, u64) {
        (
            self.base_latency_ms.load(Ordering::Relaxed),
            self.shared_object_latency_ms.load(Ordering::Relaxed),
        )
    }

    /// Get latency statistics
    pub async fn get_latency_stats(&self) -> LatencyStats {
        let owned_samples = self.owned_latency_samples.read().await;
        let shared_samples = self.shared_latency_samples.read().await;

        LatencyStats {
            base_latency_ms: self.base_latency_ms.load(Ordering::Relaxed),
            shared_latency_ms: self.shared_object_latency_ms.load(Ordering::Relaxed),
            owned_samples: owned_samples.len(),
            shared_samples: shared_samples.len(),
            owned_avg: if owned_samples.is_empty() {
                None
            } else {
                Some(owned_samples.iter().sum::<f64>() / owned_samples.len() as f64)
            },
            shared_avg: if shared_samples.is_empty() {
                None
            } else {
                Some(shared_samples.iter().sum::<f64>() / shared_samples.len() as f64)
            },
        }
    }

    /// Select optimal route for a limit order request
    #[tracing::instrument(skip_all, fields(pool = %req.pool, side = if req.is_bid { "bid" } else { "ask" }))]
    pub async fn select_route(&self, req: &LimitReq) -> Result<RouteSelection> {
        let mut alternatives = Vec::new();

        // Evaluate DeepBook route if adapter is available
        if let Some(adapter) = &self.deepbook {
            match self.evaluate_deepbook_route(adapter, req).await {
                Ok(plan) => {
                    debug!(
                        pool = %req.pool,
                        side = if req.is_bid { "bid" } else { "ask" },
                        total_cost = plan.score.total_cost,
                        latency_ms = plan.expected_latency_ms,
                        "evaluated DeepBook route"
                    );
                    alternatives.push(plan);
                }
                Err(e) => {
                    debug!(
                        error = %e,
                        pool = %req.pool,
                        "failed to evaluate DeepBook route"
                    );
                }
            }
        }

        // Future: Evaluate other venues (AMMs, etc.)
        // For now, we only have DeepBook

        if alternatives.is_empty() {
            anyhow::bail!("no viable routes found for order");
        }

        // Sort by total cost (lower is better)
        alternatives.sort_by(|a, b| a.compare(b));

        let best = alternatives.remove(0);
        info!(
            pool = %req.pool,
            best_cost = best.score.total_cost,
            best_latency_ms = best.expected_latency_ms,
            alternatives = alternatives.len(),
            "selected best route"
        );

        Ok(RouteSelection {
            plan: best,
            alternatives,
        })
    }

    /// Evaluate a DeepBook route with real order book data
    async fn evaluate_deepbook_route(
        &self,
        adapter: &DeepBookAdapter,
        req: &LimitReq,
    ) -> Result<RoutePlan> {
        // Fetch pool parameters for quantization and pricing
        let pool_params = adapter
            .pool_params(&req.pool)
            .await
            .context("fetch pool parameters")?;

        // Get mid price from DeepBook
        let mid_price = adapter
            .mid_price(&req.pool)
            .await
            .context("fetch mid price")?;

        // Use mid price as L2 price, or requested price if it's better
        let l2_price = if req.is_bid {
            // For bids, use the better of requested price or mid price
            req.price.max(mid_price)
        } else {
            // For asks, use the better of requested price or mid price
            req.price.min(mid_price)
        };

        // Fetch level 2 order book data for slippage estimation
        // Get 20 ticks from mid (adjustable based on needs)
        let level2 = adapter
            .level2_ticks_from_mid(&req.pool, 20)
            .await
            .context("fetch level2 order book")?;

        // Calculate expected slippage based on order book depth
        let slippage = self.calculate_slippage(
            req.price,
            req.quantity,
            req.is_bid,
            &level2,
            &pool_params,
        )?;

        // Fetch trade parameters for fee estimation
        let trade_params = adapter
            .trade_params(&req.pool)
            .await
            .context("fetch trade parameters")?;

        // Fetch real gas price from network
        let gas_price_per_unit = adapter
            .reference_gas_price()
            .await
            .context("fetch reference gas price")?;

        // Estimate gas cost (for limit orders, assume ~10M gas units)
        let gas_units = 10_000_000u64;
        let gas_cost_sui = (gas_units as f64 * gas_price_per_unit as f64) / 1e9;
        let gas_cost = gas_cost_sui * l2_price; // Convert to quote units

        // Add maker/taker fee to cost
        let fee_rate = if req.is_bid {
            // If placing a bid above mid, likely to be taker
            if req.price >= mid_price {
                trade_params.taker_fee
            } else {
                trade_params.maker_fee
            }
        } else {
            // If placing an ask below mid, likely to be taker
            if req.price <= mid_price {
                trade_params.taker_fee
            } else {
                trade_params.maker_fee
            }
        };
        let fee_cost = req.quantity * req.price * fee_rate;

        // DeepBook uses shared BalanceManager, so it requires consensus
        let expected_latency_ms = self.shared_object_latency_ms.load(Ordering::Relaxed);

        // Venue failure risk (DeepBook is native, so low risk)
        let risk_factor = req.price * req.quantity * 0.00001; // 0.001% risk

        Ok(RoutePlan::deepbook_single(
            req.clone(),
            l2_price,
            slippage + fee_cost,
            gas_cost,
            expected_latency_ms,
            self.base_latency_ms.load(Ordering::Relaxed),
            risk_factor,
        ))
    }

    /// Calculate expected slippage based on order book depth
    fn calculate_slippage(
        &self,
        price: f64,
        quantity: f64,
        is_bid: bool,
        level2: &sui_deepbookv3::client::Level2TicksFromMid,
        pool_params: &crate::quant::PoolParams,
    ) -> Result<f64> {
        let (prices, quantities) = if is_bid {
            (&level2.bid_prices, &level2.bid_quantities)
        } else {
            (&level2.ask_prices, &level2.ask_quantities)
        };

        if prices.is_empty() || quantities.is_empty() {
            // No liquidity, assume high slippage
            return Ok(price * quantity * 0.01); // 1% slippage
        }

        // Find the price level that would fill our order
        let mut remaining_qty = quantity;
        let mut total_cost = 0.0;

        for (p, q) in prices.iter().zip(quantities.iter()) {
            if remaining_qty <= 0.0 {
                break;
            }

            let fill_qty = remaining_qty.min(*q);
            let cost = fill_qty * *p;
            total_cost += cost;
            remaining_qty -= fill_qty;
        }

        // If order can't be fully filled at current levels, estimate slippage
        if remaining_qty > 0.0 {
            // Use worst-case: assume remaining fills at last price + tick_size
            let last_price = prices.last().copied().unwrap_or(price);
            let tick_size = pool_params.tick_size;
            let worst_price = if is_bid {
                last_price - tick_size // Bids go down
            } else {
                last_price + tick_size // Asks go up
            };
            total_cost += remaining_qty * worst_price;
        }

        let avg_fill_price = total_cost / quantity;
        let slippage = if is_bid {
            // For bids, slippage is when we pay more than requested
            (avg_fill_price - price).max(0.0) * quantity
        } else {
            // For asks, slippage is when we receive less than requested
            (price - avg_fill_price).max(0.0) * quantity
        };

        Ok(slippage)
    }

    /// Update latency estimates based on recent observations
    /// This method can be called from multiple threads safely
    pub fn update_latency_estimates(&self, base_ms: u64, shared_ms: u64) {
        self.base_latency_ms.store(base_ms, Ordering::Relaxed);
        self.shared_object_latency_ms.store(shared_ms, Ordering::Relaxed);
        debug!(
            base_latency_ms = base_ms,
            shared_latency_ms = shared_ms,
            "updated latency estimates"
        );
    }
}

/// Latency statistics for monitoring
#[derive(Debug, Clone, serde::Serialize)]
pub struct LatencyStats {
    pub base_latency_ms: u64,
    pub shared_latency_ms: u64,
    pub owned_samples: usize,
    pub shared_samples: usize,
    pub owned_avg: Option<f64>,
    pub shared_avg: Option<f64>,
}

