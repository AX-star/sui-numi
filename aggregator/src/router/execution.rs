// Execution engine - compiles routes to PTBs, signs, submits, and handles retries
// This file implements the execution plane that submits through Transaction Driver
// with idempotent retry logic
//
// Numan Thabit 2025 Nov

use crate::errors::AggrError;
use crate::metrics::DEEPBOOK_EVENT_COUNTER;
use crate::quant::{quantize_price, quantize_size};
use crate::router::routes::{Route, RoutePlan};
use crate::router::validator::ValidatorSelector;
use crate::signing::sign_tx_bcs_ed25519_to_serialized_signature;
use crate::sponsorship::{SponsorshipManager, SponsorshipRequest};
use crate::transport::grpc::sui::rpc::v2::ExecutedTransaction;
use crate::transport::grpc::GrpcClients;
use crate::transport::jsonrpc::JsonRpc;
use crate::venues::adapter::{BalanceSnapshot, DeepBookAdapter, LimitReq};
use anyhow::{Context, Result};
use backoff::{future::retry, ExponentialBackoff};
use bcs;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use sui_sdk::rpc_types::SuiEvent;
use sui_sdk::types::programmable_transaction_builder::ProgrammableTransactionBuilder;
use sui_sdk::types::transaction::{InputObjectKind, TransactionData, TransactionKind};
use tracing::{debug, info, warn};

const MICROS_PER_UNIT: f64 = 1_000_000.0;
const PRICE_TOLERANCE: f64 = 1e-6;

/// Execution statistics for monitoring
#[derive(Debug, Clone, serde::Serialize)]
pub struct ExecutionStats {
    pub total_executions: u64,
    pub successful_executions: u64,
    pub failed_executions: u64,
    pub avg_effects_time_ms: Option<f64>,
    pub avg_checkpoint_time_ms: Option<f64>,
    pub success_rate: f64,
    pub total_quote_fees: f64,
    pub total_deep_fees: f64,
    pub total_quote_rebates: f64,
    pub total_deep_rebates: f64,
    pub total_sponsored_gas: u64,
}

#[derive(Debug, Clone, Copy, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FeeKind {
    Maker,
    Taker,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct DeepBookAccounting {
    pub pool: String,
    pub fee_kind: FeeKind,
    pub fee_rate: f64,
    pub quote_fee: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deep_fee: Option<f64>,
    pub quote_rebate_delta: f64,
    pub deep_rebate_delta: f64,
    pub pay_with_deep: bool,
    pub stake_required: f64,
}

#[derive(Debug, Clone, serde::Serialize, Default)]
pub struct DeepBookEventStats {
    pub placed: u64,
    pub filled: u64,
    pub cancelled: u64,
    pub settled: u64,
    pub other: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_base_filled: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_quote_filled: Option<f64>,
}

#[derive(Debug, Clone, serde::Serialize, Default)]
pub struct ExecutionAccounting {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deepbook: Vec<DeepBookAccounting>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gas_used: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sponsor_gas_used: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deepbook_events: Option<DeepBookEventStats>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct OrderHandle {
    pub pool: String,
    pub order_id: u128,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_order_id: Option<u64>,
}

#[derive(Debug, Clone)]
struct OrderRecord {
    digest: String,
    pool: String,
    order_id: u128,
    client_order_id: Option<u64>,
}

#[derive(Default)]
struct OrderIndex {
    by_digest: HashMap<String, Vec<OrderRecord>>,
}

impl OrderIndex {
    fn insert(&mut self, record: OrderRecord) {
        self.by_digest
            .entry(record.digest.clone())
            .or_default()
            .push(record);
    }

    fn find_by_digest_pool(&self, digest: &str, pool: &str) -> Option<OrderRecord> {
        self.by_digest
            .get(digest)
            .and_then(|records| records.iter().rev().find(|rec| rec.pool == pool))
            .cloned()
    }
}

/// Execution result with timing information
#[derive(Debug, Clone)]
pub struct ExecutionResult {
    pub digest: String,
    pub executed: ExecutedTransaction,
    /// Time from submission to effects observed (milliseconds)
    pub effects_time_ms: f64,
    /// Time from submission to checkpoint inclusion (milliseconds)
    pub checkpoint_time_ms: Option<f64>,
    pub accounting: ExecutionAccounting,
    pub orders: Vec<OrderHandle>,
}

/// Execution engine that compiles routes to PTBs and executes them
pub struct ExecutionEngine {
    deepbook: Option<Arc<DeepBookAdapter>>,
    grpc: Arc<tokio::sync::Mutex<GrpcClients>>,
    jsonrpc: Arc<JsonRpc>,
    validator_selector: Arc<ValidatorSelector>,
    secret_key_hex: String,
    /// User's Sui address (derived from secret key or from config)
    user_address: sui_sdk::types::base_types::SuiAddress,
    /// Set of transaction digests we've seen (for idempotent retries)
    seen_digests: Arc<tokio::sync::RwLock<HashSet<String>>>,
    /// Use gRPC execution if available
    use_grpc_execute: bool,
    /// Optional sponsorship manager for sponsored transactions
    sponsorship: Option<Arc<SponsorshipManager>>,
    /// Execution statistics
    total_executions: AtomicU64,
    successful_executions: AtomicU64,
    failed_executions: AtomicU64,
    total_effects_time_ms: AtomicU64, // Sum of all effects times in milliseconds (as u64 * 1000 for precision)
    total_checkpoint_time_ms: AtomicU64, // Sum of all checkpoint times in milliseconds
    checkpoint_count: AtomicU64,
    total_quote_fees_micros: AtomicU64,
    total_deep_fees_micros: AtomicU64,
    total_quote_rebates_micros: AtomicU64,
    total_deep_rebates_micros: AtomicU64,
    total_sponsor_gas: AtomicU64,
    order_index: Arc<tokio::sync::RwLock<OrderIndex>>,
}

impl ExecutionEngine {
    pub fn new(
        deepbook: Option<Arc<DeepBookAdapter>>,
        grpc: GrpcClients,
        jsonrpc: JsonRpc,
        validator_selector: Arc<ValidatorSelector>,
        secret_key_hex: String,
        user_address: sui_sdk::types::base_types::SuiAddress,
        use_grpc_execute: bool,
    ) -> Self {
        Self {
            deepbook,
            grpc: Arc::new(tokio::sync::Mutex::new(grpc)),
            jsonrpc: Arc::new(jsonrpc),
            validator_selector,
            secret_key_hex,
            user_address,
            seen_digests: Arc::new(tokio::sync::RwLock::new(HashSet::new())),
            use_grpc_execute,
            sponsorship: None,
            total_executions: AtomicU64::new(0),
            successful_executions: AtomicU64::new(0),
            failed_executions: AtomicU64::new(0),
            total_effects_time_ms: AtomicU64::new(0),
            total_checkpoint_time_ms: AtomicU64::new(0),
            checkpoint_count: AtomicU64::new(0),
            total_quote_fees_micros: AtomicU64::new(0),
            total_deep_fees_micros: AtomicU64::new(0),
            total_quote_rebates_micros: AtomicU64::new(0),
            total_deep_rebates_micros: AtomicU64::new(0),
            total_sponsor_gas: AtomicU64::new(0),
            order_index: Arc::new(tokio::sync::RwLock::new(OrderIndex::default())),
        }
    }

    /// Set sponsorship manager for sponsored transactions
    pub fn with_sponsorship(mut self, sponsorship: Arc<SponsorshipManager>) -> Self {
        self.sponsorship = Some(sponsorship);
        self
    }

    /// Execute a route plan
    pub async fn execute(&self, plan: &RoutePlan) -> Result<ExecutionResult> {
        self.execute_with_sponsorship(plan, false).await
    }

    /// Set sponsorship manager for sponsored transactions
    pub fn set_sponsorship(&self, _sponsorship: Arc<SponsorshipManager>) {
        // Note: This requires interior mutability, so we'll need to wrap sponsorship in Arc<Mutex<Option<...>>>
        // For now, sponsorship should be set during construction
        warn!("set_sponsorship called but sponsorship is immutable after construction");
    }

    /// Get execution statistics
    pub fn get_stats(&self) -> ExecutionStats {
        let total = self.total_executions.load(Ordering::Relaxed);
        let successful = self.successful_executions.load(Ordering::Relaxed);
        let failed = self.failed_executions.load(Ordering::Relaxed);
        let total_effects_ms = self.total_effects_time_ms.load(Ordering::Relaxed) as f64 / 1000.0;
        let total_checkpoint_ms =
            self.total_checkpoint_time_ms.load(Ordering::Relaxed) as f64 / 1000.0;
        let checkpoint_count = self.checkpoint_count.load(Ordering::Relaxed);
        let total_quote_fees =
            self.total_quote_fees_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        let total_deep_fees =
            self.total_deep_fees_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        let total_quote_rebates =
            self.total_quote_rebates_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        let total_deep_rebates =
            self.total_deep_rebates_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        let total_sponsored_gas = self.total_sponsor_gas.load(Ordering::Relaxed);

        ExecutionStats {
            total_executions: total,
            successful_executions: successful,
            failed_executions: failed,
            avg_effects_time_ms: if successful > 0 {
                Some(total_effects_ms / successful as f64)
            } else {
                None
            },
            avg_checkpoint_time_ms: if checkpoint_count > 0 {
                Some(total_checkpoint_ms / checkpoint_count as f64)
            } else {
                None
            },
            success_rate: if total > 0 {
                successful as f64 / total as f64
            } else {
                0.0
            },
            total_quote_fees,
            total_deep_fees,
            total_quote_rebates,
            total_deep_rebates,
            total_sponsored_gas,
        }
    }

    /// Execute a route plan with optional sponsorship
    #[tracing::instrument(skip_all, fields(uses_sponsorship = use_sponsorship))]
    pub async fn execute_with_sponsorship(
        &self,
        plan: &RoutePlan,
        use_sponsorship: bool,
    ) -> Result<ExecutionResult> {
        self.total_executions.fetch_add(1, Ordering::Relaxed);

        let uses_deepbook = !Self::deepbook_requests(plan).is_empty();
        let pre_balances = if uses_deepbook {
            if let Some(adapter) = &self.deepbook {
                Self::collect_balance_snapshots(adapter, plan).await
            } else {
                HashMap::new()
            }
        } else {
            HashMap::new()
        };

        // 1. Compile route to PTB (may be gasless if sponsorship is enabled)
        let (tx_bcs, is_sponsored) = if use_sponsorship && self.sponsorship.is_some() {
            self.compile_route_sponsored(plan).await?
        } else {
            (self.compile_route(plan).await?, false)
        };

        // 2. Sign transaction(s)
        let signatures = if is_sponsored {
            // For sponsored transactions, we need both user and sponsor signatures
            self.sign_sponsored_transaction(&tx_bcs).await?
        } else {
            // Regular transaction: just user signature
            let (signature_bytes, _pubkey) =
                sign_tx_bcs_ed25519_to_serialized_signature(&tx_bcs, &self.secret_key_hex)
                    .map_err(|e| AggrError::Signing(e.to_string()))?;
            vec![signature_bytes]
        };

        // 3. Compute transaction digest (for idempotency check)
        let digest = self.compute_digest(&tx_bcs)?;

        // 4. Check if we've already seen this digest (idempotent retry)
        {
            let seen = self.seen_digests.read().await;
            if seen.contains(&digest) {
                warn!(
                    digest = %digest,
                    "transaction digest already seen, skipping duplicate execution"
                );
                self.failed_executions.fetch_add(1, Ordering::Relaxed);
                anyhow::bail!("transaction already executed: {}", digest);
            }
        }

        // 5. Submit and wait for execution
        let submit_start = Instant::now();
        let executed = match self.submit_with_retry(tx_bcs, signatures).await {
            Ok(executed) => executed,
            Err(e) => {
                self.failed_executions.fetch_add(1, Ordering::Relaxed);
                return Err(e);
            }
        };
        let submit_duration = submit_start.elapsed();

        // 6. Record digest to prevent duplicate execution
        {
            let mut seen = self.seen_digests.write().await;
            seen.insert(digest.clone());
        }

        // 7. Extract timing information
        let effects_time_ms = submit_duration.as_secs_f64() * 1000.0;

        // Record effects time for validator selection
        if let Some(endpoint) = self.validator_selector.select_best().await {
            self.validator_selector
                .record_effects_time(&endpoint, effects_time_ms)
                .await;
        }

        // 8. Extract checkpoint inclusion time if available
        // Check checkpoint info before moving executed into ExecutionResult
        let checkpoint_time_ms = if executed.checkpoint.is_some() {
            // ExecutedTransaction includes checkpoint sequence number and timestamp
            // The checkpoint timestamp is absolute, so we approximate checkpoint inclusion time
            // as effects_time_ms (since checkpoint inclusion typically happens shortly after effects)
            // In a more sophisticated implementation, we'd track submission wall-clock time
            // and compare against checkpoint timestamp for precise measurement
            if executed.timestamp.is_some() {
                // Checkpoint timestamp is available - use effects time as approximation
                // (checkpoint inclusion typically happens within a few seconds of effects)
                Some(effects_time_ms)
            } else {
                // No timestamp available, use effects time as approximation
                Some(effects_time_ms)
            }
        } else {
            // Transaction not yet included in a checkpoint (may be included in future checkpoint)
            None
        };

        // Update statistics
        self.successful_executions.fetch_add(1, Ordering::Relaxed);
        self.total_effects_time_ms
            .fetch_add((effects_time_ms * 1000.0) as u64, Ordering::Relaxed);

        if let Some(checkpoint_ms) = checkpoint_time_ms {
            self.total_checkpoint_time_ms
                .fetch_add((checkpoint_ms * 1000.0) as u64, Ordering::Relaxed);
            self.checkpoint_count.fetch_add(1, Ordering::Relaxed);
        }

        let mut accounting = ExecutionAccounting::default();
        let mut orders: Vec<OrderHandle> = Vec::new();
        let gas_used = Self::extract_gas_used(&executed);
        if let Some(gas) = gas_used {
            accounting.gas_used = Some(gas);
        }

        if uses_deepbook {
            if let Some(adapter) = &self.deepbook {
                let events = match adapter.deepbook_events_for_digest(&digest).await {
                    Ok(events) => events,
                    Err(err) => {
                        warn!(
                            digest = %digest,
                            error = %err,
                            "failed to fetch DeepBook events for analytics"
                        );
                        Vec::new()
                    }
                };

                if !events.is_empty() {
                    accounting.deepbook_events = Self::summarize_deepbook_events(&events);
                }

                match Self::build_deepbook_accounting(adapter, plan, &pre_balances).await {
                    Ok(breakdown) => accounting.deepbook = breakdown,
                    Err(err) => {
                        warn!(
                            error = %err,
                            "failed to compute DeepBook accounting for executed route"
                        );
                    }
                }

                orders = if events.is_empty() {
                    Self::collect_order_handles(adapter, plan, &digest, None).await
                } else {
                    Self::collect_order_handles(adapter, plan, &digest, Some(&events)).await
                };
            }
        }

        if is_sponsored {
            if let Some(gas) = gas_used {
                if let Some(manager) = &self.sponsorship {
                    let route_class = Self::route_class(plan);
                    manager
                        .apply_spending(self.user_address, Some(route_class.as_str()), gas)
                        .await;
                    accounting.sponsor_gas_used = Some(gas);
                    self.total_sponsor_gas.fetch_add(gas, Ordering::Relaxed);
                }
            } else {
                warn!(
                    digest = %digest,
                    "sponsored transaction executed but gas usage unavailable"
                );
            }
        }

        if !orders.is_empty() {
            self.record_order_handles(&digest, &orders).await;
        }

        self.update_fee_counters(&accounting);

        info!(
            digest = %digest,
            effects_ms = effects_time_ms,
            uses_shared = plan.uses_shared_objects,
            sponsored = is_sponsored,
            "route executed successfully"
        );

        Ok(ExecutionResult {
            digest,
            executed,
            effects_time_ms,
            checkpoint_time_ms,
            accounting,
            orders,
        })
    }

    /// Compile a route plan into a PTB (BCS TransactionData bytes)
    async fn compile_route(&self, plan: &RoutePlan) -> Result<Vec<u8>> {
        match &plan.route {
            crate::router::routes::Route::DeepBookSingle(req) => {
                let adapter = self
                    .deepbook
                    .as_ref()
                    .context("DeepBook adapter not available")?;
                adapter
                    .build_limit_order_ptb_bcs(req, false)
                    .await
                    .context("build DeepBook limit order PTB")
            }
            crate::router::routes::Route::MultiVenueSplit { deepbook } => {
                self.compile_multi_venue_split(deepbook.as_ref()).await
            }
            crate::router::routes::Route::CancelReplace {
                cancel_digest,
                existing_order_id,
                replace,
            } => {
                self.compile_cancel_replace(cancel_digest.as_deref(), *existing_order_id, replace)
                    .await
            }
            crate::router::routes::Route::CancelDeepBook { pool, order_id } => {
                self.compile_cancel(pool, *order_id).await
            }
            crate::router::routes::Route::FlashLoanArb { .. } => {
                // Flash loan routes require flash loan contract integration
                // For now, return an error indicating it needs implementation
                anyhow::bail!("flash-loan routes require flash loan contract integration - not yet implemented")
            }
        }
    }

    /// Compile a multi-venue split route into a single PTB
    async fn compile_multi_venue_split(
        &self,
        deepbook_req: Option<&crate::venues::adapter::LimitReq>,
    ) -> Result<Vec<u8>> {
        let mut ptb = ProgrammableTransactionBuilder::new();
        let mut has_commands = false;

        // Add DeepBook order if present
        if let Some(req) = deepbook_req {
            let adapter = self
                .deepbook
                .as_ref()
                .context("DeepBook adapter not available for multi-venue route")?;

            // Build DeepBook order command directly into the PTB
            use crate::quant::{quantize_price, quantize_size};
            use sui_deepbookv3::utils::config::MAX_TIMESTAMP;
            use sui_deepbookv3::utils::types::{
                OrderType, PlaceLimitOrderParams, SelfMatchingOptions,
            };

            // Quantize price and size
            let params = adapter.pool_params(&req.pool).await?;
            let q_px = quantize_price(req.price, params.tick_size)?;
            let q_sz = quantize_size(req.quantity, params.lot_size, params.min_size)?;

            let client_order_id = req
                .client_order_id
                .parse::<u64>()
                .context("client_order_id must parse to u64")?;

            let place_params = PlaceLimitOrderParams {
                pool_key: req.pool.clone(),
                balance_manager_key: adapter.manager_key.clone(),
                client_order_id,
                price: q_px,
                quantity: q_sz,
                is_bid: req.is_bid,
                expiration: Some(req.expiration_ms.unwrap_or(MAX_TIMESTAMP)),
                order_type: Some(OrderType::NoRestriction),
                self_matching_option: Some(SelfMatchingOptions::SelfMatchingAllowed),
                pay_with_deep: Some(req.pay_with_deep),
            };

            adapter
                .db
                .deep_book
                .place_limit_order(&mut ptb, place_params)
                .await
                .context("build DeepBook order command for multi-venue route")?;

            has_commands = true;
        }

        // Future: Add AMM orders here when AMM adapters are implemented
        // if let Some(amm_req) = amm_req {
        //     // Add AMM swap commands to PTB using AMM adapter
        //     // Example:
        //     // amm_adapter.build_swap_command(&mut ptb, amm_req).await?;
        //     has_commands = true;
        // }

        if !has_commands {
            anyhow::bail!("multi-venue route must have at least one venue order");
        }

        // Finalize PTB and build TransactionData
        let programmable = ptb.finish();
        let input_objects: Vec<_> = programmable
            .input_objects()
            .context("collect input objects")?
            .into_iter()
            .map(|obj| InputObjectKind::object_id(&obj))
            .collect();

        // Get gas price and select gas
        let adapter = self
            .deepbook
            .as_ref()
            .context("DeepBook adapter needed for gas selection")?;
        let gas_price = adapter
            .reference_gas_price()
            .await
            .context("fetch reference gas price")?;

        use sui_deepbookv3::utils::config::GAS_BUDGET;

        let gas = adapter
            .sui_client()
            .transaction_builder()
            .select_gas(
                self.user_address,
                None,
                GAS_BUDGET,
                input_objects,
                gas_price,
            )
            .await
            .context("select gas coin")?;

        let tx_data = TransactionData::new(
            TransactionKind::programmable(programmable),
            self.user_address,
            gas,
            GAS_BUDGET,
            gas_price,
        );

        let tx_bcs = bcs::to_bytes(&tx_data)
            .map_err(|e| AggrError::BuildTx(format!("serialize transaction: {}", e)))?;

        Ok(tx_bcs)
    }

    /// Compile a cancel-and-replace route into a single PTB
    async fn compile_cancel_replace(
        &self,
        cancel_digest: Option<&str>,
        existing_order_id: Option<u128>,
        replace: &crate::venues::adapter::LimitReq,
    ) -> Result<Vec<u8>> {
        let adapter = self
            .deepbook
            .as_ref()
            .context("DeepBook adapter not available")?;

        // Build a PTB that:
        // 1. Cancels the existing order (by digest)
        // 2. Places a new order

        let mut ptb = ProgrammableTransactionBuilder::new();

        // 1. Determine order id (either from stored state or by digest lookup)
        let order_id = if let Some(id) = existing_order_id {
            id
        } else {
            let digest = cancel_digest.ok_or_else(|| {
                anyhow::anyhow!("cancel_replace requires either existing_order_id or cancel_digest")
            })?;
            adapter
                .get_order_id_from_digest(digest, &replace.pool)
                .await
                .context("lookup order ID from transaction digest")?
                .ok_or_else(|| {
                    anyhow::anyhow!("could not find order ID in transaction digest: {}", digest)
                })?
        };

        info!(
            cancel_digest = ?cancel_digest,
            order_id = order_id,
            pool = replace.pool,
            "found order ID for cancel-replace"
        );

        // 2. Build cancel order command
        adapter
            .build_cancel_order_command(&mut ptb, &replace.pool, order_id)
            .await
            .context("build cancel order command")?;

        // 3. Build place order command
        let client_order_id = replace
            .client_order_id
            .parse::<u64>()
            .context("client_order_id must parse to u64")?;

        use crate::quant::{quantize_price, quantize_size};
        use sui_deepbookv3::utils::config::MAX_TIMESTAMP;
        use sui_deepbookv3::utils::types::{OrderType, PlaceLimitOrderParams, SelfMatchingOptions};

        // Quantize price and size
        let params = adapter.pool_params(&replace.pool).await?;
        let q_px = quantize_price(replace.price, params.tick_size)?;
        let q_sz = quantize_size(replace.quantity, params.lot_size, params.min_size)?;

        let place_params = PlaceLimitOrderParams {
            pool_key: replace.pool.clone(),
            balance_manager_key: adapter.manager_key.clone(),
            client_order_id,
            price: q_px,
            quantity: q_sz,
            is_bid: replace.is_bid,
            expiration: Some(replace.expiration_ms.unwrap_or(MAX_TIMESTAMP)),
            order_type: Some(OrderType::NoRestriction),
            self_matching_option: Some(SelfMatchingOptions::SelfMatchingAllowed),
            pay_with_deep: Some(replace.pay_with_deep),
        };

        adapter
            .db
            .deep_book
            .place_limit_order(&mut ptb, place_params)
            .await
            .context("build place order command")?;

        // 4. Finalize PTB and build TransactionData
        let programmable = ptb.finish();
        let input_objects: Vec<_> = programmable
            .input_objects()
            .context("collect input objects")?
            .into_iter()
            .map(|obj| InputObjectKind::object_id(&obj))
            .collect();

        let gas_price = adapter.reference_gas_price().await?;
        use sui_deepbookv3::utils::config::GAS_BUDGET;

        let gas = adapter
            .sui_client()
            .transaction_builder()
            .select_gas(
                self.user_address,
                None,
                GAS_BUDGET,
                input_objects,
                gas_price,
            )
            .await
            .context("select gas coin")?;

        let tx_data = TransactionData::new(
            TransactionKind::programmable(programmable),
            self.user_address,
            gas,
            GAS_BUDGET,
            gas_price,
        );

        let tx_bcs = bcs::to_bytes(&tx_data)
            .map_err(|e| AggrError::BuildTx(format!("serialize transaction: {}", e)))?;

        Ok(tx_bcs)
    }

    async fn compile_cancel(&self, pool: &str, order_id: u128) -> Result<Vec<u8>> {
        let adapter = self
            .deepbook
            .as_ref()
            .context("DeepBook adapter not available")?;
        adapter
            .build_cancel_order_ptb_bcs(pool, order_id)
            .await
            .context("build DeepBook cancel order PTB")
    }

    /// Compile a route plan into a sponsored PTB
    /// Returns (tx_bcs, is_sponsored)
    async fn compile_route_sponsored(&self, plan: &RoutePlan) -> Result<(Vec<u8>, bool)> {
        let sponsorship = self
            .sponsorship
            .as_ref()
            .context("sponsorship not available")?;

        // Check if sponsorship is allowed
        let req = SponsorshipRequest {
            user_address: self.user_address,
            route_plan_id: format!("{:?}", plan.route),
            estimated_gas: plan.estimated_gas,
            created_at: Instant::now(),
        };

        if !sponsorship.can_sponsor(&req).await? {
            warn!("sponsorship not allowed, falling back to regular transaction");
            return Ok((self.compile_route(plan).await?, false));
        }

        // Build gasless transaction
        match &plan.route {
            crate::router::routes::Route::DeepBookSingle(req) => {
                let adapter = self
                    .deepbook
                    .as_ref()
                    .context("DeepBook adapter not available")?;

                // Build gasless PTB (programmable transaction)
                let (programmable, _sender) = adapter
                    .build_limit_order_ptb_gasless(req)
                    .await
                    .context("build gasless DeepBook limit order PTB")?;

                // Resolve sponsor gas coin ObjectRefs
                let gas_coin_ids = sponsorship.gas_coin_ids().await;
                if gas_coin_ids.is_empty() {
                    anyhow::bail!("no sponsor gas coins available");
                }
                let gas_object_refs = adapter
                    .object_refs_for_ids(&gas_coin_ids)
                    .await
                    .context("resolve sponsor gas object refs")?;

                // Build TransactionData with sponsor gas; do not sign yet
                let tx_bcs = sponsorship
                    .build_sponsored_transaction_data(
                        programmable,
                        self.user_address,
                        gas_object_refs,
                        plan.estimated_gas.max(10_000_000), // fallback minimum
                    )
                    .await
                    .context("build sponsored transaction data")?;

                Ok((tx_bcs, true))
            }
            _ => {
                anyhow::bail!("sponsored transactions not yet implemented for this route type")
            }
        }
    }

    /// Sign a sponsored transaction (user + sponsor signatures)
    async fn sign_sponsored_transaction(&self, tx_bcs: &[u8]) -> Result<Vec<Vec<u8>>> {
        let sponsorship = self
            .sponsorship
            .as_ref()
            .context("sponsorship not available")?;

        // User signs
        let (user_sig, _) =
            sign_tx_bcs_ed25519_to_serialized_signature(tx_bcs, &self.secret_key_hex)
                .map_err(|e| AggrError::Signing(format!("user signing failed: {}", e)))?;

        // Sponsor signs
        let sponsor_sig = sponsorship.sign_sponsored_transaction(tx_bcs)?;

        Ok(vec![user_sig, sponsor_sig])
    }

    /// Submit transaction with idempotent retry logic
    async fn submit_with_retry(
        &self,
        tx_bcs: Vec<u8>,
        signatures: Vec<Vec<u8>>,
    ) -> Result<ExecutedTransaction> {
        let backoff = ExponentialBackoff {
            initial_interval: Duration::from_millis(100),
            max_interval: Duration::from_secs(5),
            max_elapsed_time: Some(Duration::from_secs(30)),
            multiplier: 2.0,
            ..Default::default()
        };

        let grpc_clone = self.grpc.clone();
        let jsonrpc_clone = self.jsonrpc.clone();
        let use_grpc = self.use_grpc_execute;

        retry(backoff, || {
            let tx_bcs = tx_bcs.clone();
            let signatures = signatures.clone();
            let grpc = grpc_clone.clone();
            let jsonrpc = jsonrpc_clone.clone();
            let use_grpc_exec = use_grpc;
            async move {
                let result = if use_grpc_exec {
                    Self::submit_grpc_internal(&grpc, &tx_bcs, &signatures).await
                } else {
                    Self::submit_jsonrpc_internal(&jsonrpc, &tx_bcs, &signatures).await
                };
                result.map_err(backoff::Error::transient)
            }
        })
        .await
        .map_err(|e| anyhow::anyhow!("submission failed after retries: {}", e))
    }

    /// Internal helper for gRPC submission (used by retry logic)
    async fn submit_grpc_internal(
        grpc: &Arc<tokio::sync::Mutex<GrpcClients>>,
        tx_bcs: &[u8],
        signatures: &[Vec<u8>],
    ) -> Result<ExecutedTransaction> {
        #[cfg(feature = "grpc-exec")]
        {
            use crate::transport::grpc::sui::rpc::v2::{Bcs, SignatureScheme, UserSignature};
            let mut grpc_guard = grpc.lock().await;

            // Convert all signatures to UserSignature format
            let user_signatures: Vec<UserSignature> = signatures
                .iter()
                .map(|sig_bytes| UserSignature {
                    bcs: Some(Bcs {
                        name: Some("sui.types.Signature".to_string()),
                        value: Some(sig_bytes.clone()),
                    }),
                    scheme: Some(SignatureScheme::Ed25519 as i32),
                    ..Default::default()
                })
                .collect();

            grpc_guard
                .execute_ptb(tx_bcs.to_vec(), user_signatures)
                .await
                .context("gRPC execute transaction")
        }

        #[cfg(not(feature = "grpc-exec"))]
        {
            let _ = (grpc, tx_bcs, signatures); // Suppress unused warnings when feature is disabled
            anyhow::bail!("gRPC execution not enabled (requires 'grpc-exec' feature)")
        }
    }

    /// Internal helper for JSON-RPC submission (used by retry logic)
    #[allow(unused_variables)]
    async fn submit_jsonrpc_internal(
        jsonrpc: &Arc<JsonRpc>,
        tx_bcs: &[u8],
        signatures: &[Vec<u8>],
    ) -> Result<ExecutedTransaction> {
        use base64::{engine::general_purpose::STANDARD_NO_PAD as B64, Engine as _};

        // Convert all signatures to base64
        let sigs_b64: Vec<String> = signatures
            .iter()
            .map(|sig_bytes| B64.encode(sig_bytes))
            .collect();

        let _resp = jsonrpc
            .execute_tx_block(tx_bcs, &sigs_b64)
            .await
            .map_err(|e| AggrError::Transport(e.to_string()))?;

        // JSON-RPC execution is supported but ExecutedTransaction conversion
        // requires parsing the full JSON response structure.
        // For now, return an error indicating gRPC should be used for full functionality.
        // In production, implement full JSON-RPC response parsing.
        anyhow::bail!(
            "JSON-RPC execution succeeded but ExecutedTransaction conversion not fully implemented. \
             Use gRPC execution (--features grpc-exec) for full functionality. Digest: {:?}",
            _resp.digest
        );
    }

    fn route_class(plan: &RoutePlan) -> String {
        format!("{:?}", plan.route)
    }

    fn deepbook_requests(plan: &RoutePlan) -> Vec<&LimitReq> {
        match &plan.route {
            Route::DeepBookSingle(req) => vec![req],
            Route::MultiVenueSplit { deepbook } => deepbook.iter().collect(),
            Route::CancelReplace { replace, .. } => vec![replace],
            Route::FlashLoanArb { .. } => Vec::new(),
            Route::CancelDeepBook { .. } => Vec::new(),
        }
    }

    async fn collect_balance_snapshots(
        adapter: &DeepBookAdapter,
        plan: &RoutePlan,
    ) -> HashMap<String, BalanceSnapshot> {
        let mut snapshots = HashMap::new();
        let mut seen = HashSet::new();

        for req in Self::deepbook_requests(plan) {
            let pool = req.pool.clone();
            if !seen.insert(pool.clone()) {
                continue;
            }

            adapter.invalidate_pool_balances(&pool).await;

            match adapter.balance_manager_balances(&pool).await {
                Ok(snapshot) => {
                    snapshots.insert(pool, snapshot);
                }
                Err(err) => {
                    warn!(pool = %pool, error = %err, "failed to fetch pre-trade balances");
                }
            }
        }

        snapshots
    }

    async fn build_deepbook_accounting(
        adapter: &DeepBookAdapter,
        plan: &RoutePlan,
        pre_balances: &HashMap<String, BalanceSnapshot>,
    ) -> Result<Vec<DeepBookAccounting>> {
        let mut breakdowns = Vec::new();

        for req in Self::deepbook_requests(plan) {
            let pool = req.pool.clone();

            adapter.invalidate_pool_balances(&pool).await;

            let params = match adapter.pool_params(&pool).await {
                Ok(params) => params,
                Err(err) => {
                    warn!(pool = %pool, error = %err, "failed to fetch pool params for accounting");
                    continue;
                }
            };

            let q_price = match quantize_price(req.price, params.tick_size) {
                Ok(price) => price,
                Err(err) => {
                    warn!(
                        pool = %pool,
                        error = %err,
                        "failed to quantize price for accounting"
                    );
                    continue;
                }
            };

            let q_size = match quantize_size(req.quantity, params.lot_size, params.min_size) {
                Ok(size) => size,
                Err(err) => {
                    warn!(
                        pool = %pool,
                        error = %err,
                        "failed to quantize size for accounting"
                    );
                    continue;
                }
            };

            let fee_kind = match Self::determine_fee_kind(adapter, req).await {
                Ok(kind) => kind,
                Err(err) => {
                    debug!(
                        pool = %pool,
                        error = %err,
                        "failed to determine fee kind; defaulting to taker"
                    );
                    FeeKind::Taker
                }
            };

            let trade_params = match adapter.trade_params(&pool).await {
                Ok(params) => params,
                Err(err) => {
                    warn!(pool = %pool, error = %err, "failed to fetch trade params");
                    continue;
                }
            };

            let fee_rate = match fee_kind {
                FeeKind::Maker => trade_params.maker_fee,
                FeeKind::Taker => trade_params.taker_fee,
            };

            let notional = q_price * q_size;
            let quote_fee = notional * fee_rate;
            if !quote_fee.is_finite() || quote_fee.is_sign_negative() {
                warn!(
                    pool = %pool,
                    quote_fee = quote_fee,
                    "computed quote fee is invalid"
                );
                continue;
            }

            let deep_fee = if req.pay_with_deep {
                match adapter.deep_price(&pool).await {
                    Ok(price) => {
                        if let Some(deep_per_quote) = price.deep_per_quote {
                            let deep_fee = quote_fee * deep_per_quote;
                            if deep_fee.is_finite() && !deep_fee.is_sign_negative() {
                                Some(deep_fee)
                            } else {
                                warn!(
                                    pool = %pool,
                                    deep_fee = deep_fee,
                                    "computed DEEP fee (quote path) is invalid"
                                );
                                None
                            }
                        } else if let Some(deep_per_base) = price.deep_per_base {
                            let base_fee = q_size * fee_rate;
                            let deep_fee = base_fee * deep_per_base;
                            if deep_fee.is_finite() && !deep_fee.is_sign_negative() {
                                Some(deep_fee)
                            } else {
                                warn!(
                                    pool = %pool,
                                    deep_fee = deep_fee,
                                    "computed DEEP fee (base path) is invalid"
                                );
                                None
                            }
                        } else {
                            None
                        }
                    }
                    Err(err) => {
                        warn!(pool = %pool, error = %err, "failed to fetch DEEP price for accounting");
                        None
                    }
                }
            } else {
                None
            };

            let post_snapshot = match adapter.balance_manager_balances(&pool).await {
                Ok(snapshot) => snapshot,
                Err(err) => {
                    warn!(pool = %pool, error = %err, "failed to fetch post-trade balances");
                    continue;
                }
            };

            let quote_rebate_delta = pre_balances
                .get(&pool)
                .map(|snap| post_snapshot.unclaimed_quote - snap.unclaimed_quote)
                .unwrap_or(0.0);
            let deep_rebate_delta = pre_balances
                .get(&pool)
                .map(|snap| post_snapshot.unclaimed_deep - snap.unclaimed_deep)
                .unwrap_or(0.0);

            breakdowns.push(DeepBookAccounting {
                pool,
                fee_kind,
                fee_rate,
                quote_fee,
                deep_fee,
                quote_rebate_delta,
                deep_rebate_delta,
                pay_with_deep: req.pay_with_deep,
                stake_required: trade_params.stake_required,
            });
        }

        Ok(breakdowns)
    }

    async fn determine_fee_kind(adapter: &DeepBookAdapter, req: &LimitReq) -> Result<FeeKind> {
        let mid_price = adapter.mid_price(&req.pool).await?;
        let is_taker = if req.is_bid {
            req.price + PRICE_TOLERANCE >= mid_price
        } else {
            req.price <= mid_price + PRICE_TOLERANCE
        };

        Ok(if is_taker {
            FeeKind::Taker
        } else {
            FeeKind::Maker
        })
    }

    fn extract_gas_used(executed: &ExecutedTransaction) -> Option<u64> {
        let effects = executed.effects.as_ref()?;
        let gas_used = effects.gas_used.as_ref()?;
        let computation = gas_used.computation_cost?;
        let storage = gas_used.storage_cost?;
        let rebate = gas_used.storage_rebate.unwrap_or(0);
        computation.checked_add(storage)?.checked_sub(rebate)
    }

    async fn collect_order_handles(
        adapter: &DeepBookAdapter,
        plan: &RoutePlan,
        digest: &str,
        events: Option<&[SuiEvent]>,
    ) -> Vec<OrderHandle> {
        let mut handles = Vec::new();
        let mut seen = HashSet::new();

        for req in Self::deepbook_requests(plan) {
            let pool = req.pool.clone();
            if !seen.insert(pool.clone()) {
                continue;
            }

            let mut order_id_opt =
                events.and_then(|ev| adapter.order_id_from_events(ev, &pool, digest));

            if order_id_opt.is_none() && events.is_none() {
                match adapter.get_order_id_from_digest(digest, &pool).await {
                    Ok(Some(order_id)) => order_id_opt = Some(order_id),
                    Ok(None) => (),
                    Err(err) => {
                        warn!(
                            digest = %digest,
                            pool = %pool,
                            error = %err,
                            "failed to derive DeepBook order id from transaction digest"
                        );
                        continue;
                    }
                }
            }

            match order_id_opt {
                Some(order_id) => {
                    let client_order_id = req.client_order_id.parse::<u64>().ok();
                    handles.push(OrderHandle {
                        pool,
                        order_id,
                        client_order_id,
                    });
                }
                None => {
                    debug!(
                        digest = %digest,
                        pool = %pool,
                        "no DeepBook order id found in transaction events"
                    );
                }
            }
        }

        handles
    }

    fn summarize_deepbook_events(events: &[SuiEvent]) -> Option<DeepBookEventStats> {
        if events.is_empty() {
            return None;
        }

        let mut stats = DeepBookEventStats::default();
        let mut total_base = 0.0;
        let mut total_quote = 0.0;

        for event in events {
            let module = event.type_.module.as_str();
            let name = event.type_.name.as_str();

            if module != "clob" && module != "clob_v2" {
                continue;
            }

            match name {
                "OrderPlaced" | "OrderPlacedV2" => {
                    stats.placed += 1;
                    DEEPBOOK_EVENT_COUNTER.with_label_values(&["placed"]).inc();
                }
                "OrderFilled" | "OrderFilledV2" | "OrderMatch" | "OrderMatched" | "OrderFill" => {
                    stats.filled += 1;
                    DEEPBOOK_EVENT_COUNTER.with_label_values(&["filled"]).inc();
                    if let Some(base) = Self::event_f64_field(
                        &event.parsed_json,
                        &[
                            "filledQuantity",
                            "baseFilled",
                            "quantityFilled",
                            "base_filled",
                        ],
                    ) {
                        total_base += base;
                    }
                    if let Some(quote) =
                        Self::event_f64_field(&event.parsed_json, &["quoteFilled", "quote_filled"])
                    {
                        total_quote += quote;
                    }
                }
                "OrderCancelled" | "OrderCanceled" => {
                    stats.cancelled += 1;
                    DEEPBOOK_EVENT_COUNTER
                        .with_label_values(&["cancelled"])
                        .inc();
                }
                "OrderSettled" | "OrderSettledV2" => {
                    stats.settled += 1;
                    DEEPBOOK_EVENT_COUNTER.with_label_values(&["settled"]).inc();
                }
                _ => {
                    stats.other += 1;
                    DEEPBOOK_EVENT_COUNTER.with_label_values(&["other"]).inc();
                }
            }
        }

        if stats.placed == 0
            && stats.filled == 0
            && stats.cancelled == 0
            && stats.settled == 0
            && stats.other == 0
        {
            return None;
        }

        if total_base > 0.0 {
            stats.total_base_filled = Some(total_base);
        }
        if total_quote > 0.0 {
            stats.total_quote_filled = Some(total_quote);
        }

        Some(stats)
    }

    fn event_f64_field(value: &Value, keys: &[&str]) -> Option<f64> {
        for key in keys {
            if let Some(v) = value.get(*key) {
                match v {
                    Value::Number(n) => {
                        if let Some(f) = n.as_f64() {
                            return Some(f);
                        }
                    }
                    Value::String(s) => {
                        if let Ok(f) = s.parse::<f64>() {
                            return Some(f);
                        }
                    }
                    Value::Object(obj) => {
                        if let Some(Value::Number(n)) = obj.get("value") {
                            if let Some(f) = n.as_f64() {
                                return Some(f);
                            }
                        } else if let Some(Value::String(s)) = obj.get("value") {
                            if let Ok(f) = s.parse::<f64>() {
                                return Some(f);
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        None
    }

    async fn record_order_handles(&self, digest: &str, handles: &[OrderHandle]) {
        if handles.is_empty() {
            return;
        }
        let mut index = self.order_index.write().await;
        for handle in handles {
            index.insert(OrderRecord {
                digest: digest.to_string(),
                pool: handle.pool.clone(),
                order_id: handle.order_id,
                client_order_id: handle.client_order_id,
            });
        }
    }

    pub async fn order_handle_from_digest(&self, digest: &str, pool: &str) -> Option<OrderHandle> {
        let index = self.order_index.read().await;
        index
            .find_by_digest_pool(digest, pool)
            .map(|record| OrderHandle {
                pool: record.pool,
                order_id: record.order_id,
                client_order_id: record.client_order_id,
            })
    }

    pub async fn record_external_order(&self, digest: String, handle: OrderHandle) {
        let mut index = self.order_index.write().await;
        index.insert(OrderRecord {
            digest,
            pool: handle.pool.clone(),
            order_id: handle.order_id,
            client_order_id: handle.client_order_id,
        });
    }

    fn update_fee_counters(&self, accounting: &ExecutionAccounting) {
        if accounting.deepbook.is_empty() {
            return;
        }

        let total_quote_fees: f64 = accounting
            .deepbook
            .iter()
            .map(|entry| entry.quote_fee.max(0.0))
            .sum();
        let total_deep_fees: f64 = accounting
            .deepbook
            .iter()
            .filter_map(|entry| entry.deep_fee)
            .map(|value| value.max(0.0))
            .sum();
        let total_quote_rebates: f64 = accounting
            .deepbook
            .iter()
            .map(|entry| entry.quote_rebate_delta.max(0.0))
            .sum();
        let total_deep_rebates: f64 = accounting
            .deepbook
            .iter()
            .map(|entry| entry.deep_rebate_delta.max(0.0))
            .sum();

        self.total_quote_fees_micros
            .fetch_add(Self::to_micros(total_quote_fees), Ordering::Relaxed);
        self.total_deep_fees_micros
            .fetch_add(Self::to_micros(total_deep_fees), Ordering::Relaxed);
        self.total_quote_rebates_micros
            .fetch_add(Self::to_micros(total_quote_rebates), Ordering::Relaxed);
        self.total_deep_rebates_micros
            .fetch_add(Self::to_micros(total_deep_rebates), Ordering::Relaxed);
    }

    fn to_micros(value: f64) -> u64 {
        if !value.is_finite() || value <= 0.0 {
            return 0;
        }
        let scaled = (value * MICROS_PER_UNIT).round();
        if scaled >= u64::MAX as f64 {
            u64::MAX
        } else {
            scaled as u64
        }
    }

    /// Compute transaction digest from BCS bytes
    fn compute_digest(&self, tx_bcs: &[u8]) -> Result<String> {
        use blake2::{Blake2b512, Digest};
        let mut hasher = Blake2b512::new();
        hasher.update(tx_bcs);
        let hash = hasher.finalize();
        Ok(hex::encode(&hash[..32]))
    }
}
