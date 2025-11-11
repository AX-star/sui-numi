// Sponsored transaction support module
// This file handles sponsored transaction building, budget tracking, and abuse detection
// for gasless user transactions
//
// Numan Thabit 2025 Nov

use crate::errors::AggrError;
use crate::signing::sign_tx_bcs_ed25519_to_serialized_signature;
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use sui_sdk::types::base_types::{ObjectID, ObjectRef, SuiAddress};
use sui_sdk::types::transaction::TransactionData;
use tokio::sync::RwLock;
use tracing::{info, warn};

/// Sponsored transaction request metadata
#[derive(Debug, Clone)]
pub struct SponsorshipRequest {
    /// User address initiating the transaction
    pub user_address: SuiAddress,
    /// Route plan being sponsored
    pub route_plan_id: String,
    /// Estimated gas cost
    pub estimated_gas: u64,
    /// Timestamp when request was created
    pub created_at: Instant,
}

/// Budget tracking for a user or route class
#[derive(Debug, Clone)]
pub struct Budget {
    /// Total budget allocated
    pub total_budget: u64,
    /// Amount spent so far
    pub spent: u64,
    /// Time window for budget reset (None = no reset)
    pub window: Option<Duration>,
    /// Last reset timestamp
    pub last_reset: Instant,
    /// Per-transaction limit
    pub per_tx_limit: u64,
}

impl Budget {
    pub fn new(total_budget: u64, per_tx_limit: u64, window: Option<Duration>) -> Self {
        Self {
            total_budget,
            spent: 0,
            window,
            last_reset: Instant::now(),
            per_tx_limit,
        }
    }

    /// Check if budget allows spending `amount`
    pub fn can_spend(&mut self, amount: u64) -> bool {
        // Reset budget if window expired
        if let Some(window) = self.window {
            if self.last_reset.elapsed() >= window {
                self.spent = 0;
                self.last_reset = Instant::now();
            }
        }

        // Check per-transaction limit
        if amount > self.per_tx_limit {
            return false;
        }

        // Check total budget
        self.spent + amount <= self.total_budget
    }

    /// Record spending
    pub fn spend(&mut self, amount: u64) {
        self.spent += amount;
    }

    /// Get remaining budget
    pub fn remaining(&self) -> u64 {
        self.total_budget.saturating_sub(self.spent)
    }
}

/// Abuse detection metrics
#[derive(Debug, Clone)]
struct AbuseMetrics {
    /// Number of transactions in current window
    tx_count: u64,
    /// Total gas spent in current window
    gas_spent: u64,
    /// Window start time
    window_start: Instant,
    /// Window duration
    window_duration: Duration,
}

impl AbuseMetrics {
    fn new(window_duration: Duration) -> Self {
        Self {
            tx_count: 0,
            gas_spent: 0,
            window_start: Instant::now(),
            window_duration,
        }
    }

    fn record_tx(&mut self, gas: u64) {
        // Reset if window expired
        if self.window_start.elapsed() >= self.window_duration {
            self.tx_count = 0;
            self.gas_spent = 0;
            self.window_start = Instant::now();
        }

        self.tx_count += 1;
        self.gas_spent += gas;
    }

    fn check_limits(&self, max_tx_per_window: u64, max_gas_per_window: u64) -> bool {
        // Reset if window expired
        if self.window_start.elapsed() >= self.window_duration {
            return true;
        }

        self.tx_count <= max_tx_per_window && self.gas_spent <= max_gas_per_window
    }
}

/// Sponsored transaction manager
pub struct SponsorshipManager {
    /// Sponsor's private key (hex-encoded Ed25519)
    sponsor_key_hex: String,
    /// Sponsor's address
    sponsor_address: SuiAddress,
    /// Sponsor's gas coins (object IDs)
    gas_coins: Arc<RwLock<Vec<ObjectID>>>,
    /// Per-user budgets
    user_budgets: Arc<RwLock<HashMap<SuiAddress, Budget>>>,
    /// Per-route-class budgets
    route_budgets: Arc<RwLock<HashMap<String, Budget>>>,
    /// Abuse detection per user
    abuse_metrics: Arc<RwLock<HashMap<SuiAddress, AbuseMetrics>>>,
    /// Abuse detection configuration
    abuse_config: AbuseConfig,
    /// Reference gas price (updated periodically)
    gas_price: Arc<RwLock<u64>>,
}

#[derive(Debug, Clone)]
pub struct AbuseConfig {
    /// Max transactions per user per window
    pub max_tx_per_window: u64,
    /// Max gas per user per window
    pub max_gas_per_window: u64,
    /// Abuse detection window duration
    pub window_duration: Duration,
}

impl Default for AbuseConfig {
    fn default() -> Self {
        Self {
            max_tx_per_window: 1000,
            max_gas_per_window: 100_000_000_000, // 100B gas units
            window_duration: Duration::from_secs(3600), // 1 hour
        }
    }
}

impl SponsorshipManager {
    /// Create a new sponsorship manager
    pub fn new(
        sponsor_key_hex: String,
        sponsor_address: SuiAddress,
        gas_price: u64,
        abuse_config: AbuseConfig,
    ) -> Result<Self> {
        Ok(Self {
            sponsor_key_hex,
            sponsor_address,
            gas_coins: Arc::new(RwLock::new(Vec::new())),
            user_budgets: Arc::new(RwLock::new(HashMap::new())),
            route_budgets: Arc::new(RwLock::new(HashMap::new())),
            abuse_metrics: Arc::new(RwLock::new(HashMap::new())),
            abuse_config,
            gas_price: Arc::new(RwLock::new(gas_price)),
        })
    }

    /// Update sponsor's gas coins
    pub async fn update_gas_coins(&self, coins: Vec<ObjectID>) {
        let mut gas_coins = self.gas_coins.write().await;
        *gas_coins = coins;
        info!(count = gas_coins.len(), "updated sponsor gas coins");
    }

    /// Get current sponsor gas coin IDs
    pub async fn gas_coin_ids(&self) -> Vec<ObjectID> {
        self.gas_coins.read().await.clone()
    }

    /// Update reference gas price
    pub async fn update_gas_price(&self, price: u64) {
        let mut gas_price = self.gas_price.write().await;
        *gas_price = price;
    }

    /// Set budget for a user
    pub async fn set_user_budget(
        &self,
        user: SuiAddress,
        total_budget: u64,
        per_tx_limit: u64,
        window: Option<Duration>,
    ) {
        let mut budgets = self.user_budgets.write().await;
        budgets.insert(user, Budget::new(total_budget, per_tx_limit, window));
        info!(
            user = %user,
            budget = total_budget,
            "set user sponsorship budget"
        );
    }

    /// Set budget for a route class
    pub async fn set_route_budget(
        &self,
        route_class: String,
        total_budget: u64,
        per_tx_limit: u64,
        window: Option<Duration>,
    ) {
        let mut budgets = self.route_budgets.write().await;
        let route_class_clone = route_class.clone();
        budgets.insert(route_class, Budget::new(total_budget, per_tx_limit, window));
        info!(
            route_class = %route_class_clone,
            budget = total_budget,
            "set route class sponsorship budget"
        );
    }

    /// Check if sponsorship is allowed for a request
    pub async fn can_sponsor(&self, req: &SponsorshipRequest) -> Result<bool> {
        // Check user budget
        {
            let mut budgets = self.user_budgets.write().await;
            if let Some(budget) = budgets.get_mut(&req.user_address) {
                if !budget.can_spend(req.estimated_gas) {
                    warn!(
                        user = %req.user_address,
                        estimated_gas = req.estimated_gas,
                        remaining = budget.remaining(),
                        "user budget exceeded"
                    );
                    return Ok(false);
                }
            }
        }

        // Check abuse metrics
        {
            let mut metrics = self.abuse_metrics.write().await;
            let user_metrics = metrics
                .entry(req.user_address)
                .or_insert_with(|| AbuseMetrics::new(self.abuse_config.window_duration));

            if !user_metrics.check_limits(
                self.abuse_config.max_tx_per_window,
                self.abuse_config.max_gas_per_window,
            ) {
                warn!(
                    user = %req.user_address,
                    tx_count = user_metrics.tx_count,
                    gas_spent = user_metrics.gas_spent,
                    "abuse limits exceeded"
                );
                return Ok(false);
            }
        }

        // Check route budget
        {
            let mut budgets = self.route_budgets.write().await;
            if let Some(budget) = budgets.get_mut(&req.route_plan_id) {
                if !budget.can_spend(req.estimated_gas) {
                    warn!(
                        route = %req.route_plan_id,
                        estimated_gas = req.estimated_gas,
                        remaining = budget.remaining(),
                        "route budget exceeded"
                    );
                    return Ok(false);
                }
            }
        }

        // Check gas coin availability
        {
            let gas_coins = self.gas_coins.read().await;
            if gas_coins.is_empty() {
                warn!("no sponsor gas coins available");
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// Build sponsored TransactionData with sponsor's gas objects
    /// Returns the BCS bytes of the TransactionData ready for signing
    /// Both user and sponsor sign the same TransactionData bytes
    pub async fn build_sponsored_transaction_data(
        &self,
        programmable: sui_sdk::types::transaction::TransactionKind,
        sender: SuiAddress,
        gas_object_refs: Vec<ObjectRef>,
        gas_budget: u64,
    ) -> Result<Vec<u8>> {
        if gas_object_refs.is_empty() {
            anyhow::bail!("no sponsor gas object refs provided");
        }

        // Get current gas price
        let gas_price = *self.gas_price.read().await;

        // Build sponsored TransactionData with sponsor's gas
        // TransactionData::new expects a single gas object ref, not a Vec
        // For sponsored transactions, we use the first gas object
        let gas_object_ref = gas_object_refs
            .first()
            .copied()
            .ok_or_else(|| anyhow::anyhow!("no gas object refs provided"))?;

        let sponsored_tx_data =
            TransactionData::new(programmable, sender, gas_object_ref, gas_budget, gas_price);

        // Serialize sponsored transaction
        let sponsored_tx_bcs =
            bcs::to_bytes(&sponsored_tx_data).context("serialize sponsored transaction")?;

        Ok(sponsored_tx_bcs)
    }

    /// Sign a sponsored transaction with the sponsor's key
    /// The transaction bytes should be from build_sponsored_transaction_data
    pub fn sign_sponsored_transaction(&self, tx_bcs: &[u8]) -> Result<Vec<u8>, AggrError> {
        let (sponsor_sig_bytes, _) =
            sign_tx_bcs_ed25519_to_serialized_signature(tx_bcs, &self.sponsor_key_hex)?;
        Ok(sponsor_sig_bytes)
    }

    /// Complete sponsored transaction flow:
    /// 1. Build TransactionData with sponsor's gas
    /// 2. Sign with sponsor's key
    /// 3. Record spending
    ///
    /// Returns (tx_bcs, sponsor_signature_bytes)
    pub async fn build_and_sign_sponsored_transaction(
        &self,
        programmable: sui_sdk::types::transaction::TransactionKind,
        sender: SuiAddress,
        gas_object_refs: Vec<ObjectRef>,
        gas_budget: u64,
    ) -> Result<(Vec<u8>, Vec<u8>)> {
        // Build TransactionData
        let tx_bcs = self
            .build_sponsored_transaction_data(programmable, sender, gas_object_refs, gas_budget)
            .await?;

        // Sign with sponsor's key
        let sponsor_sig_bytes = self.sign_sponsored_transaction(&tx_bcs)?;

        // Record spending
        self.apply_spending(sender, None, gas_budget).await;

        info!(
            user = %sender,
            gas_budget = gas_budget,
            "built and signed sponsored transaction"
        );

        Ok((tx_bcs, sponsor_sig_bytes))
    }

    /// Record spending for a user
    pub async fn apply_spending(&self, user: SuiAddress, route_class: Option<&str>, gas: u64) {
        self.record_spending_internal(user, gas).await;
        if let Some(route) = route_class {
            let mut budgets = self.route_budgets.write().await;
            if let Some(budget) = budgets.get_mut(route) {
                budget.spend(gas);
            }
        }
    }

    async fn record_spending_internal(&self, user: SuiAddress, gas: u64) {
        // Update user budget
        {
            let mut budgets = self.user_budgets.write().await;
            if let Some(budget) = budgets.get_mut(&user) {
                budget.spend(gas);
            }
        }

        // Update abuse metrics
        {
            let mut metrics = self.abuse_metrics.write().await;
            let user_metrics = metrics
                .entry(user)
                .or_insert_with(|| AbuseMetrics::new(self.abuse_config.window_duration));
            user_metrics.record_tx(gas);
        }
    }

    /// Get sponsor address
    pub fn sponsor_address(&self) -> SuiAddress {
        self.sponsor_address
    }

    /// Get remaining budget for a user
    pub async fn get_user_budget_remaining(&self, user: SuiAddress) -> Option<u64> {
        let budgets = self.user_budgets.read().await;
        budgets.get(&user).map(|b| b.remaining())
    }
}
