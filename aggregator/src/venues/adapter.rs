// Venue adapter module
// This file implements the adapter pattern for integrating different venue implementations
// into the aggregator's core logic
//
// Numan Thabit 2025 Nov

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::str::FromStr;
use sui_deepbookv3::client::{DeepBookClient, PoolBookParams};
use sui_deepbookv3::utils::config::{Environment, GAS_BUDGET, MAX_TIMESTAMP};
use sui_deepbookv3::utils::types::{
    BalanceManager, OrderType, PlaceLimitOrderParams, SelfMatchingOptions,
};
use sui_sdk::types::base_types::SuiAddress;
use sui_sdk::types::programmable_transaction_builder::ProgrammableTransactionBuilder;
use sui_sdk::types::base_types::ObjectRef;
use sui_sdk::types::transaction::{InputObjectKind, TransactionData, TransactionKind};
use sui_sdk::{SuiClient, SuiClientBuilder};
use tracing::{info, warn};

use crate::quant::{quantize_price, quantize_size, PoolParams};

#[derive(Debug, Clone)]
pub struct LimitReq {
    pub pool: String,
    pub price: f64,
    pub quantity: f64,
    pub is_bid: bool,
    pub client_order_id: String,
    pub pay_with_deep: bool,
    pub expiration_ms: Option<u64>,
}

#[derive(Clone)]
pub struct DeepBookAdapter {
    sui: SuiClient,
    pub(crate) db: DeepBookClient,
    sender: SuiAddress,
    pub(crate) manager_key: String, // key used inside DeepBookClient config, e.g. "MANAGER_1"
}

impl DeepBookAdapter {
    pub async fn new(
        indexer_base: &str,
        fullnode_url: &str,
        sender: SuiAddress,
        manager_object: &str, // 0x... BalanceManager object id
        manager_key: &str,    // label you'll use inside the SDK, e.g. "MANAGER_1"
        env: Environment,     // Environment::Mainnet or ::Testnet
    ) -> Result<Self> {
        let sui = SuiClientBuilder::default().build(fullnode_url).await?;

        info!(
            indexer = indexer_base,
            "DeepBook indexer configured for venue adapter"
        );
        // Note: The DeepBook SDK (DeepBookClient) methods like get_level2_ticks_from_mid
        // and pool_book_params likely use the indexer under the hood. For direct indexer
        // API access, we would need to add HTTP client calls to the indexer_base URL.
        // Currently, we rely on the SDK's abstraction which should use the indexer when available.

        // Wire minimal config maps for the SDK. We provide the BalanceManager only;
        // coins/pools use SDK defaults for the chosen environment.
        let mut managers: HashMap<&'static str, BalanceManager> = HashMap::new();
        // Note: We need to use a static string for the key. In production, you'd want to
        // manage this differently, but for now we'll use a workaround.
        let manager_key_static = Box::leak(manager_key.to_string().into_boxed_str());
        managers.insert(
            manager_key_static,
            BalanceManager {
                address: manager_object.to_string(),
                trade_cap: None,
                deposit_cap: None,
                withdraw_cap: None,
            },
        );

        // DeepBookClient requires the package "address" parameter; we pass the sender.
        let db = DeepBookClient::new(
            sui.clone(),
            sender,
            env,
            Some(managers),
            None, // coins: use defaults
            None, // pools: use defaults
            None, // admin cap
        );

        Ok(Self {
            sui,
            db,
            sender,
            manager_key: manager_key.to_string(),
        })
    }

    /// Build a PTB for a DeepBook limit order using the SDK and return BCS TransactionData bytes.
    /// If gasless is true, this method should not be used - use build_limit_order_ptb_gasless instead.
    pub async fn build_limit_order_ptb_bcs(
        &self,
        req: &LimitReq,
        gasless: bool,
    ) -> Result<Vec<u8>> {
        if gasless {
            anyhow::bail!(
                "use build_limit_order_ptb_gasless to get programmable transaction for sponsorship"
            );
        }
        // 1) Quantize to pool constraints (tick, lot, min)
        let params = self.pool_params(&req.pool).await?;
        let q_px = quantize_price(req.price, params.tick_size)?;
        let q_sz = quantize_size(req.quantity, params.lot_size, params.min_size)?;

        // 2) Compose a programmable transaction with the SDK's DeepBook contract
        let mut ptb = ProgrammableTransactionBuilder::new();

        let client_order_id = req
            .client_order_id
            .parse::<u64>()
            .context("client_order_id must parse to u64")?;

        let place_params = PlaceLimitOrderParams {
            pool_key: req.pool.clone(),
            balance_manager_key: self.manager_key.clone(),
            client_order_id,
            price: q_px,
            quantity: q_sz,
            is_bid: req.is_bid,
            expiration: Some(req.expiration_ms.unwrap_or(MAX_TIMESTAMP)),
            order_type: Some(OrderType::NoRestriction),
            self_matching_option: Some(SelfMatchingOptions::SelfMatchingAllowed),
            pay_with_deep: Some(req.pay_with_deep),
        };

        self.db
            .deep_book
            .place_limit_order(&mut ptb, place_params)
            .await
            .context("build deepbook limit order PTB")?;

        // 3) Finalize, select gas, and return BCS TransactionData bytes.
        let programmable = ptb.finish();
        let input_objects: Vec<_> = programmable
            .input_objects()
            .context("collect input objects")?
            .into_iter()
            .map(|obj| InputObjectKind::object_id(&obj))
            .collect();

        let gas_price = self
            .sui
            .read_api()
            .get_reference_gas_price()
            .await
            .context("fetch reference gas price")?;

        let gas = self
            .sui
            .transaction_builder()
            .select_gas(self.sender, None, GAS_BUDGET, input_objects, gas_price)
            .await
            .context("select gas coin")?;

        let tx_data = TransactionData::new(
            TransactionKind::programmable(programmable),
            self.sender,
            gas,
            GAS_BUDGET,
            gas_price,
        );
        let tx_bcs = bcs::to_bytes(&tx_data)?;
        Ok(tx_bcs)
    }

    /// Build a gasless PTB for a DeepBook limit order (for sponsored transactions).
    /// Returns (programmable_transaction, sender_address)
    pub async fn build_limit_order_ptb_gasless(
        &self,
        req: &LimitReq,
    ) -> Result<(sui_sdk::types::transaction::TransactionKind, SuiAddress)> {
        // 1) Quantize to pool constraints (tick, lot, min)
        let params = self.pool_params(&req.pool).await?;
        let q_px = quantize_price(req.price, params.tick_size)?;
        let q_sz = quantize_size(req.quantity, params.lot_size, params.min_size)?;

        // 2) Compose a programmable transaction with the SDK's DeepBook contract
        let mut ptb = ProgrammableTransactionBuilder::new();

        let client_order_id = req
            .client_order_id
            .parse::<u64>()
            .context("client_order_id must parse to u64")?;

        let place_params = PlaceLimitOrderParams {
            pool_key: req.pool.clone(),
            balance_manager_key: self.manager_key.clone(),
            client_order_id,
            price: q_px,
            quantity: q_sz,
            is_bid: req.is_bid,
            expiration: Some(req.expiration_ms.unwrap_or(MAX_TIMESTAMP)),
            order_type: Some(OrderType::NoRestriction),
            self_matching_option: Some(SelfMatchingOptions::SelfMatchingAllowed),
            pay_with_deep: Some(req.pay_with_deep),
        };

        self.db
            .deep_book
            .place_limit_order(&mut ptb, place_params)
            .await
            .context("build deepbook limit order PTB")?;

        // 3) Finalize programmable transaction (without gas)
        let programmable = ptb.finish();
        let tx_kind = TransactionKind::programmable(programmable);

        Ok((tx_kind, self.sender))
    }

    /// Resolve a list of ObjectIDs into ObjectRefs using the node's read API.
    pub async fn object_refs_for_ids(
        &self,
        ids: &[sui_sdk::types::base_types::ObjectID],
    ) -> Result<Vec<ObjectRef>> {
        let mut refs = Vec::with_capacity(ids.len());
        for id in ids {
            let resp = self
                .sui
                .read_api()
                .get_object_with_options(
                    *id,
                    sui_sdk::rpc_types::SuiObjectDataOptions::full_content(),
                )
                .await
                .with_context(|| format!("fetch object {id}"))?;
            
            if let Some(obj) = resp.data {
                refs.push((obj.object_id, obj.version, obj.digest));
            } else {
                anyhow::bail!("object {id} not found or does not exist");
            }
        }
        Ok(refs)
    }

    /// Fetch pool parameters from the indexer or cache.
    pub async fn pool_params(&self, pool: &str) -> Result<PoolParams> {
        let params: PoolBookParams = self
            .db
            .pool_book_params(pool)
            .await
            .with_context(|| format!("fetch pool book params for {pool}"))?;

        Ok(PoolParams {
            tick_size: params.tick_size,
            lot_size: params.lot_size,
            min_size: params.min_size,
        })
    }

    /// Get mid price for a pool
    pub async fn mid_price(&self, pool: &str) -> Result<f64> {
        self.db
            .mid_price(pool)
            .await
            .with_context(|| format!("fetch mid price for {pool}"))
    }

    /// Get level 2 order book data (ticks from mid)
    pub async fn level2_ticks_from_mid(
        &self,
        pool: &str,
        ticks: u64,
    ) -> Result<sui_deepbookv3::client::Level2TicksFromMid> {
        self.db
            .get_level2_ticks_from_mid(pool, ticks)
            .await
            .with_context(|| format!("fetch level2 order book for {pool}"))
    }

    /// Get level 2 order book data (price range)
    pub async fn level2_range(
        &self,
        pool: &str,
        price_low: f64,
        price_high: f64,
        is_bid: bool,
    ) -> Result<sui_deepbookv3::client::Level2Range> {
        self.db
            .get_level2_range(pool, price_low, price_high, is_bid)
            .await
            .with_context(|| format!("fetch level2 range for {pool}"))
    }

    /// Get pool trade parameters (fees, stake requirements)
    pub async fn trade_params(&self, pool: &str) -> Result<sui_deepbookv3::client::PoolTradeParams> {
        self.db
            .pool_trade_params(pool)
            .await
            .with_context(|| format!("fetch trade params for {pool}"))
    }

    /// Get reference gas price from the network
    pub async fn reference_gas_price(&self) -> Result<u64> {
        self.sui
            .read_api()
            .get_reference_gas_price()
            .await
            .context("fetch reference gas price")
    }

    /// Build a cancel order command for a PTB
    /// Returns the Argument that can be added to a PTB
    pub async fn build_cancel_order_command(
        &self,
        ptb: &mut ProgrammableTransactionBuilder,
        pool: &str,
        order_id: u128,
    ) -> Result<sui_sdk::types::transaction::Argument> {
        self.db
            .deep_book
            .cancel_order(ptb, pool, &self.manager_key, order_id)
            .await
            .context("build cancel order command")
    }

    /// Get order ID from transaction digest by querying transaction effects
    /// This extracts the order ID from the transaction that placed the order
    pub async fn get_order_id_from_digest(
        &self,
        digest: &str,
        pool: &str,
    ) -> Result<Option<u128>> {
        use sui_sdk::types::digests::TransactionDigest;
        
        // Query transaction by digest
        let tx_digest = TransactionDigest::from_str(digest)
            .map_err(|e| anyhow::anyhow!("invalid transaction digest: {}", e))?;
        
        let tx = self
            .sui
            .read_api()
            .get_transaction_with_options(
                tx_digest,
                sui_sdk::rpc_types::SuiTransactionBlockResponseOptions::full_content(),
            )
            .await
            .context("query transaction by digest")?;

        // Extract order ID from events
        // DeepBook emits events when orders are placed - we need to find the OrderPlaced event
        // TODO: Implement proper event parsing based on actual Sui SDK event structure
        // The event structure may vary by SDK version. For now, this is a placeholder
        // that can be extended once the exact event field names are known.
        //
        // In production, you'd want to:
        // 1. Parse events from tx.events.data
        // 2. Find OrderPlaced event (check event_type or package_id/module)
        // 3. Extract order_id from event JSON/BCS data
        // 4. Return the order_id
        
        // For now, return None - the cancel-replace route will need order ID provided
        // directly or looked up via account_open_orders
        warn!(
            digest = digest,
            pool = pool,
            "order ID lookup from transaction digest not fully implemented - event parsing needs SDK-specific implementation"
        );
        Ok(None)
    }

    /// Get open order IDs for the account in a pool
    pub async fn get_open_order_ids(&self, pool: &str) -> Result<Vec<u128>> {
        self.db
            .account_open_orders(pool, &self.manager_key)
            .await
            .context("fetch account open orders")
    }

    /// Get access to the underlying SuiClient (for advanced queries)
    pub fn sui_client(&self) -> &SuiClient {
        &self.sui
    }
}
