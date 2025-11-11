// Venue adapter module
// This file implements the adapter pattern for integrating different venue implementations
// into the aggregator's core logic
//
// Numan Thabit 2025 Nov

use crate::config::DeepBookSettings;
use crate::metrics::{
    DEEPBOOK_CACHE_HITS, DEEPBOOK_CACHE_MISSES, DEEPBOOK_INDEXER_REQUESTS,
    DEEPBOOK_RECONCILIATION_MISMATCHES,
};
use anyhow::{anyhow, bail, Context, Result};
use backoff::{future::retry, ExponentialBackoff};
use reqwest::StatusCode;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use sui_deepbookv3::client::{DeepBookClient, PoolBookParams, PoolDeepPrice};
use sui_deepbookv3::utils::config::DeepBookPackageOverride;
use sui_deepbookv3::utils::config::{GAS_BUDGET, MAX_TIMESTAMP};
use sui_deepbookv3::utils::types::{
    BalanceManager, Coin, OrderType, PlaceLimitOrderParams, Pool, SelfMatchingOptions,
};
use sui_sdk::rpc_types::SuiEvent;
use sui_sdk::types::base_types::ObjectRef;
use sui_sdk::types::base_types::SuiAddress;
use sui_sdk::types::programmable_transaction_builder::ProgrammableTransactionBuilder;
use sui_sdk::types::transaction::{InputObjectKind, TransactionData, TransactionKind};
use sui_sdk::{SuiClient, SuiClientBuilder};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use url::Url;

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

#[derive(Debug, Clone)]
pub struct TradeParams {
    pub taker_fee: f64,
    pub maker_fee: f64,
    pub stake_required: f64,
}

#[derive(Debug, Clone)]
pub struct BalanceSnapshot {
    pub net_base: f64,
    pub net_quote: f64,
    pub net_deep: f64,
    pub unclaimed_base: f64,
    pub unclaimed_quote: f64,
    pub unclaimed_deep: f64,
}

#[derive(Debug, Clone)]
pub struct DeepPrice {
    pub deep_per_base: Option<f64>,
    pub deep_per_quote: Option<f64>,
}

const POOL_PARAMS_TTL: Duration = Duration::from_secs(300);
const TRADE_PARAMS_TTL: Duration = Duration::from_secs(120);
const BALANCE_TTL: Duration = Duration::from_secs(3);
const DEEP_PRICE_TTL: Duration = Duration::from_secs(30);

#[derive(Clone)]
struct TimedCache<T> {
    ttl: Duration,
    label: &'static str,
    store: Arc<RwLock<HashMap<String, CacheEntry<T>>>>,
}

struct CacheEntry<T> {
    value: T,
    expires_at: Instant,
}

#[derive(Debug, Clone)]
struct RetryConfig {
    initial: Duration,
    max: Duration,
    max_elapsed: Duration,
    multiplier: f64,
}

impl RetryConfig {
    fn new(initial: Duration, max: Duration, max_elapsed: Duration, multiplier: f64) -> Self {
        Self {
            initial,
            max,
            max_elapsed,
            multiplier,
        }
    }

    fn to_backoff(&self) -> ExponentialBackoff {
        ExponentialBackoff {
            initial_interval: self.initial,
            max_interval: self.max,
            max_elapsed_time: Some(self.max_elapsed),
            multiplier: self.multiplier,
            ..ExponentialBackoff::default()
        }
    }
}

#[derive(Clone)]
struct DeepBookIndexer {
    base: Url,
    client: reqwest::Client,
}

impl DeepBookIndexer {
    fn new(base: Url, timeout: Duration) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .gzip(true)
            .brotli(true)
            .zstd(true)
            .build()
            .context("build HTTP client for DeepBook indexer")?;
        Ok(Self { base, client })
    }

    async fn fetch_pool_params(&self, pool: &str) -> Result<PoolParams> {
        let url = self
            .base
            .join("v1/pools/params")
            .context("construct pool params URL")?;
        let payload = self
            .get_json(url, &[("pool", pool)])
            .await
            .context("fetch pool params from indexer")?;
        parse_pool_params(payload)
    }

    async fn fetch_trade_params(&self, pool: &str) -> Result<TradeParams> {
        let url = self
            .base
            .join("v1/pools/trade-params")
            .context("construct trade params URL")?;
        let payload = self
            .get_json(url, &[("pool", pool)])
            .await
            .context("fetch trade params from indexer")?;
        parse_trade_params(payload)
    }

    async fn fetch_balance_snapshot(
        &self,
        pool: &str,
        manager_key: &str,
    ) -> Result<BalanceSnapshot> {
        let url = self
            .base
            .join("v1/accounts/balances")
            .context("construct balances URL")?;
        let payload = self
            .get_json(url, &[("pool", pool), ("managerKey", manager_key)])
            .await
            .context("fetch balance snapshot from indexer")?;
        parse_balance_snapshot(payload)
    }

    async fn fetch_deep_price(&self, pool: &str) -> Result<DeepPrice> {
        let url = self
            .base
            .join("v1/pools/deep-price")
            .context("construct deep price URL")?;
        let payload = self
            .get_json(url, &[("pool", pool)])
            .await
            .context("fetch deep price from indexer")?;
        parse_deep_price(payload)
    }

    async fn fetch_open_orders(&self, pool: &str, manager_key: &str) -> Result<Vec<u128>> {
        let url = self
            .base
            .join("v1/accounts/open-orders")
            .context("construct open orders URL")?;
        let response = self
            .client
            .get(url)
            .query(&[("pool", pool), ("managerKey", manager_key)])
            .send()
            .await
            .context("send deepbook open orders request")?;

        if response.status() == StatusCode::NOT_FOUND {
            bail!("DeepBook indexer open orders endpoint not available");
        }

        if !response.status().is_success() {
            bail!(
                "DeepBook open orders request failed with status {}",
                response.status()
            );
        }

        let value: Value = response
            .json()
            .await
            .context("parse DeepBook open orders response JSON")?;
        parse_order_ids(value)
    }

    async fn get_json(&self, url: Url, query: &[(&str, &str)]) -> Result<Value> {
        let response = self
            .client
            .get(url)
            .query(query)
            .send()
            .await
            .context("send DeepBook indexer request")?;

        if response.status() == StatusCode::NOT_FOUND {
            bail!("DeepBook indexer endpoint not available");
        }

        if !response.status().is_success() {
            bail!(
                "DeepBook indexer request failed with status {}",
                response.status()
            );
        }

        response
            .json::<Value>()
            .await
            .context("decode DeepBook indexer response")
    }
}

#[derive(Debug, Clone)]
pub struct OpenOrderDiscrepancy {
    pub pool: String,
    pub missing_on_indexer: Vec<u128>,
    pub missing_on_chain: Vec<u128>,
}

#[derive(Debug, Deserialize)]
struct PoolParamsDto {
    #[serde(alias = "tickSize")]
    tick_size: f64,
    #[serde(alias = "lotSize")]
    lot_size: f64,
    #[serde(alias = "minSize")]
    min_size: f64,
}

#[derive(Debug, Deserialize)]
struct TradeParamsDto {
    #[serde(alias = "takerFee")]
    taker_fee: f64,
    #[serde(alias = "makerFee")]
    maker_fee: f64,
    #[serde(alias = "stakeRequired")]
    stake_required: f64,
}

#[derive(Debug, Deserialize, Default)]
struct BalanceComponentDto {
    #[serde(default)]
    base: f64,
    #[serde(default)]
    quote: f64,
    #[serde(default)]
    deep: f64,
}

#[derive(Debug, Deserialize, Default)]
struct BalanceSnapshotDto {
    #[serde(alias = "settledBalances", default)]
    settled_balances: BalanceComponentDto,
    #[serde(alias = "owedBalances", default)]
    owed_balances: BalanceComponentDto,
    #[serde(alias = "unclaimedRebates", default)]
    unclaimed_rebates: BalanceComponentDto,
}

#[derive(Debug, Deserialize)]
struct DeepPriceDto {
    #[serde(alias = "deepPerBase")]
    deep_per_base: Option<f64>,
    #[serde(alias = "deepPerQuote")]
    deep_per_quote: Option<f64>,
}

fn extract_payload<T: DeserializeOwned>(value: Value) -> Result<T> {
    if let Some(obj) = value.get("data") {
        if !obj.is_null() {
            if let Ok(parsed) = serde_json::from_value(obj.clone()) {
                return Ok(parsed);
            }
        }
    }
    if let Some(obj) = value.get("result") {
        if !obj.is_null() {
            if let Ok(parsed) = serde_json::from_value(obj.clone()) {
                return Ok(parsed);
            }
        }
    }
    if let Some(obj) = value.get("payload") {
        if !obj.is_null() {
            if let Ok(parsed) = serde_json::from_value(obj.clone()) {
                return Ok(parsed);
            }
        }
    }
    serde_json::from_value(value.clone())
        .map_err(|err| anyhow!("failed to decode DeepBook indexer payload: {err}"))
}

fn parse_pool_params(value: Value) -> Result<PoolParams> {
    let dto: PoolParamsDto = extract_payload(value)?;
    Ok(PoolParams {
        tick_size: dto.tick_size,
        lot_size: dto.lot_size,
        min_size: dto.min_size,
    })
}

fn parse_trade_params(value: Value) -> Result<TradeParams> {
    let dto: TradeParamsDto = extract_payload(value)?;
    Ok(TradeParams {
        taker_fee: dto.taker_fee,
        maker_fee: dto.maker_fee,
        stake_required: dto.stake_required,
    })
}

fn parse_balance_snapshot(value: Value) -> Result<BalanceSnapshot> {
    let dto: BalanceSnapshotDto = extract_payload(value)?;
    let net_base = (dto.settled_balances.base - dto.owed_balances.base).max(0.0);
    let net_quote = (dto.settled_balances.quote - dto.owed_balances.quote).max(0.0);
    let net_deep = (dto.settled_balances.deep - dto.owed_balances.deep).max(0.0);
    Ok(BalanceSnapshot {
        net_base,
        net_quote,
        net_deep,
        unclaimed_base: dto.unclaimed_rebates.base,
        unclaimed_quote: dto.unclaimed_rebates.quote,
        unclaimed_deep: dto.unclaimed_rebates.deep,
    })
}

fn parse_deep_price(value: Value) -> Result<DeepPrice> {
    let dto: DeepPriceDto = extract_payload(value)?;
    Ok(DeepPrice {
        deep_per_base: dto.deep_per_base,
        deep_per_quote: dto.deep_per_quote,
    })
}

fn parse_order_ids(value: Value) -> Result<Vec<u128>> {
    let payload = extract_payload::<Value>(value)?;
    let candidates = [
        payload.get("orders"),
        payload.get("order_ids"),
        payload.get("orderIds"),
        payload.get("open_orders"),
        payload.get("openOrders"),
    ];

    for candidate in candidates.into_iter().flatten() {
        if candidate.is_array() {
            if let Ok(ids) = serde_json::from_value::<Vec<u128>>(candidate.clone()) {
                return Ok(ids);
            }
            if let Ok(strings) = serde_json::from_value::<Vec<String>>(candidate.clone()) {
                return strings_to_u128(strings);
            }
        }
    }

    if payload.is_array() {
        if let Ok(ids) = serde_json::from_value::<Vec<u128>>(payload.clone()) {
            return Ok(ids);
        }
        if let Ok(strings) = serde_json::from_value::<Vec<String>>(payload) {
            return strings_to_u128(strings);
        }
    }

    bail!("DeepBook indexer open orders payload missing order ids");
}

fn strings_to_u128(strings: Vec<String>) -> Result<Vec<u128>> {
    let mut out = Vec::with_capacity(strings.len());
    for s in strings {
        let trimmed = s.trim();
        let value = if let Some(hex) = trimmed.strip_prefix("0x") {
            u128::from_str_radix(hex, 16)
                .map_err(|err| anyhow!("failed to parse hex order id {trimmed}: {err}"))?
        } else {
            trimmed
                .parse::<u128>()
                .map_err(|err| anyhow!("failed to parse order id {trimmed} as integer: {err}"))?
        };
        out.push(value);
    }
    Ok(out)
}

fn leak_str(value: &str) -> &'static str {
    Box::leak(value.to_string().into_boxed_str())
}

impl<T: Clone> TimedCache<T> {
    fn new(ttl: Duration, label: &'static str) -> Self {
        Self {
            ttl,
            label,
            store: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn get_or_try_insert_with<E, Fut, F>(&self, key: &str, loader: F) -> Result<T, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        let now = Instant::now();
        let mut stale = false;
        {
            let guard = self.store.read().await;
            if let Some(entry) = guard.get(key) {
                if entry.expires_at > now {
                    DEEPBOOK_CACHE_HITS.with_label_values(&[self.label]).inc();
                    return Ok(entry.value.clone());
                } else {
                    stale = true;
                }
            }
        }

        if stale {
            let mut guard = self.store.write().await;
            if let Some(entry) = guard.get(key) {
                if entry.expires_at <= Instant::now() {
                    guard.remove(key);
                }
            }
        }

        DEEPBOOK_CACHE_MISSES.with_label_values(&[self.label]).inc();
        let value = loader().await?;
        let expires_at = Instant::now() + self.ttl;
        let mut guard = self.store.write().await;
        guard.insert(
            key.to_owned(),
            CacheEntry {
                value: value.clone(),
                expires_at,
            },
        );
        Ok(value)
    }

    async fn invalidate(&self, key: &str) {
        let mut guard = self.store.write().await;
        guard.remove(key);
    }

    async fn invalidate_all(&self) {
        let mut guard = self.store.write().await;
        guard.clear();
    }
}

#[derive(Clone)]
pub struct DeepBookAdapter {
    sui: SuiClient,
    pub(crate) db: DeepBookClient,
    sender: SuiAddress,
    pub(crate) manager_key: String, // key used inside DeepBookClient config, e.g. "MANAGER_1"
    pool_params_cache: TimedCache<PoolParams>,
    trade_params_cache: TimedCache<TradeParams>,
    balance_cache: TimedCache<BalanceSnapshot>,
    deep_price_cache: TimedCache<DeepPrice>,
    indexer: Option<DeepBookIndexer>,
    retry_config: RetryConfig,
    fallback_use_fullnode: bool,
    monitored_pools: Vec<String>,
    reconcile_interval: Duration,
}

impl DeepBookAdapter {
    pub async fn new(
        fullnode_url: &str,
        sender: SuiAddress,
        settings: &DeepBookSettings,
    ) -> Result<Self> {
        let sui = SuiClientBuilder::default().build(fullnode_url).await?;

        info!(
            indexer = %settings.indexer,
            "DeepBook indexer configured for venue adapter"
        );

        let mut managers: HashMap<&'static str, BalanceManager> = HashMap::new();
        let mut coins: HashMap<&'static str, Coin> = HashMap::new();
        let mut pools: HashMap<&'static str, Pool> = HashMap::new();

        if let Some(overrides) = &settings.overrides {
            for manager in &overrides.balance_managers {
                let key = leak_str(&manager.key);
                managers.insert(
                    key,
                    BalanceManager {
                        address: manager.address.clone(),
                        trade_cap: manager.trade_cap.clone(),
                        deposit_cap: manager.deposit_cap.clone(),
                        withdraw_cap: manager.withdraw_cap.clone(),
                    },
                );
            }

            for coin in &overrides.coins {
                let key = leak_str(&coin.key);
                coins.insert(
                    key,
                    Coin {
                        address: coin.address.clone(),
                        type_name: coin.type_name.clone(),
                        scalar: coin.scalar,
                    },
                );
            }

            for pool in &overrides.pools {
                let key = leak_str(&pool.key);
                pools.insert(
                    key,
                    Pool {
                        address: pool.address.clone(),
                        base_coin: pool.base_coin.clone(),
                        quote_coin: pool.quote_coin.clone(),
                    },
                );
            }
        }

        let manager_key_static = leak_str(&settings.balance_manager_label);
        managers
            .entry(manager_key_static)
            .or_insert_with(|| BalanceManager {
                address: settings.balance_manager_object.clone(),
                trade_cap: None,
                deposit_cap: None,
                withdraw_cap: None,
            });

        let coins_opt = if coins.is_empty() { None } else { Some(coins) };
        let pools_opt = if pools.is_empty() { None } else { Some(pools) };

        let package_override = settings
            .overrides
            .as_ref()
            .and_then(|o| o.package_ids.as_ref())
            .map(|ids| DeepBookPackageOverride {
                deepbook_package_id: ids.deepbook_package_id.clone(),
                registry_id: ids.registry_id.clone(),
                deep_treasury_id: ids.deep_treasury_id.clone(),
            });

        let db = DeepBookClient::new_with_overrides(
            sui.clone(),
            sender,
            settings.environment,
            Some(managers),
            coins_opt,
            pools_opt,
            None,
            package_override,
        );

        let indexer = match DeepBookIndexer::new(settings.indexer.clone(), settings.indexer_timeout)
        {
            Ok(idx) => Some(idx),
            Err(err) => {
                warn!(
                    error = %err,
                    "failed to initialize DeepBook indexer client; indexer fallback disabled"
                );
                None
            }
        };

        let retry_config = RetryConfig::new(
            settings.retry.initial,
            settings.retry.max,
            settings.retry.max_elapsed,
            settings.retry.multiplier,
        );

        Ok(Self {
            sui,
            db,
            sender,
            manager_key: settings.balance_manager_label.clone(),
            pool_params_cache: TimedCache::new(POOL_PARAMS_TTL, "pool_params"),
            trade_params_cache: TimedCache::new(TRADE_PARAMS_TTL, "trade_params"),
            balance_cache: TimedCache::new(BALANCE_TTL, "balances"),
            deep_price_cache: TimedCache::new(DEEP_PRICE_TTL, "deep_price"),
            indexer,
            retry_config,
            fallback_use_fullnode: settings.fallback_use_fullnode,
            monitored_pools: settings.monitored_pools.clone(),
            reconcile_interval: settings.reconcile_interval,
        })
    }

    fn new_backoff(&self) -> ExponentialBackoff {
        self.retry_config.to_backoff()
    }

    async fn retry_with_backoff<T, F, Fut>(
        &self,
        label: &'static str,
        mut operation: F,
    ) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let backoff = self.new_backoff();
        retry(backoff, || {
            let fut = operation();
            async move { fut.await.map_err(backoff::Error::transient) }
        })
        .await
        .map_err(|err| anyhow!("{label} failed after retries: {err}"))
    }

    async fn load_pool_params(&self, pool: &str) -> Result<PoolParams> {
        if let Some(indexer) = &self.indexer {
            let indexer = indexer.clone();
            let pool_key = pool.to_string();
            match self
                .retry_with_backoff("deepbook_indexer_pool_params", move || {
                    let indexer = indexer.clone();
                    let pool = pool_key.clone();
                    async move { indexer.fetch_pool_params(&pool).await }
                })
                .await
            {
                Ok(params) => {
                    DEEPBOOK_INDEXER_REQUESTS
                        .with_label_values(&["pool_params", "ok"])
                        .inc();
                    return Ok(params);
                }
                Err(err) => {
                    DEEPBOOK_INDEXER_REQUESTS
                        .with_label_values(&["pool_params", "error"])
                        .inc();
                    warn!(
                        pool = pool,
                        error = %err,
                        "DeepBook indexer pool params lookup failed; falling back to fullnode"
                    );
                }
            }
        }

        if !self.fallback_use_fullnode {
            bail!("DeepBook pool params unavailable and fallback disabled");
        }

        let db = self.db.clone();
        let pool_key = pool.to_string();
        self.retry_with_backoff("deepbook_fullnode_pool_params", move || {
            let db = db.clone();
            let pool = pool_key.clone();
            async move {
                let params: PoolBookParams = db
                    .pool_book_params(&pool)
                    .await
                    .with_context(|| format!("fetch pool book params for {pool}"))?;
                Ok(PoolParams {
                    tick_size: params.tick_size,
                    lot_size: params.lot_size,
                    min_size: params.min_size,
                })
            }
        })
        .await
    }

    async fn load_trade_params(&self, pool: &str) -> Result<TradeParams> {
        if let Some(indexer) = &self.indexer {
            let indexer = indexer.clone();
            let pool_key = pool.to_string();
            match self
                .retry_with_backoff("deepbook_indexer_trade_params", move || {
                    let indexer = indexer.clone();
                    let pool = pool_key.clone();
                    async move { indexer.fetch_trade_params(&pool).await }
                })
                .await
            {
                Ok(params) => {
                    DEEPBOOK_INDEXER_REQUESTS
                        .with_label_values(&["trade_params", "ok"])
                        .inc();
                    return Ok(params);
                }
                Err(err) => {
                    DEEPBOOK_INDEXER_REQUESTS
                        .with_label_values(&["trade_params", "error"])
                        .inc();
                    warn!(
                        pool = pool,
                        error = %err,
                        "DeepBook indexer trade params lookup failed; using fullnode fallback"
                    );
                }
            }
        }

        if !self.fallback_use_fullnode {
            bail!("DeepBook trade params unavailable and fallback disabled");
        }

        let db = self.db.clone();
        let pool_key = pool.to_string();
        self.retry_with_backoff("deepbook_fullnode_trade_params", move || {
            let db = db.clone();
            let pool = pool_key.clone();
            async move {
                let params = db
                    .pool_trade_params(&pool)
                    .await
                    .with_context(|| format!("fetch trade params for {pool}"))?;
                Ok(TradeParams {
                    taker_fee: params.taker_fee,
                    maker_fee: params.maker_fee,
                    stake_required: params.stake_required,
                })
            }
        })
        .await
    }

    async fn load_balance_snapshot(&self, pool: &str) -> Result<BalanceSnapshot> {
        if let Some(indexer) = &self.indexer {
            let indexer = indexer.clone();
            let pool_key = pool.to_string();
            let manager_key = self.manager_key.clone();
            match self
                .retry_with_backoff("deepbook_indexer_balances", move || {
                    let indexer = indexer.clone();
                    let pool = pool_key.clone();
                    let manager = manager_key.clone();
                    async move { indexer.fetch_balance_snapshot(&pool, &manager).await }
                })
                .await
            {
                Ok(snapshot) => {
                    DEEPBOOK_INDEXER_REQUESTS
                        .with_label_values(&["balances", "ok"])
                        .inc();
                    return Ok(snapshot);
                }
                Err(err) => {
                    DEEPBOOK_INDEXER_REQUESTS
                        .with_label_values(&["balances", "error"])
                        .inc();
                    warn!(
                        pool = pool,
                        error = %err,
                        "DeepBook indexer balance lookup failed; using fullnode fallback"
                    );
                }
            }
        }

        if !self.fallback_use_fullnode {
            bail!("DeepBook balance lookup unavailable and fallback disabled");
        }

        let db = self.db.clone();
        let pool_key = pool.to_string();
        let manager_key = self.manager_key.clone();
        self.retry_with_backoff("deepbook_fullnode_balances", move || {
            let db = db.clone();
            let pool = pool_key.clone();
            let manager = manager_key.clone();
            async move {
                let account = db
                    .account(&pool, &manager)
                    .await
                    .with_context(|| format!("fetch balance manager account for {pool}"))?;
                let net_base =
                    (account.settled_balances.base - account.owed_balances.base).max(0.0);
                let net_quote =
                    (account.settled_balances.quote - account.owed_balances.quote).max(0.0);
                let net_deep =
                    (account.settled_balances.deep - account.owed_balances.deep).max(0.0);
                Ok(BalanceSnapshot {
                    net_base,
                    net_quote,
                    net_deep,
                    unclaimed_base: account.unclaimed_rebates.base,
                    unclaimed_quote: account.unclaimed_rebates.quote,
                    unclaimed_deep: account.unclaimed_rebates.deep,
                })
            }
        })
        .await
    }

    async fn load_deep_price(&self, pool: &str) -> Result<DeepPrice> {
        if let Some(indexer) = &self.indexer {
            let indexer = indexer.clone();
            let pool_key = pool.to_string();
            match self
                .retry_with_backoff("deepbook_indexer_deep_price", move || {
                    let indexer = indexer.clone();
                    let pool = pool_key.clone();
                    async move { indexer.fetch_deep_price(&pool).await }
                })
                .await
            {
                Ok(price) => {
                    DEEPBOOK_INDEXER_REQUESTS
                        .with_label_values(&["deep_price", "ok"])
                        .inc();
                    return Ok(price);
                }
                Err(err) => {
                    DEEPBOOK_INDEXER_REQUESTS
                        .with_label_values(&["deep_price", "error"])
                        .inc();
                    warn!(
                        pool = pool,
                        error = %err,
                        "DeepBook indexer DEEP price lookup failed; using fullnode fallback"
                    );
                }
            }
        }

        if !self.fallback_use_fullnode {
            bail!("DeepBook DEEP price lookup unavailable and fallback disabled");
        }

        let db = self.db.clone();
        let pool_key = pool.to_string();
        self.retry_with_backoff("deepbook_fullnode_deep_price", move || {
            let db = db.clone();
            let pool = pool_key.clone();
            async move {
                let price: PoolDeepPrice = db
                    .get_pool_deep_price(&pool)
                    .await
                    .with_context(|| format!("fetch DEEP price conversion for {pool}"))?;
                Ok(DeepPrice {
                    deep_per_base: price.deep_per_base,
                    deep_per_quote: price.deep_per_quote,
                })
            }
        })
        .await
    }

    async fn load_open_orders_fullnode(&self, pool: &str) -> Result<Vec<u128>> {
        let db = self.db.clone();
        let pool_key = pool.to_string();
        let manager_key = self.manager_key.clone();
        self.retry_with_backoff("deepbook_fullnode_open_orders", move || {
            let db = db.clone();
            let pool = pool_key.clone();
            let manager = manager_key.clone();
            async move {
                db.account_open_orders(&pool, &manager)
                    .await
                    .context("fetch account open orders")
            }
        })
        .await
    }

    async fn fetch_indexer_open_orders(&self, pool: &str) -> Result<Vec<u128>> {
        let indexer = self
            .indexer
            .as_ref()
            .ok_or_else(|| anyhow!("DeepBook indexer not configured"))?
            .clone();
        let pool_key = pool.to_string();
        let manager_key = self.manager_key.clone();
        let result = self
            .retry_with_backoff("deepbook_indexer_open_orders", move || {
                let indexer = indexer.clone();
                let pool = pool_key.clone();
                let manager = manager_key.clone();
                async move { indexer.fetch_open_orders(&pool, &manager).await }
            })
            .await;

        match &result {
            Ok(_) => {
                DEEPBOOK_INDEXER_REQUESTS
                    .with_label_values(&["open_orders", "ok"])
                    .inc();
            }
            Err(_) => {
                DEEPBOOK_INDEXER_REQUESTS
                    .with_label_values(&["open_orders", "error"])
                    .inc();
            }
        }

        result
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
        self.pool_params_cache
            .get_or_try_insert_with(pool, || {
                let adapter = self.clone();
                let pool_key = pool.to_string();
                async move { adapter.load_pool_params(&pool_key).await }
            })
            .await
    }

    pub async fn balance_manager_balances(&self, pool: &str) -> Result<BalanceSnapshot> {
        self.balance_cache
            .get_or_try_insert_with(pool, || {
                let adapter = self.clone();
                let pool_key = pool.to_string();
                async move { adapter.load_balance_snapshot(&pool_key).await }
            })
            .await
    }

    pub async fn deep_price(&self, pool: &str) -> Result<DeepPrice> {
        self.deep_price_cache
            .get_or_try_insert_with(pool, || {
                let adapter = self.clone();
                let pool_key = pool.to_string();
                async move { adapter.load_deep_price(&pool_key).await }
            })
            .await
    }

    pub async fn invalidate_pool_metadata(&self, pool: &str) {
        self.pool_params_cache.invalidate(pool).await;
        self.trade_params_cache.invalidate(pool).await;
        self.deep_price_cache.invalidate(pool).await;
    }

    pub async fn invalidate_pool_balances(&self, pool: &str) {
        self.balance_cache.invalidate(pool).await;
    }

    pub async fn invalidate_all_caches(&self) {
        self.pool_params_cache.invalidate_all().await;
        self.trade_params_cache.invalidate_all().await;
        self.balance_cache.invalidate_all().await;
        self.deep_price_cache.invalidate_all().await;
    }

    fn is_order_placed_event(event: &SuiEvent) -> bool {
        let module = event.type_.module.as_str();
        let name = event.type_.name.as_str();
        (module == "clob_v2" && name == "OrderPlaced")
            || (module == "clob" && (name == "OrderPlacedV2" || name == "OrderPlaced"))
    }

    fn is_deepbook_module(module: &str) -> bool {
        matches!(module, "clob" | "clob_v2")
    }

    fn matches_owner(event: &SuiEvent, sender: &SuiAddress) -> bool {
        if let Some(owner) =
            Self::extract_string_field(&event.parsed_json, &["owner", "accountOwner"])
        {
            if let Ok(owner_addr) = SuiAddress::from_str(&owner) {
                return &owner_addr == sender;
            }
        }
        true
    }

    fn pool_key_matches(event_pool: &str, target_pool: &str) -> bool {
        event_pool.eq_ignore_ascii_case(target_pool)
    }

    fn extract_string_field(value: &Value, keys: &[&str]) -> Option<String> {
        for key in keys {
            if let Some(v) = value.get(*key) {
                match v {
                    Value::String(s) => return Some(s.clone()),
                    Value::Number(n) => {
                        if let Some(u) = n.as_u64() {
                            return Some(u.to_string());
                        }
                    }
                    Value::Bool(b) => return Some(b.to_string()),
                    Value::Object(obj) => {
                        if let Some(Value::String(id)) = obj.get("id") {
                            return Some(id.clone());
                        }
                        if let Some(Value::String(inner)) = obj.get("value") {
                            return Some(inner.clone());
                        }
                        if let Some(Value::Number(inner)) = obj.get("value") {
                            if let Some(u) = inner.as_u64() {
                                return Some(u.to_string());
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        None
    }

    fn extract_u64_field(value: &Value, keys: &[&str]) -> Option<u64> {
        for key in keys {
            if let Some(v) = value.get(*key) {
                match v {
                    Value::Number(n) => {
                        if let Some(u) = n.as_u64() {
                            return Some(u);
                        }
                    }
                    Value::String(s) => {
                        if let Ok(u) = s.parse::<u64>() {
                            return Some(u);
                        }
                    }
                    Value::Object(obj) => {
                        if let Some(Value::String(inner)) = obj.get("value") {
                            if let Ok(u) = inner.parse::<u64>() {
                                return Some(u);
                            }
                        } else if let Some(Value::Number(inner)) = obj.get("value") {
                            if let Some(u) = inner.as_u64() {
                                return Some(u);
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        None
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
    pub async fn trade_params(&self, pool: &str) -> Result<TradeParams> {
        self.trade_params_cache
            .get_or_try_insert_with(pool, || {
                let adapter = self.clone();
                let pool_key = pool.to_string();
                async move { adapter.load_trade_params(&pool_key).await }
            })
            .await
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

    /// Build a standalone PTB for canceling a DeepBook order.
    pub async fn build_cancel_order_ptb_bcs(&self, pool: &str, order_id: u128) -> Result<Vec<u8>> {
        let mut ptb = ProgrammableTransactionBuilder::new();

        self.db
            .deep_book
            .cancel_order(&mut ptb, pool, &self.manager_key, order_id)
            .await
            .with_context(|| format!("build cancel order command for {pool}"))?;

        let programmable = ptb.finish();
        let input_objects: Vec<_> = programmable
            .input_objects()
            .context("collect input objects for cancel PTB")?
            .into_iter()
            .map(|obj| InputObjectKind::object_id(&obj))
            .collect();

        let gas_price = self
            .sui
            .read_api()
            .get_reference_gas_price()
            .await
            .context("fetch reference gas price for cancel order")?;

        let gas = self
            .sui
            .transaction_builder()
            .select_gas(self.sender, None, GAS_BUDGET, input_objects, gas_price)
            .await
            .context("select gas coin for cancel order")?;

        let tx_data = TransactionData::new(
            TransactionKind::programmable(programmable),
            self.sender,
            gas,
            GAS_BUDGET,
            gas_price,
        );

        let tx_bcs = bcs::to_bytes(&tx_data).context("serialize cancel order transaction")?;
        Ok(tx_bcs)
    }

    /// Get order ID from transaction digest by querying transaction effects
    /// This extracts the order ID from the transaction that placed the order
    pub async fn deepbook_events_for_digest(&self, digest: &str) -> Result<Vec<SuiEvent>> {
        use sui_sdk::types::digests::TransactionDigest;

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

        let Some(events) = tx.events else {
            return Ok(Vec::new());
        };

        let filtered = events
            .data
            .into_iter()
            .filter(|event| {
                Self::is_deepbook_module(event.type_.module.as_str())
                    && Self::matches_owner(event, &self.sender)
            })
            .collect();

        Ok(filtered)
    }

    pub async fn get_order_id_from_digest(&self, digest: &str, pool: &str) -> Result<Option<u128>> {
        let events = self.deepbook_events_for_digest(digest).await?;
        if events.is_empty() {
            debug!(
                digest = digest,
                pool = pool,
                "transaction contained no DeepBook events when looking up order id"
            );
            return Ok(None);
        }

        let order_id = Self::extract_order_id_from_events(&events, pool, &self.sender, digest);

        if order_id.is_none() {
            warn!(
                            digest = digest,
                            pool = pool,
                            sender = %self.sender,
                "failed to locate DeepBook OrderPlaced event for transaction"
            );
        }

        Ok(order_id)
    }

    fn extract_order_id_from_events(
        events: &[SuiEvent],
        pool: &str,
        sender: &SuiAddress,
        digest: &str,
    ) -> Option<u128> {
        for event in events {
            if !Self::is_order_placed_event(event) {
                continue;
            }

            if !Self::matches_owner(event, sender) {
                continue;
            }

            if let Some(event_pool) =
                Self::extract_string_field(&event.parsed_json, &["poolKey", "pool", "pool_id"])
            {
                if !Self::pool_key_matches(&event_pool, pool) {
                    continue;
                }
            }

            if let Some(order_id) =
                Self::extract_u64_field(&event.parsed_json, &["orderId", "order_id"])
            {
                debug!(
                    digest = digest,
                    pool = pool,
                    order_id = order_id,
                    "derived DeepBook order id from cached events"
                );
                return Some(order_id as u128);
            }
        }
        None
    }

    pub(crate) fn order_id_from_events(
        &self,
        events: &[SuiEvent],
        pool: &str,
        digest: &str,
    ) -> Option<u128> {
        Self::extract_order_id_from_events(events, pool, &self.sender, digest)
    }

    /// Get open order IDs for the account in a pool
    pub async fn get_open_order_ids(&self, pool: &str) -> Result<Vec<u128>> {
        if self.indexer.is_some() {
            match self.fetch_indexer_open_orders(pool).await {
                Ok(ids) => return Ok(ids),
                Err(err) => {
                    warn!(
                        pool = pool,
                        error = %err,
                        "DeepBook indexer open orders lookup failed; using fullnode fallback"
                    );
                }
            }
        }

        if !self.fallback_use_fullnode {
            bail!("DeepBook open orders unavailable and fallback disabled");
        }

        self.load_open_orders_fullnode(pool).await
    }

    pub async fn reconcile_open_orders(&self) -> Result<Vec<OpenOrderDiscrepancy>> {
        if self.indexer.is_none() {
            debug!("DeepBook indexer not configured; skipping reconciliation");
            return Ok(Vec::new());
        }

        let mut discrepancies = Vec::new();
        for pool in &self.monitored_pools {
            let indexer_orders = match self.fetch_indexer_open_orders(pool).await {
                Ok(ids) => ids,
                Err(err) => {
                    warn!(
                        pool = pool,
                        error = %err,
                        "DeepBook reconciliation skipped due to indexer error"
                    );
                    continue;
                }
            };

            let on_chain_orders = match self.load_open_orders_fullnode(pool).await {
                Ok(ids) => ids,
                Err(err) => {
                    warn!(
                        pool = pool,
                        error = %err,
                        "DeepBook reconciliation skipped due to fullnode error"
                    );
                    continue;
                }
            };

            let indexer_set: HashSet<u128> = indexer_orders.iter().copied().collect();
            let on_chain_set: HashSet<u128> = on_chain_orders.iter().copied().collect();

            let missing_on_indexer = on_chain_set
                .difference(&indexer_set)
                .copied()
                .collect::<Vec<_>>();
            let missing_on_chain = indexer_set
                .difference(&on_chain_set)
                .copied()
                .collect::<Vec<_>>();

            if missing_on_indexer.is_empty() && missing_on_chain.is_empty() {
                debug!(
                    pool = pool,
                    orders = on_chain_orders.len(),
                    "DeepBook open orders reconciled"
                );
                continue;
            }

            warn!(
            pool = pool,
                missing_on_indexer = %missing_on_indexer.len(),
                missing_on_chain = %missing_on_chain.len(),
                "DeepBook open order mismatch detected"
            );

            if !missing_on_indexer.is_empty() {
                DEEPBOOK_RECONCILIATION_MISMATCHES
                    .with_label_values(&[pool, "missing_on_indexer"])
                    .inc_by(missing_on_indexer.len() as f64);
            }
            if !missing_on_chain.is_empty() {
                DEEPBOOK_RECONCILIATION_MISMATCHES
                    .with_label_values(&[pool, "missing_on_chain"])
                    .inc_by(missing_on_chain.len() as f64);
            }

            discrepancies.push(OpenOrderDiscrepancy {
                pool: pool.clone(),
                missing_on_indexer,
                missing_on_chain,
            });
        }

        Ok(discrepancies)
    }

    /// Get access to the underlying SuiClient (for advanced queries)
    pub fn sui_client(&self) -> &SuiClient {
        &self.sui
    }

    pub fn monitored_pools(&self) -> &[String] {
        &self.monitored_pools
    }

    pub fn reconciliation_interval(&self) -> Duration {
        self.reconcile_interval
    }

    pub fn has_indexer(&self) -> bool {
        self.indexer.is_some()
    }
}
