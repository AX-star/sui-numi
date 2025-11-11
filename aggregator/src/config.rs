// Configuration management module
// This file handles loading and parsing of configuration settings
// from environment variables, config files, and command-line arguments
//
// Numan Thabit 2025 Nov

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use std::collections::HashSet;
use std::str::FromStr;
use std::time::Duration;
use sui_deepbookv3::utils::config::Environment;
use sui_sdk::types::base_types::SuiAddress;
use url::Url;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    /// gRPC fullnode endpoint, e.g. https://fullnode.mainnet.sui.io:443
    pub grpc_endpoint: Url,
    /// JSON-RPC endpoint for execute fallback, e.g. https://fullnode.mainnet.sui.io:443
    pub jsonrpc_endpoint: Url,
    /// GraphQL RPC + General-Purpose Indexer endpoint (optional)
    pub graphql_endpoint: Option<Url>,
    /// DeepBook public indexer (optional; defaults to Mysten Labs public indexer)
    pub deepbook_indexer: Option<Url>,
    /// Sui address of the trading account
    pub address: String,
    /// Hex-encoded 32-byte Ed25519 private key (do not use in prod; replace with HSM)
    pub ed25519_secret_hex: String,
    /// Concurrency control
    pub max_inflight: usize,
    /// Feature switch: use gRPC ExecuteTransaction
    pub use_grpc_execute: Option<bool>,
    /// DeepBook environment selector (mainnet/testnet)
    pub deepbook_env: Option<String>,
    /// BalanceManager object id (0x...)
    pub deepbook_manager_object: Option<String>,
    /// Label used when registering the BalanceManager with the SDK (defaults MANAGER_1)
    pub deepbook_manager_label: Option<String>,
    /// Optional DeepBook configuration overrides and telemetry toggles
    #[serde(default)]
    pub deepbook_config: Option<DeepBookConfigSection>,
    /// Sponsored transaction configuration (optional)
    pub sponsorship: Option<SponsorshipConfig>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct DeepBookConfigSection {
    #[serde(default)]
    pub package_ids: Option<DeepBookPackageIdsSection>,
    #[serde(default)]
    pub coins: Vec<DeepBookCoinSection>,
    #[serde(default)]
    pub pools: Vec<DeepBookPoolSection>,
    #[serde(default)]
    pub balance_managers: Vec<DeepBookBalanceManagerSection>,
    #[serde(default)]
    pub monitored_pools: Vec<String>,
    pub reconcile_interval_secs: Option<u64>,
    pub indexer_timeout_ms: Option<u64>,
    pub retry_initial_backoff_ms: Option<u64>,
    pub retry_max_backoff_ms: Option<u64>,
    pub retry_multiplier: Option<f64>,
    pub retry_max_elapsed_secs: Option<u64>,
    pub fallback_use_fullnode: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DeepBookPackageIdsSection {
    pub deepbook_package_id: String,
    pub registry_id: String,
    pub deep_treasury_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DeepBookCoinSection {
    pub key: String,
    pub address: String,
    pub type_name: String,
    pub scalar: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DeepBookPoolSection {
    pub key: String,
    pub address: String,
    pub base_coin: String,
    pub quote_coin: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DeepBookBalanceManagerSection {
    pub key: String,
    pub address: String,
    pub trade_cap: Option<String>,
    pub deposit_cap: Option<String>,
    pub withdraw_cap: Option<String>,
}

impl DeepBookConfigSection {
    fn monitored_pool_keys(&self) -> Vec<String> {
        self.monitored_pools
            .iter()
            .map(|p| p.trim().to_string())
            .filter(|p| !p.is_empty())
            .collect()
    }

    fn reconcile_interval(&self) -> Result<Duration> {
        match self.reconcile_interval_secs {
            Some(0) => bail!("DeepBook reconcile interval must be greater than zero"),
            Some(secs) => Ok(Duration::from_secs(secs)),
            None => Ok(Duration::from_secs(300)),
        }
    }

    fn indexer_timeout(&self) -> Result<Duration> {
        match self.indexer_timeout_ms {
            Some(0) => bail!("DeepBook indexer timeout must be greater than zero"),
            Some(ms) => Ok(Duration::from_millis(ms)),
            None => Ok(Duration::from_secs(5)),
        }
    }

    fn retry_settings(&self) -> Result<DeepBookRetrySettings> {
        let mut settings = DeepBookRetrySettings::default();
        if let Some(initial_ms) = self.retry_initial_backoff_ms {
            if initial_ms == 0 {
                bail!("DeepBook retry initial backoff must be greater than zero");
            }
            settings.initial = Duration::from_millis(initial_ms);
        }
        if let Some(max_ms) = self.retry_max_backoff_ms {
            if max_ms == 0 {
                bail!("DeepBook retry max backoff must be greater than zero");
            }
            settings.max = Duration::from_millis(max_ms);
        }
        if let Some(max_elapsed) = self.retry_max_elapsed_secs {
            if max_elapsed == 0 {
                bail!("DeepBook retry max elapsed must be greater than zero");
            }
            settings.max_elapsed = Duration::from_secs(max_elapsed);
        }
        if let Some(multiplier) = self.retry_multiplier {
            if multiplier <= 1.0 {
                bail!("DeepBook retry multiplier must be greater than 1.0");
            }
            settings.multiplier = multiplier;
        }
        Ok(settings)
    }

    fn fallback_use_fullnode(&self) -> bool {
        self.fallback_use_fullnode.unwrap_or(true)
    }

    fn build_overrides(&self) -> Result<Option<DeepBookOverrideSettings>> {
        let mut overrides = DeepBookOverrideSettings {
            package_ids: self
                .package_ids
                .as_ref()
                .map(|ids| DeepBookPackageIdsOverride {
                    deepbook_package_id: ids.deepbook_package_id.clone(),
                    registry_id: ids.registry_id.clone(),
                    deep_treasury_id: ids.deep_treasury_id.clone(),
                }),
            coins: Vec::new(),
            pools: Vec::new(),
            balance_managers: Vec::new(),
        };

        let mut coin_keys = HashSet::new();
        for coin in &self.coins {
            if !coin_keys.insert(coin.key.clone()) {
                bail!("duplicate DeepBook coin override key: {}", coin.key);
            }
            overrides.coins.push(DeepBookCoinOverride {
                key: coin.key.clone(),
                address: coin.address.clone(),
                type_name: coin.type_name.clone(),
                scalar: coin.scalar,
            });
        }

        let mut pool_keys = HashSet::new();
        for pool in &self.pools {
            if !pool_keys.insert(pool.key.clone()) {
                bail!("duplicate DeepBook pool override key: {}", pool.key);
            }
            overrides.pools.push(DeepBookPoolOverride {
                key: pool.key.clone(),
                address: pool.address.clone(),
                base_coin: pool.base_coin.clone(),
                quote_coin: pool.quote_coin.clone(),
            });
        }

        let mut manager_keys = HashSet::new();
        for manager in &self.balance_managers {
            if !manager_keys.insert(manager.key.clone()) {
                bail!(
                    "duplicate DeepBook balance manager override key: {}",
                    manager.key
                );
            }
            overrides
                .balance_managers
                .push(DeepBookBalanceManagerOverride {
                    key: manager.key.clone(),
                    address: manager.address.clone(),
                    trade_cap: manager.trade_cap.clone(),
                    deposit_cap: manager.deposit_cap.clone(),
                    withdraw_cap: manager.withdraw_cap.clone(),
                });
        }

        if overrides.is_empty() {
            Ok(None)
        } else {
            Ok(Some(overrides))
        }
    }
}

#[derive(Debug, Clone)]
pub struct DeepBookRetrySettings {
    pub initial: Duration,
    pub max: Duration,
    pub max_elapsed: Duration,
    pub multiplier: f64,
}

impl Default for DeepBookRetrySettings {
    fn default() -> Self {
        Self {
            initial: Duration::from_millis(200),
            max: Duration::from_secs(5),
            max_elapsed: Duration::from_secs(30),
            multiplier: 2.0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DeepBookOverrideSettings {
    pub package_ids: Option<DeepBookPackageIdsOverride>,
    pub coins: Vec<DeepBookCoinOverride>,
    pub pools: Vec<DeepBookPoolOverride>,
    pub balance_managers: Vec<DeepBookBalanceManagerOverride>,
}

impl DeepBookOverrideSettings {
    fn is_empty(&self) -> bool {
        self.package_ids.is_none()
            && self.coins.is_empty()
            && self.pools.is_empty()
            && self.balance_managers.is_empty()
    }
}

#[derive(Debug, Clone)]
pub struct DeepBookPackageIdsOverride {
    pub deepbook_package_id: String,
    pub registry_id: String,
    pub deep_treasury_id: String,
}

#[derive(Debug, Clone)]
pub struct DeepBookCoinOverride {
    pub key: String,
    pub address: String,
    pub type_name: String,
    pub scalar: u64,
}

#[derive(Debug, Clone)]
pub struct DeepBookPoolOverride {
    pub key: String,
    pub address: String,
    pub base_coin: String,
    pub quote_coin: String,
}

#[derive(Debug, Clone)]
pub struct DeepBookBalanceManagerOverride {
    pub key: String,
    pub address: String,
    pub trade_cap: Option<String>,
    pub deposit_cap: Option<String>,
    pub withdraw_cap: Option<String>,
}

impl AppConfig {
    pub fn load() -> Result<Self> {
        let cfg = config::Config::builder()
            .add_source(config::Environment::default().separator("__"))
            .build()?;
        Ok(cfg.try_deserialize()?)
    }

    pub fn sui_address(&self) -> Result<SuiAddress> {
        SuiAddress::from_str(&self.address)
            .with_context(|| format!("invalid Sui address: {}", self.address))
    }

    pub fn deepbook_settings(&self) -> Result<Option<DeepBookSettings>> {
        let indexer = match &self.deepbook_indexer {
            Some(url) => url.clone(),
            None => return Ok(None),
        };

        let manager_object = self.deepbook_manager_object.clone().with_context(|| {
            "APP__DEEPBOOK_MANAGER_OBJECT is required when DeepBook indexer is set"
        })?;

        let manager_label = self
            .deepbook_manager_label
            .clone()
            .unwrap_or_else(|| "MANAGER_1".to_string());

        let env = self
            .deepbook_env
            .as_deref()
            .unwrap_or("mainnet")
            .to_ascii_lowercase();
        let environment = match env.as_str() {
            "mainnet" | "prod" => Environment::Mainnet,
            "testnet" => Environment::Testnet,
            other => bail!("unsupported deepbook environment: {other}"),
        };

        let (
            overrides,
            monitored_pools,
            reconcile_interval,
            indexer_timeout,
            retry,
            fallback_use_fullnode,
        ) = if let Some(section) = &self.deepbook_config {
            let overrides = section.build_overrides()?;
            let monitored_pools = section.monitored_pool_keys();
            let reconcile_interval = section.reconcile_interval()?;
            let indexer_timeout = section.indexer_timeout()?;
            let retry = section.retry_settings()?;
            let fallback_use_fullnode = section.fallback_use_fullnode();
            (
                overrides,
                monitored_pools,
                reconcile_interval,
                indexer_timeout,
                retry,
                fallback_use_fullnode,
            )
        } else {
            (
                None,
                Vec::new(),
                Duration::from_secs(300),
                Duration::from_secs(5),
                DeepBookRetrySettings::default(),
                true,
            )
        };

        Ok(Some(DeepBookSettings {
            indexer,
            environment,
            balance_manager_object: manager_object,
            balance_manager_label: manager_label,
            overrides,
            monitored_pools,
            reconcile_interval,
            indexer_timeout,
            retry,
            fallback_use_fullnode,
        }))
    }
}

#[derive(Debug, Clone)]
pub struct DeepBookSettings {
    pub indexer: Url,
    pub environment: Environment,
    pub balance_manager_object: String,
    pub balance_manager_label: String,
    pub overrides: Option<DeepBookOverrideSettings>,
    pub monitored_pools: Vec<String>,
    pub reconcile_interval: Duration,
    pub indexer_timeout: Duration,
    pub retry: DeepBookRetrySettings,
    pub fallback_use_fullnode: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SponsorshipConfig {
    /// Sponsor's Sui address
    pub sponsor_address: String,
    /// Hex-encoded 32-byte Ed25519 private key for sponsor (do not use in prod; replace with HSM)
    pub sponsor_key_hex: String,
    /// Per-user budget (in gas units)
    pub per_user_budget: Option<u64>,
    /// Per-transaction limit (in gas units)
    pub per_tx_limit: Option<u64>,
    /// Budget window duration in seconds (None = no reset)
    pub budget_window_seconds: Option<u64>,
    /// Max transactions per user per abuse detection window
    pub max_tx_per_window: Option<u64>,
    /// Max gas per user per abuse detection window
    pub max_gas_per_window: Option<u64>,
    /// Abuse detection window duration in seconds
    pub abuse_window_seconds: Option<u64>,
}

impl SponsorshipConfig {
    pub fn sponsor_address_parsed(&self) -> Result<SuiAddress> {
        SuiAddress::from_str(&self.sponsor_address)
            .with_context(|| format!("invalid sponsor address: {}", self.sponsor_address))
    }
}
