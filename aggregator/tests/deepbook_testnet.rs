use std::str::FromStr;
use std::time::Duration;

use anyhow::Result;
use sui_deepbookv3::utils::config::Environment;
use sui_sdk::types::base_types::SuiAddress;
use ultra_aggr::config::{DeepBookRetrySettings, DeepBookSettings};
use ultra_aggr::venues::adapter::DeepBookAdapter;
use url::Url;

fn required_env(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

#[tokio::test]
#[ignore]
async fn deepbook_testnet_adapter_smoke() -> Result<()> {
    let fullnode = match required_env("TEST_SUI_FULLNODE_URL") {
        Some(url) => url,
        None => return Ok(()),
    };

    let indexer = match required_env("TEST_DEEPBOOK_INDEXER_URL") {
        Some(url) => url,
        None => return Ok(()),
    };

    let manager_object = match required_env("TEST_DEEPBOOK_MANAGER_OBJECT") {
        Some(id) => id,
        None => return Ok(()),
    };

    let sender_hex = match required_env("TEST_SUI_ADDRESS") {
        Some(addr) => addr,
        None => return Ok(()),
    };

    let pool_key = required_env("TEST_DEEPBOOK_POOL_KEY").unwrap_or_else(|| "SUI_USDC".to_string());
    let manager_label =
        required_env("TEST_DEEPBOOK_MANAGER_LABEL").unwrap_or_else(|| "MANAGER_1".to_string());

    let sender = SuiAddress::from_str(&sender_hex)?;
    let indexer_url = Url::parse(&indexer)?;

    let settings = DeepBookSettings {
        indexer: indexer_url,
        environment: Environment::Testnet,
        balance_manager_object: manager_object,
        balance_manager_label: manager_label,
        overrides: None,
        monitored_pools: vec![pool_key],
        reconcile_interval: Duration::from_secs(60),
        indexer_timeout: Duration::from_secs(10),
        retry: DeepBookRetrySettings::default(),
        fallback_use_fullnode: true,
    };

    let adapter = DeepBookAdapter::new(&fullnode, sender, &settings).await?;
    adapter.reference_gas_price().await?;

    Ok(())
}
