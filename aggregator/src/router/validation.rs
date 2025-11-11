// Pre-trade validation module
// Validates BalanceManager funding, quantization, and order parameters before execution
//
// Numan Thabit 2025 Nov

use crate::venues::adapter::{DeepBookAdapter, LimitReq};
use anyhow::Result;
use tracing::warn;

/// Pre-trade validation result
#[derive(Debug)]
pub struct ValidationResult {
    pub is_valid: bool,
    pub errors: Vec<String>,
}

impl ValidationResult {
    pub fn new() -> Self {
        Self {
            is_valid: true,
            errors: Vec::new(),
        }
    }

    pub fn add_error(&mut self, error: String) {
        self.is_valid = false;
        self.errors.push(error);
    }

    pub fn into_result(self) -> Result<()> {
        if self.is_valid {
            Ok(())
        } else {
            anyhow::bail!("validation failed: {}", self.errors.join("; "))
        }
    }
}

impl Default for ValidationResult {
    fn default() -> Self {
        Self::new()
    }
}

/// Validate a limit order request before routing/execution
pub async fn validate_limit_order(
    adapter: &DeepBookAdapter,
    req: &LimitReq,
) -> Result<ValidationResult> {
    let mut result = ValidationResult::new();
    let mut quantized_price = None;
    let mut quantized_size = None;

    // 1. Validate pool parameters exist
    let pool_params = match adapter.pool_params(&req.pool).await {
        Ok(params) => params,
        Err(e) => {
            result.add_error(format!("failed to fetch pool parameters: {}", e));
            return Ok(result);
        }
    };

    // 2. Validate quantization (price and size meet tick/lot/min constraints)
    match crate::quant::quantize_price(req.price, pool_params.tick_size) {
        Ok(price) => {
            quantized_price = Some(price);
            if (price - req.price).abs() / req.price > 0.001 {
                warn!(
                    original_price = req.price,
                    quantized_price = price,
                    "price was quantized significantly"
                );
            }
        }
        Err(e) => {
            result.add_error(format!("price quantization failed: {}", e));
        }
    }

    match crate::quant::quantize_size(req.quantity, pool_params.lot_size, pool_params.min_size) {
        Ok(size) => {
            quantized_size = Some(size);
            if size < pool_params.min_size {
                result.add_error(format!(
                    "quantized size {} is below minimum size {}",
                    size, pool_params.min_size
                ));
            }
            if (size - req.quantity).abs() / req.quantity > 0.001 {
                warn!(
                    original_quantity = req.quantity,
                    quantized_quantity = size,
                    "quantity was quantized significantly"
                );
            }
        }
        Err(e) => {
            result.add_error(format!("size quantization failed: {}", e));
        }
    }

    // 3. Validate BalanceManager balance (if adapter supports it)
    // For bids: need quote coin balance
    // For asks: need base coin balance
    // Note: This requires knowing the pool's base/quote coins
    // For now, we'll add a placeholder that can be extended

    // TODO: Add actual balance check once we have pool coin types
    // For DeepBook, we can use the adapter's DeepBookClient to check balance
    if let (Some(q_price), Some(q_size)) = (quantized_price, quantized_size) {
        if let Some(err) = validate_balance_manager_funding(adapter, req, q_price, q_size).await? {
            result.add_error(err);
        }
    }

    Ok(result)
}

/// Validate BalanceManager has sufficient balance for an order
pub async fn validate_balance_manager_funding(
    adapter: &DeepBookAdapter,
    req: &LimitReq,
    quantized_price: f64,
    quantized_size: f64,
) -> Result<Option<String>> {
    let (net_base, net_quote, net_deep) = match adapter.balance_manager_balances(&req.pool).await {
        Ok(balances) => balances,
        Err(e) => {
            return Ok(Some(format!(
                "failed to fetch balance manager balances: {}",
                e
            )))
        }
    };

    if req.is_bid {
        let taker_fee_multiplier = match adapter.trade_params(&req.pool).await {
            Ok(params) => 1.0 + params.taker_fee,
            Err(e) => {
                return Ok(Some(format!(
                    "failed to fetch trade params for pool {}: {}",
                    req.pool, e
                )))
            }
        };
        let required_quote = quantized_price * quantized_size * taker_fee_multiplier;
        if net_quote + f64::EPSILON < required_quote {
            return Ok(Some(format!(
                "insufficient quote balance: requires {:.6}, available {:.6}",
                required_quote, net_quote
            )));
        }
        if req.pay_with_deep && net_deep <= 0.0 {
            return Ok(Some(
                "pay_with_deep set but BalanceManager has no DEEP balance".to_string(),
            ));
        }
    } else {
        let required_base = quantized_size;
        if net_base + f64::EPSILON < required_base {
            return Ok(Some(format!(
                "insufficient base balance: requires {:.6}, available {:.6}",
                required_base, net_base
            )));
        }
    }

    Ok(None)
}
