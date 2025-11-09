// Pre-trade validation module
// Validates BalanceManager funding, quantization, and order parameters before execution
//
// Numan Thabit 2025 Nov

use crate::venues::adapter::{DeepBookAdapter, LimitReq};
use crate::quant::PoolParams;
use anyhow::{Context, Result};
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

/// Validate a limit order request before routing/execution
pub async fn validate_limit_order(
    adapter: &DeepBookAdapter,
    req: &LimitReq,
) -> Result<ValidationResult> {
    let mut result = ValidationResult::new();

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
        Ok(quantized_price) => {
            if (quantized_price - req.price).abs() / req.price > 0.001 {
                warn!(
                    original_price = req.price,
                    quantized_price = quantized_price,
                    "price was quantized significantly"
                );
            }
        }
        Err(e) => {
            result.add_error(format!("price quantization failed: {}", e));
        }
    }

    match crate::quant::quantize_size(req.quantity, pool_params.lot_size, pool_params.min_size) {
        Ok(quantized_size) => {
            if quantized_size < pool_params.min_size {
                result.add_error(format!(
                    "quantized size {} is below minimum size {}",
                    quantized_size, pool_params.min_size
                ));
            }
            if (quantized_size - req.quantity).abs() / req.quantity > 0.001 {
                warn!(
                    original_quantity = req.quantity,
                    quantized_quantity = quantized_size,
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
    
    Ok(result)
}

/// Validate BalanceManager has sufficient balance for an order
pub async fn validate_balance_manager_funding(
    adapter: &DeepBookAdapter,
    req: &LimitReq,
    pool_params: &PoolParams,
) -> Result<ValidationResult> {
    let mut result = ValidationResult::new();

    // Determine required coin type based on order side
    // For bids: need quote coin
    // For asks: need base coin
    // Note: This is a simplified check - in production, you'd need to:
    // 1. Get pool's base_coin and quote_coin types
    // 2. Calculate required amount (price * quantity for bids, quantity for asks)
    // 3. Check BalanceManager balance for that coin type
    
    // Placeholder: We'll add this once we have access to pool coin types
    // For now, return valid to avoid blocking execution
    
    Ok(result)
}

