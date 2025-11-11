// Metrics and observability module
// This file handles collection and reporting of performance metrics,
// statistics, and monitoring data for the aggregator
//
// Numan Thabit 2025 Nov

use once_cell::sync::Lazy;
use prometheus::{register_counter_vec, register_histogram_vec, CounterVec, HistogramVec};

pub static REQ_LATENCY: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "aggr_request_latency_seconds",
        "latency for upstream calls",
        &["service", "method"]
    )
    .unwrap()
});

pub static REQ_ERRORS: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "aggr_request_errors_total",
        "errors by upstream",
        &["service", "method"]
    )
    .unwrap()
});

pub static DEEPBOOK_CACHE_HITS: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "aggr_deepbook_cache_hits_total",
        "DeepBook metadata cache hits",
        &["cache"]
    )
    .unwrap()
});

pub static DEEPBOOK_CACHE_MISSES: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "aggr_deepbook_cache_misses_total",
        "DeepBook metadata cache misses",
        &["cache"]
    )
    .unwrap()
});

pub static DEEPBOOK_INDEXER_REQUESTS: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "aggr_deepbook_indexer_requests_total",
        "DeepBook indexer request outcomes",
        &["operation", "status"]
    )
    .unwrap()
});

pub static DEEPBOOK_RECONCILIATION_MISMATCHES: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "aggr_deepbook_reconciliation_mismatches_total",
        "DeepBook reconciliation discrepancies",
        &["pool", "kind"]
    )
    .unwrap()
});

pub static DEEPBOOK_EVENT_COUNTER: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "aggr_deepbook_events_total",
        "DeepBook events observed within executions",
        &["event"]
    )
    .unwrap()
});
