use anyhow::{anyhow, Context, Result};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;
use ultra_aggr::config::AppConfig;
use ultra_aggr::control::{AdmissionControl, CircuitBreakers};
use ultra_aggr::router::{ExecutionEngine, RouteSelector, Router, ValidatorSelector};
use ultra_aggr::state::{start_checkpoint_streaming, CheckpointState};
use ultra_aggr::transport::graphql::GraphQLRpc;
use ultra_aggr::transport::grpc::GrpcClients;
use ultra_aggr::transport::jsonrpc::JsonRpc;
use ultra_aggr::venues::adapter::DeepBookAdapter;

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing().context("initialize tracing subscriber")?;

    if let Err(err) = run().await {
        tracing::error!(error = ?err, "fatal aggregator error");
        std::process::exit(1);
    }
    Ok(())
}

async fn run() -> Result<()> {
    let config = AppConfig::load().context("load configuration from environment")?;
    let sui_address = config.sui_address().context("parse Sui address")?;

    let grpc = GrpcClients::new(config.grpc_endpoint.as_str())
        .await
        .with_context(|| format!("connect gRPC endpoint {}", config.grpc_endpoint))?;

    let jsonrpc = JsonRpc::new(config.jsonrpc_endpoint.to_string());

    let graphql = if let Some(endpoint) = &config.graphql_endpoint {
        Some(GraphQLRpc::new(endpoint.clone()).context("initialize GraphQL RPC client")?)
    } else {
        warn!("GraphQL endpoint not provided; GraphQL RPC disabled");
        None
    };

    let deepbook = if let Some(settings) = config.deepbook_settings()? {
        Some(
            DeepBookAdapter::new(config.jsonrpc_endpoint.as_str(), sui_address, &settings)
                .await
                .context("initialize DeepBook adapter")?,
        )
    } else {
        warn!("DeepBook settings not provided; venue adapter disabled");
        None
    };

    // Initialize router components
    let validator_selector = Arc::new(ValidatorSelector::default());

    // Register gRPC endpoint as a validator
    validator_selector
        .register(config.grpc_endpoint.to_string())
        .await;

    // Initialize route selector with latency estimates
    // Base latency for fast-path (owned objects): ~100ms
    // Shared-object latency (consensus): ~400ms (Mysticeti v2 target)
    let deepbook_arc = deepbook.clone().map(Arc::new);
    let route_selector = RouteSelector::new(
        deepbook_arc.as_ref().map(Arc::clone),
        100, // base_latency_ms
        400, // shared_object_latency_ms
    );

    // Initialize execution engine
    let mut execution_engine = ExecutionEngine::new(
        deepbook_arc.as_ref().map(Arc::clone),
        grpc.clone(),
        jsonrpc.clone(),
        validator_selector.clone(),
        config.ed25519_secret_hex.clone(),
        sui_address,
        config.use_grpc_execute.unwrap_or(false),
    );

    // Set up sponsorship if configured
    if let Some(sponsorship_config) = &config.sponsorship {
        use ultra_aggr::sponsorship::{AbuseConfig, SponsorshipManager};
        let sponsor_address = sponsorship_config
            .sponsor_address_parsed()
            .context("parse sponsor address")?;

        let abuse_config = AbuseConfig {
            max_tx_per_window: sponsorship_config.max_tx_per_window.unwrap_or(1000),
            max_gas_per_window: sponsorship_config
                .max_gas_per_window
                .unwrap_or(1_000_000_000),
            window_duration: Duration::from_secs(
                sponsorship_config.abuse_window_seconds.unwrap_or(3600),
            ),
        };

        // Get reference gas price for sponsorship manager
        let gas_price = if let Some(adapter) = &deepbook_arc {
            adapter.reference_gas_price().await.unwrap_or(1000)
        } else {
            1000 // Default fallback
        };

        let sponsorship_manager = Arc::new(
            SponsorshipManager::new(
                sponsorship_config.sponsor_key_hex.clone(),
                sponsor_address,
                gas_price,
                abuse_config,
            )
            .context("initialize sponsorship manager")?,
        );

        // Set per-user budget if configured
        if let Some(per_user_budget) = sponsorship_config.per_user_budget {
            let window = sponsorship_config
                .budget_window_seconds
                .map(Duration::from_secs);
            sponsorship_manager
                .set_user_budget(
                    sui_address,
                    per_user_budget,
                    sponsorship_config.per_tx_limit.unwrap_or(10_000_000),
                    window,
                )
                .await;
        }

        execution_engine = execution_engine.with_sponsorship(sponsorship_manager);
        info!("sponsorship manager initialized");
    }

    let execution_engine = Arc::new(execution_engine);

    // Initialize control plane
    let admission = Arc::new(AdmissionControl::new(config.max_inflight, None));
    let breakers = Arc::new(CircuitBreakers::new());

    // Create Router instance for order execution
    let route_selector_arc = Arc::new(route_selector);
    let router = Arc::new(
        Router::new(route_selector_arc.clone(), execution_engine.clone())
            .with_control(admission.clone(), breakers.clone()),
    );

    let app = App {
        config: Arc::new(config),
        grpc,
        jsonrpc,
        graphql,
        deepbook,
        router,
        route_selector: route_selector_arc,
        execution_engine,
        validator_selector,
        checkpoint_state: None,
        admission: None,
        breakers: None,
        reconcile_handle: None,
    };

    app.run().await
}

struct App {
    config: Arc<AppConfig>,
    grpc: GrpcClients,
    jsonrpc: JsonRpc,
    graphql: Option<GraphQLRpc>,
    deepbook: Option<DeepBookAdapter>,
    router: Arc<Router>,
    /// Route selector stored separately for direct access (e.g., updating latency estimates)
    /// Can also be accessed via router.selector()
    route_selector: Arc<RouteSelector>,
    /// Execution engine stored separately for direct access (e.g., setting sponsorship)
    /// Can also be accessed via router.executor()
    execution_engine: Arc<ExecutionEngine>,
    validator_selector: Arc<ValidatorSelector>,
    checkpoint_state: Option<CheckpointState>,
    #[allow(dead_code)]
    admission: Option<AdmissionControl>,
    #[allow(dead_code)]
    breakers: Option<CircuitBreakers>,
    reconcile_handle: Option<tokio::task::JoinHandle<()>>,
}

impl App {
    async fn run(mut self) -> Result<()> {
        self.grpc
            .readiness_probe()
            .await
            .context("gRPC readiness probe failed")?;

        info!(
            address = %self.config.address,
            grpc = %self.config.grpc_endpoint,
            jsonrpc = %self.jsonrpc.endpoint(),
            graphql = ?self.config.graphql_endpoint,
            "Numi Sui Stack aggregator online"
        );

        if let Some(_graphql_client) = &self.graphql {
            info!("GraphQL RPC + Indexer client initialized");
        }

        if let Some(adapter) = &self.deepbook {
            info!(
                manager = %self
                    .config
                    .deepbook_manager_label
                    .as_deref()
                    .unwrap_or("MANAGER_1"),
                "DeepBook adapter initialized"
            );
            if let Err(err) = adapter.pool_params("SUI_USDC").await {
                warn!(error = %err, "DeepBook pool metadata lookup failed; continuing");
            }

            if adapter.has_indexer() && !adapter.monitored_pools().is_empty() {
                let interval = adapter.reconciliation_interval();
                info!(
                    pools = adapter.monitored_pools().len(),
                    interval_secs = interval.as_secs(),
                    "starting DeepBook open-order reconciliation"
                );
                let adapter_clone = adapter.clone();
                let handle = tokio::spawn(async move {
                    let mut ticker = tokio::time::interval(interval);
                    loop {
                        ticker.tick().await;
                        match adapter_clone.reconcile_open_orders().await {
                            Ok(discrepancies) => {
                                if discrepancies.is_empty() {
                                    debug!("DeepBook reconciliation run complete");
                                } else {
                                    for diff in discrepancies {
                                        error!(
                                            pool = diff.pool,
                                            missing_on_indexer = ?diff.missing_on_indexer,
                                            missing_on_chain = ?diff.missing_on_chain,
                                            "DeepBook reconciliation discrepancy"
                                        );
                                    }
                                }
                            }
                            Err(err) => {
                                warn!(
                                    error = %err,
                                    "DeepBook reconciliation iteration failed"
                                );
                            }
                        }
                    }
                });
                self.reconcile_handle = Some(handle);
            } else {
                debug!(
                    has_indexer = adapter.has_indexer(),
                    pools = adapter.monitored_pools().len(),
                    "DeepBook reconciliation disabled"
                );
            }
        }

        // Log validator selector stats
        let validator_stats = self.validator_selector.stats().await;
        info!(
            validators = validator_stats.len(),
            "validator selector initialized"
        );
        for (endpoint, (ewma_ms, observations, healthy)) in validator_stats {
            info!(
                endpoint = %endpoint,
                ewma_ms = ewma_ms,
                observations = observations,
                healthy = healthy,
                "validator stats"
            );
        }

        // Control plane is now initialized in main() and passed to Router

        // Start checkpoint streaming and reconciliation
        let checkpoint_state = CheckpointState::new(1024);
        let grpc_clone = self.grpc.clone();
        let _stream_handle =
            start_checkpoint_streaming(grpc_clone, checkpoint_state.clone()).await?;
        self.checkpoint_state = Some(checkpoint_state.clone());
        info!("started checkpoint streaming");

        // Start HTTP API server
        let router_clone = self.router.clone();
        let api_router = ultra_aggr::router::router::create_api_router(router_clone);
        // Default API server address (can be configured via env var in future)
        let api_addr: std::net::SocketAddr =
            "0.0.0.0:8080".parse().expect("valid default API address");

        info!(address = %api_addr, "HTTP API server starting");
        let _api_handle = tokio::spawn(async move {
            let listener = tokio::net::TcpListener::bind(&api_addr)
                .await
                .expect("bind API server address");
            if let Err(e) = axum::serve(listener, api_router).await {
                warn!(error = %e, "API server error");
            }
        });

        let mut ticker = tokio::time::interval(Duration::from_secs(30));
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    info!(
                        max_inflight = self.config.max_inflight,
                        grpc_execute = self.config.use_grpc_execute.unwrap_or(false),
                        "Numi heartbeat"
                    );

                    // Log validator stats periodically
                    let stats = self.validator_selector.stats().await;
                    for (endpoint, (ewma_ms, observations, healthy)) in stats {
                        debug!(
                            endpoint = %endpoint,
                            ewma_ms = ewma_ms,
                            observations = observations,
                            healthy = healthy,
                            "validator telemetry"
                        );
                    }

                    // Report last checkpoint cursor if available
                    if let Some(cs) = &self.checkpoint_state {
                        if let Some(cursor) = cs.last_cursor().await {
                            info!(last_checkpoint = cursor, "checkpoint reconciliation state");
                        }
                    }

                    // Report execution and latency statistics
                    let exec_stats = self.execution_engine.get_stats();
                    let latency_stats = self.route_selector.get_latency_stats().await;

                    info!(
                        total_executions = exec_stats.total_executions,
                        successful = exec_stats.successful_executions,
                        failed = exec_stats.failed_executions,
                        success_rate = exec_stats.success_rate,
                        avg_effects_ms = ?exec_stats.avg_effects_time_ms,
                        base_latency_ms = latency_stats.base_latency_ms,
                        shared_latency_ms = latency_stats.shared_latency_ms,
                        owned_samples = latency_stats.owned_samples,
                        shared_samples = latency_stats.shared_samples,
                        "execution and latency statistics"
                    );

                    // Latency estimates are automatically updated via record_latency()
                    // after each execution, so no manual update needed here
                }
                res = tokio::signal::ctrl_c() => {
                    if let Err(err) = res {
                        warn!(error = %err, "ctrl_c listener error");
                    }
                    info!("Shutdown signal received, exiting");
                    break;
                }
            }
        }
        if let Some(handle) = self.reconcile_handle.take() {
            handle.abort();
        }
        Ok(())
    }
}

fn init_tracing() -> Result<()> {
    let env_filter =
        std::env::var("RUST_LOG").unwrap_or_else(|_| "info,hyper=warn,tonic=warn".to_string());
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(env_filter))
        .with_target(false)
        .try_init()
        .map_err(|err| anyhow!("tracing subscriber init: {err}"))
}
