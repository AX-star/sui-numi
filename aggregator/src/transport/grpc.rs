// gRPC transport layer implementation
// This file implements the gRPC client/server communication protocol
// for interacting with Sui network nodes and other services
//
// Numan Thabit 2025 Nov

use std::time::Duration;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

#[cfg(feature = "grpc-exec")]
use crate::metrics::{REQ_ERRORS, REQ_LATENCY};
#[cfg(not(feature = "grpc-exec"))]
use tracing::warn;

// Generated modules from build.rs (package "sui.rpc.v2")
pub mod sui {
    #![allow(
        clippy::large_enum_variant,
        clippy::doc_overindented_list_items,
        clippy::upper_case_acronyms
    )]
    pub mod rpc {
        pub mod v2 {
            tonic::include_proto!("sui.rpc.v2");
        }
    }
}

pub mod google {
    #![allow(clippy::doc_overindented_list_items)]
    pub mod rpc {
        tonic::include_proto!("google.rpc");
    }
}

use sui::rpc::v2::{
    ledger_service_client::LedgerServiceClient, state_service_client::StateServiceClient,
    subscription_service_client::SubscriptionServiceClient,
};

use sui::rpc::v2::{SubscribeCheckpointsRequest, SubscribeCheckpointsResponse};

#[cfg(feature = "grpc-exec")]
use sui::rpc::v2::{
    transaction_execution_service_client::TransactionExecutionServiceClient, Bcs,
    ExecuteTransactionRequest, SimulateTransactionRequest, Transaction,
};

#[derive(Clone)]
pub struct GrpcClients {
    pub ledger: LedgerServiceClient<Channel>,
    pub state: StateServiceClient<Channel>,
    pub subs: SubscriptionServiceClient<Channel>,
    #[cfg(feature = "grpc-exec")]
    pub exec: TransactionExecutionServiceClient<Channel>,
}

pub async fn connect_tls(endpoint: &str) -> anyhow::Result<Channel> {
    let ep = Endpoint::from_shared(endpoint.to_string())?
        .timeout(Duration::from_secs(10))
        .connect_timeout(Duration::from_secs(5))
        .tcp_nodelay(true)
        .tls_config(ClientTlsConfig::new())?;
    Ok(ep.connect().await?)
}

impl GrpcClients {
    pub async fn new(endpoint: &str) -> anyhow::Result<Self> {
        let ch = connect_tls(endpoint).await?;
        Ok(Self {
            ledger: LedgerServiceClient::new(ch.clone()),
            state: StateServiceClient::new(ch.clone()),
            subs: SubscriptionServiceClient::new(ch.clone()),
            #[cfg(feature = "grpc-exec")]
            exec: TransactionExecutionServiceClient::new(ch),
        })
    }

    pub async fn readiness_probe(&mut self) -> anyhow::Result<()> {
        self.ledger
            .get_service_info(sui::rpc::v2::GetServiceInfoRequest::default())
            .await
            .map(|_| ())
            .map_err(|status| status.into())
    }

    /// Dry-run a PTB using gRPC v2 (requires the `grpc-exec` feature).
    #[cfg(feature = "grpc-exec")]
    pub async fn simulate_ptb(&mut self, tx_bcs: Vec<u8>) -> anyhow::Result<()> {
        let _timer = REQ_LATENCY
            .with_label_values(&["grpc", "SimulateTransaction"])
            .start_timer();
        let request = SimulateTransactionRequest {
            transaction: Some(Transaction {
                bcs: Some(Bcs {
                    // Type name is informational; value is required.
                    name: Some("sui.types.TransactionData".to_string()),
                    value: Some(tx_bcs),
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

        if let Err(status) = self
            .exec
            .simulate_transaction(tonic::Request::new(request))
            .await
        {
            REQ_ERRORS
                .with_label_values(&["grpc", "SimulateTransaction"])
                .inc();
            return Err(status.into());
        }

        Ok(())
    }

    /// Fallback implementation when gRPC execution client is not enabled.
    #[cfg(not(feature = "grpc-exec"))]
    pub async fn simulate_ptb(&mut self, tx_bcs: Vec<u8>) -> anyhow::Result<()> {
        warn!(
            bytes = tx_bcs.len(),
            "simulate_ptb requires the 'grpc-exec' feature; skipping gRPC dry-run"
        );
        Ok(())
    }

    /// Execute via gRPC v2 Transaction Execution Service (enable with `--features grpc-exec`).
    ///
    /// This method uses the Transaction Execution Service which, with Mysticeti v2,
    /// routes through the Transaction Driver for optimized submission. The Transaction Driver
    /// replaces the legacy Quorum Driver and provides:
    /// - Reduced round trips
    /// - Lower confirmation latency
    /// - Simplified submission path
    ///
    /// The Transaction Driver is transparent at the gRPC API level - nodes with Mysticeti v2
    /// automatically use it when processing ExecuteTransaction requests.
    #[cfg(feature = "grpc-exec")]
    pub async fn execute_ptb(
        &mut self,
        tx_bcs: Vec<u8>,
        signatures: Vec<sui::rpc::v2::UserSignature>,
    ) -> anyhow::Result<sui::rpc::v2::ExecutedTransaction> {
        let _timer = REQ_LATENCY
            .with_label_values(&["grpc", "ExecuteTransaction"])
            .start_timer();
        let request = ExecuteTransactionRequest {
            transaction: Some(Transaction {
                bcs: Some(Bcs {
                    name: Some("sui.types.TransactionData".into()),
                    value: Some(tx_bcs),
                }),
                ..Default::default()
            }),
            signatures,
            ..Default::default()
        };

        match self
            .exec
            .execute_transaction(tonic::Request::new(request))
            .await
        {
            Ok(resp) => Ok(resp.into_inner().transaction.unwrap_or_default()),
            Err(status) => {
                REQ_ERRORS
                    .with_label_values(&["grpc", "ExecuteTransaction"])
                    .inc();
                Err(status.into())
            }
        }
    }

    /// Subscribe to checkpoint stream via gRPC.
    /// Returns a tonic Streaming that yields in-order checkpoints with cursors.
    pub async fn subscribe_checkpoints(
        &mut self,
    ) -> anyhow::Result<tonic::Streaming<SubscribeCheckpointsResponse>> {
        let req = SubscribeCheckpointsRequest { read_mask: None };
        let resp = self
            .subs
            .subscribe_checkpoints(tonic::Request::new(req))
            .await?;
        Ok(resp.into_inner())
    }
}
