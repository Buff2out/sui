// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use sui_rpc::Client;
use tonic::body::Body;
use tower::util::BoxService;

use crate::metrics::MetricsLayer;

type BoxedChannel = BoxService<http::Request<Body>, http::Response<Body>, tonic::Status>;

pub struct SuiRpcClient {
    client: Client,
}

impl SuiRpcClient {
    /// Create a new client connected to the given URI with metrics instrumentation.
    pub fn new<T>(
        uri: T,
        metrics_prefix: Option<&str>,
        registry: &Registry,
    ) -> Result<Self, tonic::Status>
    where
        T: TryInto<http::Uri>,
        T::Error: Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
    {
        let client = Client::new(uri)?.request_layer(MetricsLayer::new(metrics_prefix, registry));

        Ok(Self { client })
    }

    /// Create a client with a custom max decoding message size.
    pub fn with_max_decoding_message_size(mut self, limit: usize) -> Self {
        self.client = self.client.with_max_decoding_message_size(limit);
        self
    }

    /// Get a `LedgerServiceClient` with metrics applied.
    pub fn ledger_client(
        &mut self,
    ) -> sui_rpc::proto::sui::rpc::v2::ledger_service_client::LedgerServiceClient<BoxedChannel>
    {
        self.client.ledger_client()
    }

    /// Get a `StateServiceClient` with metrics applied.
    pub fn state_client(
        &mut self,
    ) -> sui_rpc::proto::sui::rpc::v2::state_service_client::StateServiceClient<BoxedChannel> {
        self.client.state_client()
    }

    /// Get a `TransactionExecutionServiceClient` with metrics applied.
    pub fn execution_client(
        &mut self,
    ) -> sui_rpc::proto::sui::rpc::v2::transaction_execution_service_client::TransactionExecutionServiceClient<BoxedChannel>
    {
        self.client.execution_client()
    }

    /// Get a `SubscriptionServiceClient` with metrics applied.
    pub fn subscription_client(
        &mut self,
    ) -> sui_rpc::proto::sui::rpc::v2::subscription_service_client::SubscriptionServiceClient<
        BoxedChannel,
    > {
        self.client.subscription_client()
    }
}
