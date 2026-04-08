// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use prometheus::Registry;
use sui_rpc::Client;
use tonic::body::Body;
use tower::util::BoxService;

use crate::metrics::MetricsLayer;

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

    pub fn client(&self) -> Client {
        self.client.clone()
    }
}
