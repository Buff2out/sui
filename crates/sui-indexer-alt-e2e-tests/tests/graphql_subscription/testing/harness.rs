// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeSet;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::time::Duration;

use fastcrypto::encoding::Base58;
use fastcrypto::encoding::Encoding;
use futures::SinkExt;
use futures::StreamExt;
use prometheus::Registry;
use serde_json::Value;
use serde_json::json;
use sui_futures::service::Service;
use sui_indexer_alt_graphql::RpcArgs as GraphQlArgs;
use sui_indexer_alt_graphql::args::KvArgs as GraphQlKvArgs;
use sui_indexer_alt_graphql::args::SubscriptionArgs;
use sui_indexer_alt_graphql::config::RpcConfig as GraphQlConfig;
use sui_indexer_alt_graphql::start_rpc as start_graphql;
use sui_indexer_alt_reader::consistent_reader::ConsistentReaderArgs;
use sui_indexer_alt_reader::fullnode_client::FullnodeArgs;
use sui_indexer_alt_reader::system_package_task::SystemPackageTaskArgs;
use sui_pg_db::DbArgs;
use sui_pg_db::temp::get_available_port;
use sui_test_transaction_builder::TestTransactionBuilder;
use tokio::time::timeout;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::http::Request;

pub struct SubscriptionTestCluster {
    pub subscription_url: String,
    #[allow(unused)]
    service: Service,
}

impl SubscriptionTestCluster {
    pub async fn new(validator_cluster: &test_cluster::TestCluster) -> Self {
        Self::new_impl(validator_cluster, None).await
    }

    pub async fn new_with_db(
        validator_cluster: &test_cluster::TestCluster,
        database_url: url::Url,
    ) -> Self {
        Self::new_impl(validator_cluster, Some(database_url)).await
    }

    async fn new_impl(
        validator_cluster: &test_cluster::TestCluster,
        database_url: Option<url::Url>,
    ) -> Self {
        let graphql_port = get_available_port();
        let graphql_listen_address = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), graphql_port);
        let rpc_url = validator_cluster.rpc_url();

        let service = start_graphql(
            database_url,
            FullnodeArgs {
                fullnode_rpc_url: Some(rpc_url.parse().unwrap()),
            },
            DbArgs::default(),
            GraphQlKvArgs::default(),
            ConsistentReaderArgs::default(),
            GraphQlArgs {
                rpc_listen_address: graphql_listen_address,
                no_ide: true,
            },
            SystemPackageTaskArgs::default(),
            SubscriptionArgs {
                checkpoint_stream_url: Some(rpc_url.parse().unwrap()),
            },
            "0.0.0",
            GraphQlConfig::default(),
            vec![],
            &Registry::new(),
        )
        .await
        .expect("Failed to start GraphQL server");

        Self {
            subscription_url: format!("ws://{}/graphql", graphql_listen_address),
            service,
        }
    }

    pub async fn subscribe(&self, query: &str) -> SubscriptionStream {
        let request = Request::builder()
            .uri(&self.subscription_url)
            .header("Sec-WebSocket-Protocol", "graphql-transport-ws")
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-WebSocket-Version", "13")
            .header("Host", "localhost")
            .header(
                "Sec-WebSocket-Key",
                tokio_tungstenite::tungstenite::handshake::client::generate_key(),
            )
            .body(())
            .unwrap();

        let (ws, _) = connect_async(request)
            .await
            .expect("Failed to connect WebSocket");

        let (mut sink, mut stream) = ws.split();

        sink.send(Message::Text(
            json!({"type": "connection_init"}).to_string().into(),
        ))
        .await
        .expect("Failed to send connection_init");

        let ack = stream.next().await.expect("No ack").expect("WS error");
        let ack: Value = serde_json::from_str(ack.to_text().unwrap()).unwrap();
        assert_eq!(ack["type"], "connection_ack");

        sink.send(Message::Text(
            json!({
                "id": "1",
                "type": "subscribe",
                "payload": { "query": query }
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("Failed to send subscribe");

        SubscriptionStream { stream }
    }
}

pub struct SubscriptionStream {
    stream: futures::stream::SplitStream<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    >,
}

impl SubscriptionStream {
    pub async fn next_item(&mut self) -> Value {
        let msg = timeout(Duration::from_secs(30), self.stream.next())
            .await
            .expect("Timeout waiting for subscription item")
            .expect("Stream ended")
            .expect("WS error");

        let text = match msg {
            Message::Text(t) => t,
            other => panic!("Expected text message, got: {other:?}"),
        };

        let msg: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(msg["type"], "next", "Expected 'next' message, got: {msg}");
        msg["payload"].clone()
    }

    pub async fn collect_items(&mut self, n: usize) -> Vec<Value> {
        let mut items = Vec::with_capacity(n);
        for _ in 0..n {
            items.push(self.next_item().await);
        }
        items
    }

    /// Wait for a subscription item where `find_digests` extracts digests from the
    /// response and any of them match the expected digests.
    pub async fn wait_for_matching_item(
        &mut self,
        digests: &[String],
        find_digests: impl Fn(&Value) -> Vec<&str>,
    ) -> Value {
        timeout(Duration::from_secs(60), async {
            loop {
                let item = self.next_item().await;
                let found = find_digests(&item);
                if found
                    .iter()
                    .any(|d| digests.iter().any(|expected| expected == d))
                {
                    return item;
                }
            }
        })
        .await
        .unwrap()
    }
}

/// Execute SUI transfers as a soft bundle and return Base58-encoded digests.
pub async fn transfer_coins(
    cluster: &mut test_cluster::TestCluster,
    amounts: &[u64],
) -> Vec<String> {
    let sender = cluster.wallet.active_address().unwrap();
    let recipient = sui_types::base_types::SuiAddress::ZERO;
    let mut excluded = BTreeSet::new();
    let mut txns = Vec::with_capacity(amounts.len());

    for &amount in amounts {
        let gas = cluster
            .wallet
            .gas_for_owner_budget(sender, 5000, excluded.clone())
            .await
            .unwrap()
            .1
            .compute_object_reference();
        excluded.insert(gas.0);
        txns.push(
            TestTransactionBuilder::new(sender, gas, 1000)
                .transfer_sui(Some(amount), recipient)
                .build(),
        );
    }

    cluster
        .sign_and_execute_txns_in_soft_bundle(&txns)
        .await
        .unwrap()
        .into_iter()
        .map(|(digest, _)| Base58::encode(digest))
        .collect()
}

/// Extract digests from a checkpoint subscription response.
/// Path: data.checkpoints.transactions.nodes[].digest
pub fn checkpoint_tx_digests(item: &Value) -> Vec<&str> {
    item["data"]["checkpoints"]["transactions"]["nodes"]
        .as_array()
        .map(|nodes| nodes.iter().filter_map(|n| n["digest"].as_str()).collect())
        .unwrap_or_default()
}

/// Extract digest from a top-level transaction subscription response.
/// Path: data.transactions.digest
pub fn transaction_digest(item: &Value) -> Vec<&str> {
    item["data"]["transactions"]["digest"]
        .as_str()
        .into_iter()
        .collect()
}

/// Poll the kv_packages watermark until it reaches `target_checkpoint`.
pub async fn wait_for_kv_packages(db: &sui_pg_db::temp::TempDb, target_checkpoint: u64) {
    use diesel::QueryableByName;
    use diesel::sql_types::BigInt;

    let reader = sui_indexer_alt_reader::pg_reader::PgReader::new(
        Some("wait_for_kv_packages"),
        Some(db.database().url().clone()),
        DbArgs::default(),
        &Registry::new(),
    )
    .await
    .expect("Failed to create PgReader");

    timeout(Duration::from_secs(30), async {
        loop {
            if let Ok(mut conn) = reader.connect().await {
                #[derive(QueryableByName)]
                struct W {
                    #[diesel(sql_type = BigInt)]
                    checkpoint_hi_inclusive: i64,
                }
                if let Ok(rows) = conn
                    .results::<_, _, W>(sui_sql_macro::query!(
                        "SELECT checkpoint_hi_inclusive FROM watermarks \
                         WHERE pipeline = 'kv_packages'"
                    ))
                    .await
                    && rows
                        .first()
                        .is_some_and(|r| r.checkpoint_hi_inclusive as u64 >= target_checkpoint)
                {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("Timed out waiting for kv_packages indexer");
}
