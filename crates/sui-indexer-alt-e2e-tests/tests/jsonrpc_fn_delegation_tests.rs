// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use anyhow::Context;
use reqwest::Client;
use serde_json::Value;
use serde_json::json;
use sui_indexer_alt_e2e_tests::OffchainCluster;
use sui_indexer_alt_e2e_tests::OffchainClusterConfig;
use sui_indexer_alt_e2e_tests::local_ingestion_client_args;
use sui_indexer_alt_jsonrpc::NodeArgs as JsonRpcNodeArgs;
use sui_macros::sim_test;
use sui_swarm_config::genesis_config::AccountConfig;
use sui_test_transaction_builder::make_staking_transaction;
use sui_types::base_types::SuiAddress;
use sui_types::transaction::TransactionDataAPI;
use test_cluster::TestCluster;
use test_cluster::TestClusterBuilder;
use url::Url;

struct FnDelegationTestCluster {
    onchain_cluster: TestCluster,
    offchain: OffchainCluster,
    client: Client,
    /// Checkpoint ingestion directory shared between TestCluster and OffchainCluster.
    #[allow(unused)]
    ingestion_dir: tempfile::TempDir,
}

impl FnDelegationTestCluster {
    /// Creates a new test cluster with a full indexing stack: Postgres (obj_versions),
    /// consistent store (owned objects), BigTable (object content), and a gRPC client
    /// to the fullnode (dry runs). Transaction execution is also enabled via the fullnode proxy.
    async fn new() -> anyhow::Result<Self> {
        let (client_args, ingestion_dir) = local_ingestion_client_args();

        let onchain_cluster = TestClusterBuilder::new()
            .with_num_validators(2)
            .with_epoch_duration_ms(300_000)
            .with_accounts(vec![
                AccountConfig {
                    address: None,
                    gas_amounts: vec![1_000_000_000_000; 5],
                };
                4
            ])
            .with_data_ingestion_dir(ingestion_dir.path().to_owned())
            .build()
            .await;

        let fullnode_rpc_url = Url::parse(onchain_cluster.rpc_url())?;

        let offchain = OffchainCluster::new(
            client_args,
            OffchainClusterConfig {
                jsonrpc_node_args: JsonRpcNodeArgs {
                    fullnode_rpc_url: Some(fullnode_rpc_url),
                },
                ..Default::default()
            },
            &prometheus::Registry::new(),
        )
        .await
        .context("Failed to create off-chain cluster")?;

        Ok(Self {
            onchain_cluster,
            offchain,
            client: Client::new(),
            ingestion_dir,
        })
    }

    /// Builds a simple transaction and returns the digest, tx bytes, and sigs to be used for testing.
    async fn transfer_transaction(&self) -> anyhow::Result<(String, String, Vec<String>)> {
        let addresses = self.onchain_cluster.wallet.get_addresses();

        let recipient = addresses[1];
        let tx = self
            .onchain_cluster
            .test_transaction_builder()
            .await
            .transfer_sui(Some(1_000), recipient)
            .build();
        let tx_digest = tx.digest().to_string();
        let signed_tx = self.onchain_cluster.wallet.sign_transaction(&tx).await;
        let (tx_bytes, sigs) = signed_tx.to_tx_bytes_and_signatures();
        let tx_bytes = tx_bytes.encoded();
        let sigs: Vec<_> = sigs.iter().map(|sig| sig.encoded()).collect();

        Ok((tx_digest, tx_bytes, sigs))
    }

    /// Builds a transaction that would abort if called by a normal user.
    async fn privileged_transaction(&self) -> anyhow::Result<(String, String, Vec<String>)> {
        let tx: sui_types::transaction::TransactionData = self
            .onchain_cluster
            .test_transaction_builder()
            .await
            .call_request_remove_validator()
            .build();
        let tx_digest = tx.digest().to_string();
        let signed_tx = self.onchain_cluster.wallet.sign_transaction(&tx).await;
        let (tx_bytes, sigs) = signed_tx.to_tx_bytes_and_signatures();
        let tx_bytes = tx_bytes.encoded();
        let sigs: Vec<_> = sigs.iter().map(|sig| sig.encoded()).collect();

        Ok((tx_digest, tx_bytes, sigs))
    }

    async fn get_validator_address(&self) -> SuiAddress {
        self.get_validator_addresses().await[0]
    }

    async fn get_validator_addresses(&self) -> Vec<SuiAddress> {
        self.onchain_cluster
            .grpc_client()
            .get_system_state_summary(None)
            .await
            .unwrap()
            .active_validators
            .iter()
            .map(|v| v.sui_address)
            .collect()
    }

    async fn execute_jsonrpc(&self, method: String, params: Value) -> anyhow::Result<Value> {
        let query = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        });

        let response = self
            .client
            .post(self.offchain.jsonrpc_url())
            .json(&query)
            .send()
            .await
            .context("Request to JSON-RPC server failed")?;

        let body: Value = response
            .json()
            .await
            .context("Failed to parse JSON-RPC response")?;

        Ok(body)
    }
}

#[sim_test]
async fn test_execution() {
    telemetry_subscribers::init_for_testing();
    let test_cluster = FnDelegationTestCluster::new()
        .await
        .expect("Failed to create test cluster");

    let (tx_digest, tx_bytes, sigs) = test_cluster.transfer_transaction().await.unwrap();

    // Call the executeTransactionBlock method and check that the response is valid.
    let response = test_cluster
        .execute_jsonrpc(
            "sui_executeTransactionBlock".to_string(),
            json!({
                "tx_bytes": tx_bytes,
                "signatures": sigs,
                "options": {
                    "showInput": true,
                    "showRawInput": true,
                    "showEffects": true,
                    "showRawEffects": true,
                    "showEvents": true,
                    "showObjectChanges": true,
                    "showBalanceChanges": true,
                },
            }),
        )
        .await
        .unwrap();

    tracing::info!("execution rpc response is {:?}", response);

    // Checking that all the requested fields are present in the response.
    assert_eq!(response["result"]["digest"], tx_digest);
    assert!(response["result"]["transaction"].is_object());
    assert!(response["result"]["rawTransaction"].is_string());
    assert!(response["result"]["effects"].is_object());
    assert!(response["result"]["rawEffects"].is_array());
    assert!(response["result"]["events"].is_array());
    assert!(response["result"]["objectChanges"].is_array());
    assert!(response["result"]["balanceChanges"].is_array());
}

#[sim_test]
async fn test_execution_with_deprecated_mode() {
    telemetry_subscribers::init_for_testing();

    let test_cluster = FnDelegationTestCluster::new()
        .await
        .expect("Failed to create test cluster");

    let (_, tx_bytes, sigs) = test_cluster.transfer_transaction().await.unwrap();

    // Call the executeTransactionBlock method and check that the response is valid.
    let response = test_cluster
        .execute_jsonrpc(
            "sui_executeTransactionBlock".to_string(),
            json!({
                "tx_bytes": tx_bytes,
                "signatures": sigs,
                "request_type": "WaitForLocalExecution",
            }),
        )
        .await
        .unwrap();

    tracing::info!("execution rpc response is {:?}", response);

    assert_eq!(response["error"]["code"], -32602);
    assert_eq!(
        response["error"]["message"],
        "Invalid Params: WaitForLocalExecution mode is deprecated"
    );
}

#[sim_test]
async fn test_execution_with_no_sigs() {
    telemetry_subscribers::init_for_testing();

    let test_cluster = FnDelegationTestCluster::new()
        .await
        .expect("Failed to create test cluster");

    let (_, tx_bytes, _) = test_cluster.transfer_transaction().await.unwrap();

    // Call the executeTransactionBlock method and check that the response is valid.
    let response = test_cluster
        .execute_jsonrpc(
            "sui_executeTransactionBlock".to_string(),
            json!({
                "tx_bytes": tx_bytes,
            }),
        )
        .await
        .unwrap();

    tracing::info!("execution rpc response is {:?}", response);

    assert_eq!(response["error"]["code"], -32602);
    assert_eq!(response["error"]["message"], "Invalid params");
    assert!(
        response["error"]["data"]
            .as_str()
            .unwrap()
            .starts_with("missing field `signatures`")
    );
}

#[sim_test]
async fn test_execution_with_empty_sigs() {
    telemetry_subscribers::init_for_testing();

    let test_cluster = FnDelegationTestCluster::new()
        .await
        .expect("Failed to create test cluster");

    let (_, tx_bytes, _) = test_cluster.transfer_transaction().await.unwrap();

    // Call the executeTransactionBlock method and check that the response is valid.
    let response = test_cluster
        .execute_jsonrpc(
            "sui_executeTransactionBlock".to_string(),
            json!({
                "tx_bytes": tx_bytes,
                "signatures": [],
            }),
        )
        .await
        .unwrap();

    tracing::info!("execution rpc response is {:?}", response);

    assert_eq!(response["error"]["code"], -32002);
    assert_eq!(
        response["error"]["message"],
        "Invalid user signature: Expect 1 signer signatures but got 0"
    );
}

#[sim_test]
async fn test_execution_with_aborted_tx() {
    telemetry_subscribers::init_for_testing();

    let test_cluster = FnDelegationTestCluster::new()
        .await
        .expect("Failed to create test cluster");

    let (_, tx_bytes, sigs) = test_cluster.privileged_transaction().await.unwrap();

    // Call the executeTransactionBlock method and check that the response is valid.
    let response = test_cluster
        .execute_jsonrpc(
            "sui_executeTransactionBlock".to_string(),
            json!({
                "tx_bytes": tx_bytes,
                "signatures": sigs,
                "options": {
                    "showEffects": true,
                },
            }),
        )
        .await
        .unwrap();

    tracing::info!("execution rpc response is {:?}", response);

    assert_eq!(response["result"]["effects"]["status"]["status"], "failure");
}

#[sim_test]
async fn test_dry_run() {
    let test_cluster = FnDelegationTestCluster::new()
        .await
        .expect("Failed to create test cluster");

    let (_, tx_bytes, _) = test_cluster.transfer_transaction().await.unwrap();

    let response = test_cluster
        .execute_jsonrpc(
            "sui_dryRunTransactionBlock".to_string(),
            json!({
                "tx_bytes": tx_bytes,
            }),
        )
        .await
        .unwrap();

    assert_eq!(response["result"]["effects"]["status"]["status"], "success");
}

#[sim_test]
async fn test_dry_run_with_invalid_tx() {
    let test_cluster = FnDelegationTestCluster::new()
        .await
        .expect("Failed to create test cluster");

    let response = test_cluster
        .execute_jsonrpc(
            "sui_dryRunTransactionBlock".to_string(),
            json!({
                "tx_bytes": "invalid_tx_bytes",
            }),
        )
        .await
        .unwrap();

    assert_eq!(response["error"]["code"], -32602);
    assert_eq!(response["error"]["message"], "Invalid params");
    assert!(
        response["error"]["data"]
            .as_str()
            .unwrap()
            .starts_with("Invalid value was given to the function")
    );
}

#[sim_test]
async fn test_get_stakes_and_by_ids() {
    let test_cluster = FnDelegationTestCluster::new()
        .await
        .expect("Failed to create test cluster");

    let wallet = &test_cluster.onchain_cluster.wallet;

    // Execute a staking transaction so we have a stake to query.
    let validator_address = test_cluster.get_validator_address().await;
    let staking_transaction = make_staking_transaction(wallet, validator_address).await;
    let stake_owner_address = staking_transaction.data().transaction_data().sender();

    wallet
        .execute_transaction_must_succeed(staking_transaction)
        .await;

    // Get the stake by owner.
    let get_stakes_response = test_cluster
        .execute_jsonrpc(
            "suix_getStakes".to_string(),
            json!({ "owner": stake_owner_address }),
        )
        .await
        .unwrap();

    assert_eq!(
        get_stakes_response["result"][0]["validatorAddress"],
        validator_address.to_string().as_str()
    );
    assert!(get_stakes_response["result"][0]["stakes"][0]["stakedSuiId"].is_string());
    let stake_id = get_stakes_response["result"][0]["stakes"][0]["stakedSuiId"]
        .as_str()
        .unwrap();

    // Now get the stake by id.
    let get_stakes_by_ids_response = test_cluster
        .execute_jsonrpc(
            "suix_getStakesByIds".to_string(),
            json!({ "staked_sui_ids": [stake_id] }),
        )
        .await
        .unwrap();

    // Two responses should match.
    assert_eq!(get_stakes_response, get_stakes_by_ids_response);
}

#[sim_test]
async fn test_get_stakes_invalid_params() {
    let test_cluster = FnDelegationTestCluster::new()
        .await
        .expect("Failed to create test cluster");

    let response = test_cluster
        .execute_jsonrpc(
            "suix_getStakes".to_string(),
            json!({ "owner": "invalid_address" }),
        )
        .await
        .unwrap();

    // Check that we have all the error information in the response.
    assert_eq!(response["error"]["code"], -32602);
    assert_eq!(response["error"]["message"], "Invalid params");
    assert!(
        response["error"]["data"]
            .as_str()
            .unwrap()
            .contains("Deserialization failed")
    );

    let response = test_cluster
        .execute_jsonrpc(
            "suix_getStakesByIds".to_string(),
            json!({ "staked_sui_ids": ["invalid_stake_id"] }),
        )
        .await
        .unwrap();

    assert_eq!(response["error"]["code"], -32602);
    assert_eq!(response["error"]["message"], "Invalid params");
    assert!(
        response["error"]["data"]
            .as_str()
            .unwrap()
            .contains("AccountAddressParseError")
    );
}

#[sim_test]
async fn test_get_validators_apy() {
    let test_cluster = FnDelegationTestCluster::new()
        .await
        .expect("Failed to create test cluster");

    let validator_address = test_cluster.get_validator_address().await;

    let response = test_cluster
        .execute_jsonrpc("suix_getValidatorsApy".to_string(), json!({}))
        .await
        .unwrap();

    assert_eq!(
        response["result"]["apys"][0]["address"],
        validator_address.to_string()
    );
}
