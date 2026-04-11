// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use sui_macros::sim_test;
use test_cluster::TestClusterBuilder;

use super::testing::*;

#[sim_test]
async fn test_subscription_sequential() {
    let validator_cluster = TestClusterBuilder::new()
        .with_num_validators(1)
        .build()
        .await;
    let cluster = SubscriptionTestCluster::new(&validator_cluster).await;

    let mut stream = cluster
        .subscribe("subscription { checkpoints { sequenceNumber } }")
        .await;
    let items = stream.collect_items(3).await;

    insta::assert_json_snapshot!("subscription_sequential", items);
}

#[sim_test]
async fn test_subscription_fields() {
    let validator_cluster = TestClusterBuilder::new()
        .with_num_validators(1)
        .build()
        .await;
    let cluster = SubscriptionTestCluster::new(&validator_cluster).await;

    let mut stream = cluster
        .subscribe(
            r#"subscription {
                checkpoints {
                    sequenceNumber
                    digest
                    contentDigest
                    timestamp
                    networkTotalTransactions
                    rollingGasSummary {
                        computationCost
                        storageCost
                        storageRebate
                        nonRefundableStorageFee
                    }
                    epoch {
                        epochId
                    }
                    validatorSignatures {
                        signature
                        signersMap
                    }
                }
            }"#,
        )
        .await;
    let item = stream.next_item().await;

    insta::assert_json_snapshot!("subscription_fields", item);
}

#[sim_test]
async fn test_subscription_transactions() {
    let mut validator_cluster = TestClusterBuilder::new()
        .with_num_validators(1)
        .build()
        .await;
    let cluster = SubscriptionTestCluster::new(&validator_cluster).await;
    let sender = validator_cluster.wallet.active_address().unwrap();

    let query = r#"subscription {
        checkpoints {
            sequenceNumber
            transactions(filter: { sentAddress: "SENDER" }) {
                nodes {
                    digest
                    sender { address }
                    gasInput { gasBudget }
                    effects {
                        status
                        balanceChanges {
                            nodes {
                                amount
                                coinType { repr }
                                owner { address }
                            }
                        }
                    }
                }
            }
        }
    }"#
    .replace("SENDER", &sender.to_string());
    let mut stream = cluster.subscribe(&query).await;
    let digests = transfer_coins(&mut validator_cluster, &[1000]).await;
    let item = stream
        .wait_for_matching_item(&digests, checkpoint_tx_digests)
        .await;

    insta::assert_json_snapshot!("subscription_transactions", item);
}

#[sim_test]
async fn test_subscription_transactions_pagination_first() {
    let mut validator_cluster = TestClusterBuilder::new()
        .with_num_validators(1)
        .build()
        .await;
    let cluster = SubscriptionTestCluster::new(&validator_cluster).await;
    let sender = validator_cluster.wallet.active_address().unwrap();

    let query = r#"subscription {
        checkpoints {
            sequenceNumber
            transactions(first: 1, filter: { sentAddress: "SENDER" }) {
                nodes {
                    digest
                    effects {
                        status
                        balanceChanges {
                            nodes {
                                amount
                                coinType { repr }
                            }
                        }
                    }
                }
                edges { cursor }
                pageInfo { hasNextPage hasPreviousPage }
            }
        }
    }"#
    .replace("SENDER", &sender.to_string());
    let mut stream = cluster.subscribe(&query).await;
    // Under sim_test, soft-bundled transactions deterministically land in the
    // same checkpoint, ordered by digest.
    let digests = transfer_coins(&mut validator_cluster, &[100, 200]).await;
    let item = stream
        .wait_for_matching_item(&digests, checkpoint_tx_digests)
        .await;

    insta::assert_json_snapshot!("subscription_transactions_pagination_first", item);
}

#[sim_test]
async fn test_subscription_transactions_pagination_last() {
    let mut validator_cluster = TestClusterBuilder::new()
        .with_num_validators(1)
        .build()
        .await;
    let cluster = SubscriptionTestCluster::new(&validator_cluster).await;
    let sender = validator_cluster.wallet.active_address().unwrap();

    let query = r#"subscription {
        checkpoints {
            sequenceNumber
            transactions(last: 1, filter: { sentAddress: "SENDER" }) {
                nodes {
                    digest
                    effects {
                        status
                        balanceChanges {
                            nodes {
                                amount
                                coinType { repr }
                            }
                        }
                    }
                }
                edges { cursor }
                pageInfo { hasNextPage hasPreviousPage }
            }
        }
    }"#
    .replace("SENDER", &sender.to_string());
    let mut stream = cluster.subscribe(&query).await;
    let digests = transfer_coins(&mut validator_cluster, &[100, 200]).await;
    let item = stream
        .wait_for_matching_item(&digests, checkpoint_tx_digests)
        .await;

    insta::assert_json_snapshot!("subscription_transactions_pagination_last", item);
}

// --- Object resolution tests ---

use super::testing::object_wrapping_harness;

#[sim_test]
async fn test_subscription_object_create() {
    let mut validator_cluster = TestClusterBuilder::new()
        .with_num_validators(1)
        .build()
        .await;
    let cluster = SubscriptionTestCluster::new(&validator_cluster).await;
    let sender = validator_cluster.wallet.active_address().unwrap();
    let package_id = object_wrapping_harness::publish(&mut validator_cluster).await;

    let query = r#"subscription {
        checkpoints {
            sequenceNumber
            transactions(filter: { sentAddress: "SENDER" }) {
                nodes {
                    digest
                    effects {
                        objectChanges {
                            nodes {
                                inputState {
                                    address
                                    version
                                    digest
                                    asMoveObject {
                                        contents { type { repr } }
                                    }
                                }
                                outputState {
                                    address
                                    version
                                    digest
                                    asMoveObject {
                                        contents { type { repr } }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }"#
    .replace("SENDER", &sender.to_string());
    let mut stream = cluster.subscribe(&query).await;

    let (digest, _) =
        object_wrapping_harness::create_item(&mut validator_cluster, package_id, 42).await;
    let item = stream
        .wait_for_matching_item(&[digest], checkpoint_tx_digests)
        .await;

    insta::assert_json_snapshot!("subscription_object_create", item);
}

#[sim_test]
async fn test_subscription_object_lifecycle() {
    let mut validator_cluster = TestClusterBuilder::new()
        .with_num_validators(1)
        .build()
        .await;
    let cluster = SubscriptionTestCluster::new(&validator_cluster).await;
    let sender = validator_cluster.wallet.active_address().unwrap();
    let package_id = object_wrapping_harness::publish(&mut validator_cluster).await;

    let query = r#"subscription {
        checkpoints {
            sequenceNumber
            transactions(filter: { sentAddress: "SENDER" }) {
                nodes {
                    digest
                    effects {
                        objectChanges {
                            nodes {
                                inputState {
                                    address
                                    version
                                    digest
                                    asMoveObject {
                                        contents { type { repr } }
                                    }
                                }
                                outputState {
                                    address
                                    version
                                    digest
                                    asMoveObject {
                                        contents { type { repr } }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }"#
    .replace("SENDER", &sender.to_string());
    let mut stream = cluster.subscribe(&query).await;

    let (d1, item) =
        object_wrapping_harness::create_item(&mut validator_cluster, package_id, 42).await;
    let cp1 = stream
        .wait_for_matching_item(&[d1], checkpoint_tx_digests)
        .await;

    let (d2, item) =
        object_wrapping_harness::update_item(&mut validator_cluster, package_id, item, 100).await;
    let cp2 = stream
        .wait_for_matching_item(&[d2], checkpoint_tx_digests)
        .await;

    let (d3, wrapper) =
        object_wrapping_harness::wrap_item(&mut validator_cluster, package_id, item).await;
    let cp3 = stream
        .wait_for_matching_item(&[d3], checkpoint_tx_digests)
        .await;

    let (d4, _) =
        object_wrapping_harness::unwrap_wrapper(&mut validator_cluster, package_id, wrapper).await;
    let cp4 = stream
        .wait_for_matching_item(&[d4], checkpoint_tx_digests)
        .await;

    insta::assert_json_snapshot!("subscription_object_lifecycle", [cp1, cp2, cp3, cp4]);
}

/// Tests that `contents.json` resolves for streamed objects when a database
/// with indexed packages is available for type layout resolution.
/// Uses #[tokio::test] because sim_test intercepts TCP, preventing Postgres access.
#[tokio::test]
async fn test_subscription_object_json() {
    use prometheus::Registry;
    use sui_pg_db::DbArgs;

    let ingestion_dir = tempfile::tempdir().unwrap();
    let mut validator_cluster = TestClusterBuilder::new()
        .with_num_validators(1)
        .with_data_ingestion_dir(ingestion_dir.path().to_owned())
        .build()
        .await;

    let db = sui_pg_db::temp::TempDb::new().expect("Failed to create TempDb");
    let database_url = db.database().url().clone();
    let writer = sui_pg_db::Db::for_write(database_url.clone(), DbArgs::default())
        .await
        .unwrap();
    writer.run_migrations(None).await.unwrap();

    let indexer = sui_indexer_alt::setup_indexer(
        database_url.clone(),
        DbArgs::default(),
        sui_indexer_alt_framework::IndexerArgs::default(),
        sui_indexer_alt_framework::ingestion::ClientArgs {
            ingestion:
                sui_indexer_alt_framework::ingestion::ingestion_client::IngestionClientArgs {
                    local_ingestion_path: Some(ingestion_dir.path().to_owned()),
                    ..Default::default()
                },
            ..Default::default()
        },
        sui_indexer_alt::config::IndexerConfig::for_test(),
        None,
        &Registry::new(),
    )
    .await
    .expect("Failed to create indexer");

    let _indexer = indexer.run().await.expect("Failed to start indexer");
    wait_for_kv_packages(&db, 0).await;

    let cluster =
        SubscriptionTestCluster::new_with_db(&validator_cluster, database_url.clone()).await;
    let sender = validator_cluster.wallet.active_address().unwrap();
    let package_id = object_wrapping_harness::publish(&mut validator_cluster).await;

    wait_for_kv_packages(&db, 0).await;

    let query = r#"subscription {
        checkpoints {
            transactions(filter: { sentAddress: "SENDER" }) {
                nodes {
                    digest
                    effects {
                        objectChanges {
                            nodes {
                                outputState {
                                    asMoveObject {
                                        contents {
                                            type { repr }
                                            json
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }"#
    .replace("SENDER", &sender.to_string());
    let mut stream = cluster.subscribe(&query).await;

    let (digest, _) =
        object_wrapping_harness::create_item(&mut validator_cluster, package_id, 42).await;
    let checkpoint = stream
        .wait_for_matching_item(&[digest], checkpoint_tx_digests)
        .await;

    insta::assert_json_snapshot!("subscription_object_json", checkpoint, {
        ".**.objectChanges.nodes" => insta::sorted_redaction(),
        ".**.digest" => "[digest]",
        ".**.type.repr" => insta::dynamic_redaction(|value, _path| {
            let s = value.as_str().unwrap();
            if let Some(idx) = s.find("::") {
                insta::internals::Content::from(format!("[pkg]{}", &s[idx..]))
            } else {
                insta::internals::Content::from(s.to_string())
            }
        }),
        ".**.json.id" => "[id]",
        ".**.json.balance" => "[balance]",
    });
}
