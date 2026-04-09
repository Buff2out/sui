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
