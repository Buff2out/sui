// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use sui_macros::sim_test;
use test_cluster::TestClusterBuilder;

use super::testing::object_wrapping_harness;
use super::testing::*;

#[sim_test]
async fn test_transaction_subscription() {
    let mut validator_cluster = TestClusterBuilder::new()
        .with_num_validators(1)
        .build()
        .await;
    let cluster = SubscriptionTestCluster::new(&validator_cluster).await;
    let sender = validator_cluster.wallet.active_address().unwrap();

    let query = r#"subscription {
        transactions(filter: { sentAddress: "SENDER" }) {
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
    }"#
    .replace("SENDER", &sender.to_string());
    let mut stream = cluster.subscribe(&query).await;
    transfer_coins(&mut validator_cluster, &[1000]).await;
    let item = stream.next_item().await;

    insta::assert_json_snapshot!("transaction_subscription", item);
}

#[sim_test]
async fn test_transaction_subscription_object_changes() {
    let mut validator_cluster = TestClusterBuilder::new()
        .with_num_validators(1)
        .build()
        .await;
    let cluster = SubscriptionTestCluster::new(&validator_cluster).await;
    let sender = validator_cluster.wallet.active_address().unwrap();
    let package_id = object_wrapping_harness::publish(&mut validator_cluster).await;

    let query = r#"subscription {
        transactions(filter: { sentAddress: "SENDER" }) {
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
    }"#
    .replace("SENDER", &sender.to_string());
    let mut stream = cluster.subscribe(&query).await;

    let (digest, _) =
        object_wrapping_harness::create_item(&mut validator_cluster, package_id, 42).await;
    let item = stream
        .wait_for_matching_item(&[digest], transaction_digest)
        .await;

    insta::assert_json_snapshot!("transaction_subscription_object_changes", item);
}
