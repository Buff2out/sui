// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::Arc;

use crate::sui_rpc_client::SuiRpcClient;
use async_graphql::dataloader::Loader;
use sui_sdk_types::Address;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RewardsKey(pub Address);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ValidatorAddressKey(pub Address);

#[async_trait::async_trait]
impl Loader<RewardsKey> for SuiRpcClient {
    type Value = u64;
    type Error = Arc<tonic::Status>;

    async fn load(&self, keys: &[RewardsKey]) -> Result<HashMap<RewardsKey, u64>, Self::Error> {
        let ids: Vec<Address> = keys.iter().map(|k| k.0).collect();
        let mut client = self.client();
        let results = client.calculate_rewards(&ids).await.map_err(Arc::new)?;
        Ok(results
            .into_iter()
            .map(|(id, reward)| (RewardsKey(id), reward))
            .collect())
    }
}

#[async_trait::async_trait]
impl Loader<ValidatorAddressKey> for SuiRpcClient {
    type Value = Address;
    type Error = Arc<tonic::Status>;

    async fn load(
        &self,
        keys: &[ValidatorAddressKey],
    ) -> Result<HashMap<ValidatorAddressKey, Address>, Self::Error> {
        let ids: Vec<Address> = keys.iter().map(|k| k.0).collect();
        let mut client = self.client();
        let results = client
            .get_validator_address_by_pool_id(&ids)
            .await
            .map_err(Arc::new)?;
        Ok(results
            .into_iter()
            .map(|(id, addr)| (ValidatorAddressKey(id), addr))
            .collect())
    }
}
