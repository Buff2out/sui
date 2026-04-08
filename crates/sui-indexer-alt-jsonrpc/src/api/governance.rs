// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::Context as _;
use diesel::ExpressionMethods;
use diesel::QueryDsl;
use futures::future;

use jsonrpsee::core::RpcResult;
use jsonrpsee::http_client::HttpClient;
use jsonrpsee::proc_macros::rpc;
use move_core_types::language_storage::StructTag;
use std::sync::Arc;

use async_graphql::dataloader::DataLoader;
use sui_indexer_alt_reader::governance::RewardsKey;
use sui_indexer_alt_reader::governance::ValidatorAddressKey;
use sui_indexer_alt_reader::sui_rpc_client::SuiRpcClient;
use sui_indexer_alt_schema::schema::kv_epoch_starts;
use sui_json_rpc_api::GovernanceReadApiClient;
use sui_json_rpc_types::DelegatedStake;
use sui_json_rpc_types::Page;
use sui_json_rpc_types::Stake;
use sui_json_rpc_types::StakeStatus;
use sui_json_rpc_types::ValidatorApys;
use sui_open_rpc::Module;
use sui_open_rpc_macros::open_rpc;
use sui_types::SUI_SYSTEM_ADDRESS;
use sui_types::SUI_SYSTEM_STATE_OBJECT_ID;
use sui_types::TypeTag;
use sui_types::base_types::ObjectID;
use sui_types::base_types::SuiAddress;
use sui_types::dynamic_field::Field;
use sui_types::dynamic_field::derive_dynamic_field_id;
use sui_types::governance::STAKED_SUI_STRUCT_NAME;
use sui_types::governance::STAKING_POOL_MODULE_NAME;
use sui_types::governance::StakedSui;
use sui_types::sui_serde::BigInt;
use sui_types::sui_system_state::SuiSystemStateTrait;
use sui_types::sui_system_state::SuiSystemStateWrapper;
use sui_types::sui_system_state::sui_system_state_inner_v1::SuiSystemStateInnerV1;
use sui_types::sui_system_state::sui_system_state_inner_v2::SuiSystemStateInnerV2;
use sui_types::sui_system_state::sui_system_state_summary::SuiSystemStateSummary;

use crate::api::objects::filter;
use crate::api::objects::filter::SuiObjectDataFilter;
use crate::api::rpc_module::RpcModule;
use crate::context::Context;
use crate::data::load_live_deserialized;
use crate::error::RpcError;
use crate::error::client_error_to_error_object;
use crate::error::rpc_bail;

#[open_rpc(namespace = "suix", tag = "Governance API")]
#[rpc(server, namespace = "suix")]
trait GovernanceApi {
    /// Return the reference gas price for the network as of the latest epoch.
    #[method(name = "getReferenceGasPrice")]
    async fn get_reference_gas_price(&self) -> RpcResult<BigInt<u64>>;

    /// Return a summary of the latest version of the Sui System State object (0x5), on-chain.
    #[method(name = "getLatestSuiSystemState")]
    async fn get_latest_sui_system_state(&self) -> RpcResult<SuiSystemStateSummary>;
}

#[open_rpc(namespace = "suix", tag = "Delegation Governance API")]
#[rpc(server, namespace = "suix")]
trait DelegationGovernanceApi {
    /// Return the validator APY
    #[method(name = "getValidatorsApy")]
    async fn get_validators_apy(&self) -> RpcResult<ValidatorApys>;
}

#[open_rpc(namespace = "suix", tag = "Grpc Delegation Governance API")]
#[rpc(server, namespace = "suix")]
trait GrpcDelegationGovernanceApi {
    /// Return one or more [DelegatedStake]. If a Stake was withdrawn its status will be Unstaked.
    #[method(name = "getStakesByIds")]
    async fn get_stakes_by_ids(
        &self,
        staked_sui_ids: Vec<ObjectID>,
    ) -> RpcResult<Vec<DelegatedStake>>;

    /// Return all [DelegatedStake].
    #[method(name = "getStakes")]
    async fn get_stakes(&self, owner: SuiAddress) -> RpcResult<Vec<DelegatedStake>>;
}

pub(crate) struct Governance(pub Context);
pub(crate) struct DelegationGovernance(HttpClient);
pub(crate) struct GrpcDelegationGovernance {
    ctx: Context,
    rpc_loader: Arc<DataLoader<SuiRpcClient>>,
}

impl GrpcDelegationGovernance {
    pub(crate) fn new(ctx: Context, rpc_loader: Arc<DataLoader<SuiRpcClient>>) -> Self {
        Self { ctx, rpc_loader }
    }
}

impl DelegationGovernance {
    pub(crate) fn new(client: HttpClient) -> Self {
        Self(client)
    }
}

impl GrpcDelegationGovernance {
    /// Given a list of StakedSui object IDs, load them, fetch rewards and validator addresses,
    /// and return grouped DelegatedStake entries.
    async fn delegated_stakes(
        &self,
        stake_ids: Vec<ObjectID>,
    ) -> Result<Vec<DelegatedStake>, RpcError> {
        let staked_sui_futures = stake_ids
            .iter()
            .map(|id| load_live_deserialized::<StakedSui>(&self.ctx, *id));
        let staked_suis: Vec<StakedSui> = future::try_join_all(staked_sui_futures)
            .await
            .context("Failed to load StakedSui objects")?;

        let reward_keys: Vec<RewardsKey> = staked_suis
            .iter()
            .map(|s: &StakedSui| RewardsKey(s.id().into()))
            .collect();
        let validator_keys: Vec<ValidatorAddressKey> = staked_suis
            .iter()
            .map(|s: &StakedSui| ValidatorAddressKey(s.pool_id().into()))
            .collect();

        let rewards = self
            .rpc_loader
            .load_many(reward_keys.clone())
            .await
            .context("Failed to dry run rewards calculation")?;
        let validator_addresses = self
            .rpc_loader
            .load_many(validator_keys.clone())
            .await
            .context("Failed to dry run validator address lookup")?;
        let current_epoch = latest_epoch(&self.ctx).await?;

        let mut grouped: std::collections::BTreeMap<(SuiAddress, ObjectID), Vec<Stake>> =
            std::collections::BTreeMap::new();

        for (staked_sui, (rk, vk)) in staked_suis
            .iter()
            .zip(reward_keys.iter().zip(validator_keys.iter()))
        {
            let estimated_reward = rewards.get(rk).copied().unwrap_or(0);
            let validator_address: SuiAddress = validator_addresses
                .get(vk)
                .map(|addr| SuiAddress::from(ObjectID::from(*addr)))
                .unwrap_or_default();

            let status = if current_epoch >= staked_sui.activation_epoch() {
                StakeStatus::Active { estimated_reward }
            } else {
                StakeStatus::Pending
            };

            grouped
                .entry((validator_address, staked_sui.pool_id()))
                .or_default()
                .push(Stake {
                    staked_sui_id: staked_sui.id(),
                    stake_request_epoch: staked_sui.request_epoch(),
                    stake_active_epoch: staked_sui.activation_epoch(),
                    principal: staked_sui.principal(),
                    status,
                });
        }

        Ok(grouped
            .into_iter()
            .map(
                |((validator_address, staking_pool), stakes)| DelegatedStake {
                    validator_address,
                    staking_pool,
                    stakes,
                },
            )
            .collect())
    }
}

#[async_trait::async_trait]
impl GovernanceApiServer for Governance {
    async fn get_reference_gas_price(&self) -> RpcResult<BigInt<u64>> {
        Ok(rgp_response(&self.0).await?)
    }

    async fn get_latest_sui_system_state(&self) -> RpcResult<SuiSystemStateSummary> {
        Ok(latest_sui_system_state_response(&self.0).await?)
    }
}

#[async_trait::async_trait]
impl GrpcDelegationGovernanceApiServer for GrpcDelegationGovernance {
    async fn get_stakes_by_ids(
        &self,
        staked_sui_ids: Vec<ObjectID>,
    ) -> RpcResult<Vec<DelegatedStake>> {
        Ok(self.delegated_stakes(staked_sui_ids).await?)
    }

    async fn get_stakes(&self, owner: SuiAddress) -> RpcResult<Vec<DelegatedStake>> {
        let config = &self.ctx.config().objects;

        let type_filter = Some(SuiObjectDataFilter::StructType(StructTag {
            address: SUI_SYSTEM_ADDRESS,
            module: STAKING_POOL_MODULE_NAME.to_owned(),
            name: STAKED_SUI_STRUCT_NAME.to_owned(),
            type_params: vec![],
        }));

        let mut all_stake_ids: Vec<ObjectID> = Vec::new();
        let mut after_cursor = None;

        loop {
            let Page {
                data: stake_ids,
                next_cursor,
                has_next_page,
            } = filter::owned_objects(
                &self.ctx,
                owner,
                &type_filter,
                after_cursor,
                Some(config.max_page_size),
            )
            .await?;

            all_stake_ids.extend(stake_ids);
            if !has_next_page {
                break;
            }
            after_cursor = next_cursor;
        }

        Ok(self.delegated_stakes(all_stake_ids).await?)
    }
}

#[async_trait::async_trait]
impl DelegationGovernanceApiServer for DelegationGovernance {
    async fn get_validators_apy(&self) -> RpcResult<ValidatorApys> {
        let Self(client) = self;

        client
            .get_validators_apy()
            .await
            .map_err(client_error_to_error_object)
    }
}

impl RpcModule for Governance {
    fn schema(&self) -> Module {
        GovernanceApiOpenRpc::module_doc()
    }

    fn into_impl(self) -> jsonrpsee::RpcModule<Self> {
        self.into_rpc()
    }
}

impl RpcModule for GrpcDelegationGovernance {
    fn schema(&self) -> Module {
        GrpcDelegationGovernanceApiOpenRpc::module_doc()
    }

    fn into_impl(self) -> jsonrpsee::RpcModule<Self> {
        self.into_rpc()
    }
}

impl RpcModule for DelegationGovernance {
    fn schema(&self) -> Module {
        DelegationGovernanceApiOpenRpc::module_doc()
    }

    fn into_impl(self) -> jsonrpsee::RpcModule<Self> {
        self.into_rpc()
    }
}

/// Load data and generate response for `getReferenceGasPrice`.
async fn rgp_response(ctx: &Context) -> Result<BigInt<u64>, RpcError> {
    use kv_epoch_starts::dsl as e;

    let mut conn = ctx
        .pg_reader()
        .connect()
        .await
        .context("Failed to connect to the database")?;

    let rgp: i64 = conn
        .first(
            e::kv_epoch_starts
                .select(e::reference_gas_price)
                .order(e::epoch.desc()),
        )
        .await
        .context("Failed to fetch the reference gas price")?;

    Ok((rgp as u64).into())
}

/// Fetch the latest indexed epoch from `kv_epoch_starts`.
async fn latest_epoch(ctx: &Context) -> Result<u64, RpcError> {
    use kv_epoch_starts::dsl as e;

    let mut conn = ctx
        .pg_reader()
        .connect()
        .await
        .context("Failed to connect to the database")?;

    let epoch: i64 = conn
        .first(e::kv_epoch_starts.select(e::epoch).order(e::epoch.desc()))
        .await
        .context("Failed to fetch the latest epoch")?;

    Ok(epoch as u64)
}

/// Load data and generate response for `getLatestSuiSystemState`.
async fn latest_sui_system_state_response(
    ctx: &Context,
) -> Result<SuiSystemStateSummary, RpcError> {
    let wrapper: SuiSystemStateWrapper = load_live_deserialized(ctx, SUI_SYSTEM_STATE_OBJECT_ID)
        .await
        .context("Failed to fetch system state wrapper object")?;

    let inner_id = derive_dynamic_field_id(
        SUI_SYSTEM_STATE_OBJECT_ID,
        &TypeTag::U64,
        &bcs::to_bytes(&wrapper.version).context("Failed to serialize system state version")?,
    )
    .context("Failed to derive inner system state field ID")?;

    Ok(match wrapper.version {
        1 => load_live_deserialized::<Field<u64, SuiSystemStateInnerV1>>(ctx, inner_id)
            .await
            .context("Failed to fetch inner system state object")?
            .value
            .into_sui_system_state_summary(),
        2 => load_live_deserialized::<Field<u64, SuiSystemStateInnerV2>>(ctx, inner_id)
            .await
            .context("Failed to fetch inner system state object")?
            .value
            .into_sui_system_state_summary(),
        v => rpc_bail!("Unexpected inner system state version: {v}"),
    })
}
