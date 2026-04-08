// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! BigTable Store implementation for sui-indexer-alt-framework.
//!
//! This implements the `Store`, `Connection`, and `ConcurrentConnection` traits to allow the
//! new framework to use BigTable for watermark storage. Per-pipeline watermarks are stored in
//! the `watermark_alt` table with three columns:
//! - `w` (BCS legacy `Watermark`) — kept in sync for backward compatibility.
//! - `w2` (JSON `WatermarkV2`) — the source of truth for new code paths.
//! - `v` (u64 BE version) — gates optimistic-locking CAS writes of `w2`.

use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use anyhow::Result;
use anyhow::bail;
use async_trait::async_trait;
use sui_indexer_alt_framework_store_traits::CommitterWatermark;
use sui_indexer_alt_framework_store_traits::ConcurrentConnection;
use sui_indexer_alt_framework_store_traits::Connection;
use sui_indexer_alt_framework_store_traits::InitWatermark;
use sui_indexer_alt_framework_store_traits::PrunerWatermark;
use sui_indexer_alt_framework_store_traits::ReaderWatermark;
use sui_indexer_alt_framework_store_traits::Store;

use crate::WatermarkV2;
use crate::bigtable::client::BigTableClient;

/// A Store implementation backed by BigTable.
#[derive(Clone)]
pub struct BigTableStore {
    client: BigTableClient,
}

/// A connection to BigTable for watermark operations and data writes.
pub struct BigTableConnection<'a> {
    client: BigTableClient,
    _marker: std::marker::PhantomData<&'a ()>,
}

impl BigTableStore {
    pub fn new(client: BigTableClient) -> Self {
        Self { client }
    }
}

impl BigTableConnection<'_> {
    /// Returns a mutable reference to the underlying BigTable client.
    pub fn client(&mut self) -> &mut BigTableClient {
        &mut self.client
    }

    /// Read a watermark for read-side methods. Enforces the "hide if `checkpoint < reader_lo`
    /// or `checkpoint == None`" rule and returns the unwrapped checkpoint for callers that
    /// need it.
    async fn get_watermark_for_read(
        &mut self,
        pipeline: &str,
    ) -> Result<Option<(WatermarkV2, u64)>> {
        let (Some((watermark, _)), _) = self.client.get_pipeline_watermark(pipeline).await? else {
            return Ok(None);
        };
        let Some(checkpoint_hi_inclusive) = watermark
            .checkpoint_hi_inclusive
            .filter(|&cp| cp >= watermark.reader_lo)
        else {
            return Ok(None);
        };
        Ok(Some((watermark, checkpoint_hi_inclusive)))
    }

    async fn get_watermark_for_write(&mut self, pipeline: &str) -> Result<(WatermarkV2, u64)> {
        let (Some((watermark, version)), _) = self.client.get_pipeline_watermark(pipeline).await?
        else {
            bail!("no watermark for pipeline {}", pipeline);
        };
        Ok((watermark, version))
    }
}

#[async_trait]
impl sui_indexer_alt_framework_store_traits::ConcurrentStore for BigTableStore {
    type ConcurrentConnection<'c> = BigTableConnection<'c>;
}

#[async_trait]
impl Store for BigTableStore {
    type Connection<'c> = BigTableConnection<'c>;

    async fn connect<'c>(&'c self) -> Result<Self::Connection<'c>> {
        Ok(BigTableConnection {
            client: self.client.clone(),
            _marker: std::marker::PhantomData,
        })
    }
}

#[async_trait]
impl Connection for BigTableConnection<'_> {
    async fn init_watermark(
        &mut self,
        pipeline_task: &str,
        checkpoint_hi_inclusive: Option<u64>,
    ) -> Result<Option<InitWatermark>> {
        let (existing_new, existing_legacy) =
            self.client.get_pipeline_watermark(pipeline_task).await?;

        // Case 1: row already in the new format -> return its values, no write.
        if let Some((wm, _version)) = existing_new {
            return Ok(Some(InitWatermark {
                checkpoint_hi_inclusive: wm.checkpoint_hi_inclusive,
                reader_lo: Some(wm.reader_lo),
            }));
        }

        // Case 2: legacy-only row -> bootstrap a new-format watermark from the legacy committer
        // fields.
        // Case 3: nothing exists -> write a fresh row from the framework's input.
        let initial = if let Some(legacy) = existing_legacy {
            let reader_lo = legacy.checkpoint_hi_inclusive + 1;
            WatermarkV2 {
                epoch_hi_inclusive: legacy.epoch_hi_inclusive,
                checkpoint_hi_inclusive: Some(legacy.checkpoint_hi_inclusive),
                tx_hi: legacy.tx_hi,
                timestamp_ms_hi_inclusive: legacy.timestamp_ms_hi_inclusive,
                reader_lo,
                pruner_hi: reader_lo,
                pruner_timestamp_ms: 0,
            }
        } else {
            let reader_lo = checkpoint_hi_inclusive.map_or(0, |cp| cp + 1);
            WatermarkV2 {
                epoch_hi_inclusive: 0,
                checkpoint_hi_inclusive,
                tx_hi: 0,
                timestamp_ms_hi_inclusive: 0,
                reader_lo,
                pruner_hi: reader_lo,
                pruner_timestamp_ms: 0,
            }
        };

        // Conditionally create the row. Tolerate the race where another writer beat us — fall
        // through to a re-read in that case.
        let _ = self
            .client
            .create_pipeline_watermark_if_absent(pipeline_task, &initial)
            .await?;

        let (Some((wm, _version)), _) = self.client.get_pipeline_watermark(pipeline_task).await?
        else {
            // Should be impossible: we just wrote (or someone else did).
            return Ok(None);
        };
        Ok(Some(InitWatermark {
            checkpoint_hi_inclusive: wm.checkpoint_hi_inclusive,
            reader_lo: Some(wm.reader_lo),
        }))
    }

    async fn accepts_chain_id(
        &mut self,
        _pipeline_task: &str,
        _chain_id: [u8; 32],
    ) -> Result<bool> {
        // TODO: Implement storing chain_id
        Ok(true)
    }

    async fn committer_watermark(
        &mut self,
        pipeline_task: &str,
    ) -> Result<Option<CommitterWatermark>> {
        Ok(self.get_watermark_for_read(pipeline_task).await?.map(
            |(wm, checkpoint_hi_inclusive)| CommitterWatermark {
                epoch_hi_inclusive: wm.epoch_hi_inclusive,
                checkpoint_hi_inclusive,
                tx_hi: wm.tx_hi,
                timestamp_ms_hi_inclusive: wm.timestamp_ms_hi_inclusive,
            },
        ))
    }

    async fn set_committer_watermark(
        &mut self,
        pipeline_task: &str,
        watermark: CommitterWatermark,
    ) -> Result<bool> {
        let (current, version) = self.get_watermark_for_write(pipeline_task).await?;
        if current
            .checkpoint_hi_inclusive
            .is_some_and(|cp| cp >= watermark.checkpoint_hi_inclusive)
        {
            return Ok(false);
        }
        let new = WatermarkV2 {
            epoch_hi_inclusive: watermark.epoch_hi_inclusive,
            checkpoint_hi_inclusive: Some(watermark.checkpoint_hi_inclusive),
            tx_hi: watermark.tx_hi,
            timestamp_ms_hi_inclusive: watermark.timestamp_ms_hi_inclusive,
            ..current
        };
        self.client
            .cas_pipeline_watermark(pipeline_task, &new, version)
            .await
    }
}

#[async_trait]
impl ConcurrentConnection for BigTableConnection<'_> {
    async fn reader_watermark(&mut self, pipeline: &str) -> Result<Option<ReaderWatermark>> {
        Ok(self
            .get_watermark_for_read(pipeline)
            .await?
            .map(|(wm, checkpoint_hi_inclusive)| ReaderWatermark {
                checkpoint_hi_inclusive,
                reader_lo: wm.reader_lo,
            }))
    }

    async fn pruner_watermark(
        &mut self,
        pipeline: &'static str,
        delay: Duration,
    ) -> Result<Option<PrunerWatermark>> {
        let Some((watermark, _)) = self.get_watermark_for_read(pipeline).await? else {
            return Ok(None);
        };
        // Compute max(0, (pruner_timestamp + delay) - now). Use u128 to avoid overflow when
        // summing the two operands, and saturating_sub so we never underflow when the wait
        // period has already elapsed. saturating_sub is safe because callers treat anything
        // < 1 the same.
        let pruner_ready_ms = (watermark.pruner_timestamp_ms as u128) + delay.as_millis();
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
        let wait_for_ms = i64::try_from(pruner_ready_ms.saturating_sub(now_ms))?;
        Ok(Some(PrunerWatermark {
            wait_for_ms,
            reader_lo: watermark.reader_lo,
            pruner_hi: watermark.pruner_hi,
        }))
    }

    async fn set_reader_watermark(
        &mut self,
        pipeline: &'static str,
        reader_lo: u64,
    ) -> Result<bool> {
        let (current, version) = self.get_watermark_for_write(pipeline).await?;
        let new = WatermarkV2 {
            reader_lo,
            ..current
        };
        self.client
            .cas_pipeline_watermark(pipeline, &new, version)
            .await
    }

    async fn set_pruner_watermark(
        &mut self,
        pipeline: &'static str,
        pruner_hi: u64,
    ) -> Result<bool> {
        let (current, version) = self.get_watermark_for_write(pipeline).await?;
        let new = WatermarkV2 {
            pruner_hi,
            ..current
        };
        self.client
            .cas_pipeline_watermark(pipeline, &new, version)
            .await
    }
}
