// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the BigTable watermark CAS / dual-write paths.
//!
//! Each test spawns its own BigTable emulator process on a random port and creates the
//! required tables. Tests require `gcloud`, `cbt`, and the BigTable emulator on PATH.

use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use bytes::Bytes;
use sui_indexer_alt_framework_store_traits::CommitterWatermark;
use sui_indexer_alt_framework_store_traits::ConcurrentConnection;
use sui_indexer_alt_framework_store_traits::Connection;
use sui_indexer_alt_framework_store_traits::Store;
use sui_kvstore::BigTableClient;
use sui_kvstore::BigTableConnection;
use sui_kvstore::BigTableStore;
use sui_kvstore::KeyValueStoreReader;
use sui_kvstore::Watermark;
use sui_kvstore::WatermarkV2;
use sui_kvstore::tables;
use sui_kvstore::testing::BigTableEmulator;
use sui_kvstore::testing::INSTANCE_ID;
use sui_kvstore::testing::create_tables;
use sui_kvstore::testing::require_bigtable_emulator;

const PIPELINE: &str = "test_pipeline";

const EPOCH_HI: u64 = 7;
const CHECKPOINT_HI: u64 = 200;
const TX_HI: u64 = 42;
const TIMESTAMP_MS_HI: u64 = 99;
const READER_LO: u64 = 123;
const PRUNER_HI: u64 = 77;

struct WatermarkHarness {
    store: BigTableStore,
    client: BigTableClient,
    _emulator: BigTableEmulator,
}

impl WatermarkHarness {
    async fn new() -> Result<Self> {
        require_bigtable_emulator();
        let emulator = tokio::task::spawn_blocking(BigTableEmulator::start)
            .await
            .context("spawn_blocking panicked")??;
        create_tables(emulator.host(), INSTANCE_ID).await?;
        let client =
            BigTableClient::new_local(emulator.host().to_string(), INSTANCE_ID.to_string()).await?;
        let store = BigTableStore::new(client.clone());
        Ok(Self {
            store,
            client,
            _emulator: emulator,
        })
    }

    async fn connect(&self) -> Result<BigTableConnection<'_>> {
        self.store.connect().await
    }

    /// Convenience wrapper around [`read_raw_cells`] that uses the harness's client.
    async fn cells(&self, pipeline: &str) -> Result<(Option<Bytes>, Option<Bytes>, Option<u64>)> {
        read_raw_cells(&mut self.client.clone(), pipeline).await
    }

    /// Call `KeyValueStoreReader::get_watermark_for_pipelines` against the harness's client.
    async fn read_watermark(&self, pipelines: &[&str]) -> Result<Option<WatermarkV2>> {
        self.client
            .clone()
            .get_watermark_for_pipelines(pipelines)
            .await
    }

    /// Bootstrap a pipeline with a committed checkpoint. `pruner_watermark` and the read-side
    /// helpers hide rows whose `checkpoint_hi_inclusive < reader_lo`. To make a row visible we
    /// need to advance the committer past `reader_lo` (which `init(None)` sets to 0 — so any
    /// committed checkpoint works).
    async fn bootstrap_with_committed_checkpoint(
        &self,
        pipeline: &'static str,
        checkpoint: u64,
    ) -> Result<()> {
        let mut conn = self.connect().await?;
        conn.init_watermark(pipeline, None).await?;
        conn.set_committer_watermark(
            pipeline,
            CommitterWatermark {
                epoch_hi_inclusive: 0,
                checkpoint_hi_inclusive: checkpoint,
                tx_hi: 0,
                timestamp_ms_hi_inclusive: 0,
            },
        )
        .await?;
        Ok(())
    }
}

/// Read the raw cells of a watermark row using the public BigTable client. Returns
/// `(w_present, w2_present, v_value_if_present)`.
async fn read_raw_cells(
    client: &mut BigTableClient,
    pipeline: &str,
) -> Result<(Option<Bytes>, Option<Bytes>, Option<u64>)> {
    let key = tables::watermarks::encode_key(pipeline);
    let rows = client
        .multi_get(tables::watermarks::NAME, vec![key.clone()], None)
        .await?;
    let mut w = None;
    let mut w2 = None;
    let mut v = None;
    for (row_key, row) in rows {
        if row_key.as_ref() != key.as_slice() {
            continue;
        }
        for (col, val) in row {
            match col.as_ref() {
                b"w" => w = Some(val),
                b"w2" => w2 = Some(val),
                b"v" => {
                    let mut buf = [0u8; 8];
                    buf.copy_from_slice(&val);
                    v = Some(u64::from_be_bytes(buf));
                }
                _ => {}
            }
        }
    }
    Ok((w, w2, v))
}

#[tokio::test]
async fn test_init_watermark_fresh_none() -> Result<()> {
    let harness = WatermarkHarness::new().await?;
    let mut conn = harness.connect().await?;
    let init = conn.init_watermark(PIPELINE, None).await?.unwrap();
    assert_eq!(init.checkpoint_hi_inclusive, None);
    assert_eq!(init.reader_lo, Some(0));

    // The row should have w2 + v but no legacy w cell.
    let (w, w2, v) = harness.cells(PIPELINE).await?;
    assert!(w.is_none(), "fresh init(None) must not write the `w` cell");
    assert!(w2.is_some(), "fresh init(None) must write the `w2` cell");
    assert_eq!(v, Some(0));
    Ok(())
}

#[tokio::test]
async fn test_init_watermark_fresh_some() -> Result<()> {
    let harness = WatermarkHarness::new().await?;
    let mut conn = harness.connect().await?;
    let init = conn
        .init_watermark(PIPELINE, Some(CHECKPOINT_HI))
        .await?
        .unwrap();
    assert_eq!(init.checkpoint_hi_inclusive, Some(CHECKPOINT_HI));
    assert_eq!(init.reader_lo, Some(CHECKPOINT_HI + 1));

    let (w, w2, v) = harness.cells(PIPELINE).await?;
    assert!(w.is_some(), "fresh init(Some) must write the `w` cell");
    assert!(w2.is_some());
    assert_eq!(v, Some(0));

    // Calling init again should return the existing values without rewriting.
    let init2 = conn.init_watermark(PIPELINE, Some(0)).await?.unwrap();
    assert_eq!(init2.checkpoint_hi_inclusive, Some(CHECKPOINT_HI));
    assert_eq!(init2.reader_lo, Some(CHECKPOINT_HI + 1));
    let (_, _, v2) = harness.cells(PIPELINE).await?;
    assert_eq!(v2, Some(0), "second init must not bump the version");
    Ok(())
}

#[tokio::test]
async fn test_init_watermark_legacy_bootstrap() -> Result<()> {
    let harness = WatermarkHarness::new().await?;

    // Seed a BCS legacy `Watermark` directly into the `w` column.
    let legacy = Watermark {
        epoch_hi_inclusive: EPOCH_HI,
        checkpoint_hi_inclusive: CHECKPOINT_HI,
        tx_hi: TX_HI,
        timestamp_ms_hi_inclusive: TIMESTAMP_MS_HI,
    };
    let cell = tables::watermarks::encode_legacy(&legacy)?;
    let entry = tables::make_entry(
        tables::watermarks::encode_key(PIPELINE),
        [cell],
        Some(TIMESTAMP_MS_HI),
    );
    harness
        .client
        .clone()
        .write_entries(tables::watermarks::NAME, [entry])
        .await?;

    // Now run init_watermark — it should bootstrap w2 + v from the legacy committer fields
    // and leave the legacy `w` cell untouched.
    let mut conn = harness.connect().await?;
    let init = conn.init_watermark(PIPELINE, Some(0)).await?.unwrap();
    assert_eq!(init.checkpoint_hi_inclusive, Some(CHECKPOINT_HI));
    assert_eq!(init.reader_lo, Some(CHECKPOINT_HI + 1));

    let (w, w2, v) = harness.cells(PIPELINE).await?;
    assert!(w.is_some(), "legacy `w` cell must be preserved");
    assert!(w2.is_some(), "new `w2` cell must be written");
    assert_eq!(v, Some(0));
    Ok(())
}

#[tokio::test]
async fn test_committer_watermark_roundtrip_and_regression() -> Result<()> {
    let harness = WatermarkHarness::new().await?;
    let mut conn = harness.connect().await?;
    conn.init_watermark(PIPELINE, None).await?;

    // First commit creates the legacy `w` cell.
    let initial = CommitterWatermark {
        epoch_hi_inclusive: EPOCH_HI / 2,
        checkpoint_hi_inclusive: CHECKPOINT_HI / 2,
        tx_hi: TX_HI / 2,
        timestamp_ms_hi_inclusive: TIMESTAMP_MS_HI / 2,
    };
    assert!(conn.set_committer_watermark(PIPELINE, initial).await?);

    let read = conn.committer_watermark(PIPELINE).await?.unwrap();
    assert_eq!(read.checkpoint_hi_inclusive, CHECKPOINT_HI / 2);
    let (w, w2, v) = harness.cells(PIPELINE).await?;
    assert!(w.is_some(), "set_committer_watermark must write `w`");
    assert!(w2.is_some());
    assert_eq!(v, Some(1));

    // Advance.
    let updated = CommitterWatermark {
        epoch_hi_inclusive: EPOCH_HI,
        checkpoint_hi_inclusive: CHECKPOINT_HI,
        tx_hi: TX_HI,
        timestamp_ms_hi_inclusive: TIMESTAMP_MS_HI,
    };
    assert!(conn.set_committer_watermark(PIPELINE, updated).await?);
    let read = conn.committer_watermark(PIPELINE).await?.unwrap();
    assert_eq!(read.checkpoint_hi_inclusive, CHECKPOINT_HI);

    // Regression must be rejected.
    let regressed = CommitterWatermark {
        epoch_hi_inclusive: EPOCH_HI,
        checkpoint_hi_inclusive: CHECKPOINT_HI / 2 + 1,
        tx_hi: TX_HI,
        timestamp_ms_hi_inclusive: TIMESTAMP_MS_HI,
    };
    assert!(!conn.set_committer_watermark(PIPELINE, regressed).await?);
    let read = conn.committer_watermark(PIPELINE).await?.unwrap();
    assert_eq!(read.checkpoint_hi_inclusive, CHECKPOINT_HI);
    Ok(())
}

#[tokio::test]
async fn test_set_reader_watermark_after_init_none_skips_legacy() -> Result<()> {
    let harness = WatermarkHarness::new().await?;
    let mut conn = harness.connect().await?;
    conn.init_watermark(PIPELINE, None).await?;

    assert!(conn.set_reader_watermark(PIPELINE, READER_LO).await?);

    let (w, w2, v) = harness.cells(PIPELINE).await?;
    assert!(
        w.is_none(),
        "set_reader_watermark must not introduce `w` when checkpoint is still None"
    );
    assert!(w2.is_some());
    assert_eq!(v, Some(1));
    Ok(())
}

#[tokio::test]
async fn test_reader_watermark_roundtrip_with_committed_checkpoint() -> Result<()> {
    let harness = WatermarkHarness::new().await?;
    harness
        .bootstrap_with_committed_checkpoint(PIPELINE, CHECKPOINT_HI)
        .await?;
    let mut conn = harness.connect().await?;

    let reader = conn.reader_watermark(PIPELINE).await?.unwrap();
    assert_eq!(reader.checkpoint_hi_inclusive, CHECKPOINT_HI);
    assert_eq!(reader.reader_lo, 0);

    assert!(conn.set_reader_watermark(PIPELINE, READER_LO).await?);
    // The legacy `w` cell must still be present after a reader-only update.
    let (w, _, _) = harness.cells(PIPELINE).await?;
    assert!(w.is_some(), "legacy `w` cell must survive reader updates");
    Ok(())
}

#[tokio::test]
async fn test_pruner_watermark_saturates_when_ready() -> Result<()> {
    let harness = WatermarkHarness::new().await?;
    harness
        .bootstrap_with_committed_checkpoint(PIPELINE, CHECKPOINT_HI)
        .await?;
    let mut conn = harness.connect().await?;

    let pruner = conn
        .pruner_watermark(PIPELINE, Duration::ZERO)
        .await?
        .unwrap();
    assert_eq!(pruner.wait_for_ms, 0);
    Ok(())
}

#[tokio::test]
async fn test_set_pruner_watermark_roundtrip() -> Result<()> {
    let harness = WatermarkHarness::new().await?;
    harness
        .bootstrap_with_committed_checkpoint(PIPELINE, CHECKPOINT_HI)
        .await?;
    let mut conn = harness.connect().await?;

    assert!(conn.set_pruner_watermark(PIPELINE, PRUNER_HI).await?);
    let pruner = conn
        .pruner_watermark(PIPELINE, Duration::ZERO)
        .await?
        .unwrap();
    assert_eq!(pruner.pruner_hi, PRUNER_HI);
    Ok(())
}

#[tokio::test]
async fn test_cas_contention() -> Result<()> {
    // Two connections fetch the same version, both try to advance the committer watermark.
    // Exactly one must succeed.
    let harness = WatermarkHarness::new().await?;
    {
        let mut conn = harness.connect().await?;
        conn.init_watermark(PIPELINE, Some(0)).await?;
    }

    let mut conn_a = harness.connect().await?;
    let mut conn_b = harness.connect().await?;

    // Both observe version 0 implicitly via committer_watermark / set_committer_watermark.
    let advance = CommitterWatermark {
        epoch_hi_inclusive: 1,
        checkpoint_hi_inclusive: 5,
        tx_hi: 1,
        timestamp_ms_hi_inclusive: 1,
    };
    let a = conn_a.set_committer_watermark(PIPELINE, advance).await?;
    let b = conn_b.set_committer_watermark(PIPELINE, advance).await?;
    // a wrote with expected_version=0; b read v=0 then b's CAS sees v=1, so it returns false.
    assert!(a, "first writer must succeed");
    assert!(!b, "second writer must lose the CAS race");
    Ok(())
}

#[tokio::test]
async fn test_get_watermark_for_pipelines_hides_init_none() -> Result<()> {
    // After init(None), the row exists but `checkpoint_hi_inclusive` is `None`. The hide
    // rule must short-circuit `get_watermark_for_pipelines` to `Ok(None)`.
    let harness = WatermarkHarness::new().await?;
    {
        let mut conn = harness.connect().await?;
        conn.init_watermark(PIPELINE, None).await?;
    }
    let wm = harness.read_watermark(&[PIPELINE]).await?;
    assert!(
        wm.is_none(),
        "init(None) row must be hidden by the read API"
    );
    Ok(())
}

#[tokio::test]
async fn test_get_watermark_for_pipelines_hides_below_reader_lo() -> Result<()> {
    // A row with a real checkpoint becomes hidden once `reader_lo` is raised past it.
    let harness = WatermarkHarness::new().await?;
    harness
        .bootstrap_with_committed_checkpoint(PIPELINE, CHECKPOINT_HI)
        .await?;
    let mut conn = harness.connect().await?;

    let visible = harness.read_watermark(&[PIPELINE]).await?;
    assert!(
        visible.is_some(),
        "row with checkpoint >= reader_lo must be visible"
    );

    conn.set_reader_watermark(PIPELINE, CHECKPOINT_HI + 1)
        .await?;
    let hidden = harness.read_watermark(&[PIPELINE]).await?;
    assert!(
        hidden.is_none(),
        "row with checkpoint < reader_lo must be hidden"
    );
    Ok(())
}

#[tokio::test]
async fn test_get_watermark_for_pipelines_ignores_legacy_only() -> Result<()> {
    // A row that only has the legacy `w` column (e.g. seeded by an older indexer) is no
    // longer surfaced after the switch to reading `w2`+`v`.
    let harness = WatermarkHarness::new().await?;
    let legacy = Watermark {
        epoch_hi_inclusive: EPOCH_HI,
        checkpoint_hi_inclusive: CHECKPOINT_HI,
        tx_hi: TX_HI,
        timestamp_ms_hi_inclusive: TIMESTAMP_MS_HI,
    };
    let cell = tables::watermarks::encode_legacy(&legacy)?;
    let entry = tables::make_entry(
        tables::watermarks::encode_key(PIPELINE),
        [cell],
        Some(TIMESTAMP_MS_HI),
    );
    harness
        .client
        .clone()
        .write_entries(tables::watermarks::NAME, [entry])
        .await?;

    let wm = harness.read_watermark(&[PIPELINE]).await?;
    assert!(
        wm.is_none(),
        "legacy-only rows must be hidden by the new read path"
    );
    Ok(())
}

#[tokio::test]
async fn test_get_watermark_for_pipelines_returns_minimum() -> Result<()> {
    // Across multiple pipelines, the read API selects the watermark with the lowest
    // `checkpoint_hi_inclusive`. If any pipeline is hidden, the whole result is `None`.
    const PIPELINE_LO: &str = "pipeline_lo";
    const PIPELINE_HI: &str = "pipeline_hi";
    const PIPELINE_MISSING: &str = "pipeline_missing";

    let harness = WatermarkHarness::new().await?;
    harness
        .bootstrap_with_committed_checkpoint(PIPELINE_LO, 50)
        .await?;
    harness
        .bootstrap_with_committed_checkpoint(PIPELINE_HI, 100)
        .await?;

    let wm = harness
        .read_watermark(&[PIPELINE_LO, PIPELINE_HI])
        .await?
        .unwrap();
    assert_eq!(
        wm.checkpoint_hi_inclusive,
        Some(50),
        "must select the minimum checkpoint across pipelines"
    );

    // Adding a missing pipeline must short-circuit to `None`.
    let wm = harness
        .read_watermark(&[PIPELINE_LO, PIPELINE_HI, PIPELINE_MISSING])
        .await?;
    assert!(
        wm.is_none(),
        "any missing pipeline must hide the whole result"
    );
    Ok(())
}
