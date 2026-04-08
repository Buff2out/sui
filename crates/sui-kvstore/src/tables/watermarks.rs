// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Watermarks table: stores per-pipeline watermarks indexed by pipeline name.
//!
//! Each row contains up to three columns:
//! - `w` (legacy): BCS-encoded [`Watermark`]. Kept in sync alongside `w2`+`v` so
//!   existing consumers that still parse BCS keep working.
//! - `w2` (new): JSON-encoded [`WatermarkV2`] with the new `reader_lo`/`pruner_hi`/
//!   `pruner_timestamp_ms` fields and an `Option<u64>` checkpoint.
//! - `v`: u64 big-endian optimistic-locking version, incremented on every successful CAS
//!   write of `w2`.

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use bytes::Bytes;

use crate::Watermark;
use crate::WatermarkV2;

pub mod col {
    /// Legacy BCS-encoded watermark column.
    pub const WATERMARK: &str = "w";
    /// Current JSON-encoded watermark column.
    pub const WATERMARK_V2: &str = "w2";
    /// u64 big-endian optimistic-locking version for the `w2` column.
    pub const VERSION: &str = "v";
}

pub const NAME: &str = "watermark_alt";

pub fn encode_key(pipeline: &str) -> Vec<u8> {
    pipeline.as_bytes().to_vec()
}

/// Single `(w, BCS)` cell. Used by tests that seed legacy-format data.
pub fn encode_legacy(legacy: &Watermark) -> Result<(&'static str, Bytes)> {
    Ok((col::WATERMARK, Bytes::from(bcs::to_bytes(legacy)?)))
}

/// Cells to write for `watermark` at `version`: always `(w2, JSON)` + `(v, u64 BE)`, plus
/// `(w, BCS legacy)` when the watermark has a real checkpoint.
pub fn encode_for_write(
    watermark: &WatermarkV2,
    version: u64,
) -> Result<Vec<(&'static str, Bytes)>> {
    let mut cells = vec![
        (
            col::WATERMARK_V2,
            Bytes::from(serde_json::to_vec(watermark)?),
        ),
        (col::VERSION, Bytes::copy_from_slice(&version.to_be_bytes())),
    ];
    if let Some(checkpoint) = watermark.checkpoint_hi_inclusive {
        let legacy = Watermark {
            epoch_hi_inclusive: watermark.epoch_hi_inclusive,
            checkpoint_hi_inclusive: checkpoint,
            tx_hi: watermark.tx_hi,
            timestamp_ms_hi_inclusive: watermark.timestamp_ms_hi_inclusive,
        };
        cells.push((col::WATERMARK, Bytes::from(bcs::to_bytes(&legacy)?)));
    }
    Ok(cells)
}

/// Strict read of `(w2, v)`. Returns `Ok(None)` if either column is absent. This is the read
/// path used by every code path except `init_watermark`.
pub fn decode_new(row: &[(Bytes, Bytes)]) -> Result<Option<(WatermarkV2, u64)>> {
    let mut w2: Option<&Bytes> = None;
    let mut v: Option<&Bytes> = None;
    for (col, val) in row {
        match col.as_ref() {
            b if b == col::WATERMARK_V2.as_bytes() => w2 = Some(val),
            b if b == col::VERSION.as_bytes() => v = Some(val),
            _ => {}
        }
    }
    let (Some(w2), Some(v)) = (w2, v) else {
        return Ok(None);
    };
    let watermark: WatermarkV2 = serde_json::from_slice(w2)
        .context("failed to deserialize JSON watermark from `w2` column")?;
    if v.len() != 8 {
        bail!("`v` column has unexpected length {} (expected 8)", v.len());
    }
    let mut buf = [0u8; 8];
    buf.copy_from_slice(v);
    Ok(Some((watermark, u64::from_be_bytes(buf))))
}

/// Reads only the legacy `w` cell. Returns `Ok(None)` if absent. Used **only** by
/// `init_watermark` to bootstrap the new format from a legacy-only row.
pub fn decode_legacy(row: &[(Bytes, Bytes)]) -> Result<Option<Watermark>> {
    for (col, val) in row {
        if col.as_ref() == col::WATERMARK.as_bytes() {
            return Ok(Some(bcs::from_bytes(val).context(
                "failed to deserialize BCS legacy watermark from `w` column",
            )?));
        }
    }
    Ok(None)
}
