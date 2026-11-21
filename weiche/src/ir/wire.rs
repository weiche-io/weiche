//! versioned per-gateway payload carried in a Kubernetes Secret.
use super::GatewaySnapshot;
use anyhow::{Context, ensure};
use flate2::{Compression, read::ZlibDecoder, write::ZlibEncoder};
use ring::digest::{SHA256, digest};
use std::io::{Read, Write};

pub const SNAPSHOT_KEY: &str = "routes.v1";
const MAX_DECODED: usize = 64 * 1024 * 1024;

impl GatewaySnapshot {
    /// encode deterministically so independent controller replicas agree on the version.
    pub fn encode(&self) -> anyhow::Result<Vec<u8>> {
        let data = rmp_serde::to_vec_named(self)?;
        ensure!(data.len() <= MAX_DECODED, "snapshot exceeds decoded size limit");
        let mut writer = ZlibEncoder::new(Vec::new(), Compression::fast());
        writer.write_all(&data)?;
        let payload = writer.finish()?;
        ensure!(
            payload.len() <= 1024 * 1024,
            "snapshot exceeds Kubernetes Secret size limit"
        );
        Ok(payload)
    }

    /// validate the wire representation and reconstruct the worker-local TLS settings.
    pub fn decode(payload: &[u8]) -> anyhow::Result<Self> {
        let mut data = Vec::new();
        ZlibDecoder::new(payload)
            .take(MAX_DECODED as u64 + 1)
            .read_to_end(&mut data)?;
        ensure!(data.len() <= MAX_DECODED, "snapshot exceeds decoded size limit");
        rmp_serde::from_slice(&data).context("decode gateway snapshot")
    }
}

/// a content digest survives controller restarts and is comparable across pods.
pub fn version(payload: &[u8]) -> String {
    digest(&SHA256, payload)
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
