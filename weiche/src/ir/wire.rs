//! versioned per-gateway payload carried in a Kubernetes Secret.
use super::GatewaySnapshot;
use anyhow::{Context, ensure};
use flate2::{Compression, read::ZlibDecoder, write::ZlibEncoder};
use ring::digest::{SHA256, digest};
use std::io::{Read, Write};

/// key used for the snapshot payload in the gateway Secret.
pub const SNAPSHOT_KEY: &str = "routes.v1";
/// maximum uncompressed snapshot size accepted by the workers.
const MAX_DECODED: usize = 64 * 1024 * 1024;

impl GatewaySnapshot {
    /// encode deterministically so independent controller replicas agree on the version.
    pub fn encode(&self) -> anyhow::Result<Vec<u8>> {
        let data = rmp_serde::to_vec_named(self)?;
        // fail before publication so the controller withdraws routes workers cannot decode.
        ensure!(data.len() <= MAX_DECODED, "snapshot exceeds decoded size limit");
        let mut writer = ZlibEncoder::new(Vec::new(), Compression::fast());
        writer.write_all(&data)?;
        let payload = writer.finish()?;
        // a rejected Secret write would leave the previous routes serving.
        ensure!(
            payload.len() <= 1024 * 1024,
            "snapshot exceeds Kubernetes Secret size limit"
        );
        Ok(payload)
    }

    /// validate the wire representation and reconstruct the worker-local TLS settings.
    pub fn decode(payload: &[u8]) -> anyhow::Result<Self> {
        let mut data = Vec::new();
        // read one extra byte to distinguish an exact fit from an oversized expansion.
        ZlibDecoder::new(payload)
            .take(MAX_DECODED as u64 + 1)
            .read_to_end(&mut data)?;
        ensure!(data.len() <= MAX_DECODED, "snapshot exceeds decoded size limit");
        let mut snapshot: Self =
            rmp_serde::from_slice(&data).context("decode gateway snapshot")?;
        // older controllers could order an exact name after an equally long wildcard.
        snapshot.order_certificates();
        Ok(snapshot)
    }

    /// hash only gateway-owned listener bindings, certificates and reserved hostnames.
    pub fn listener_version(&self) -> anyhow::Result<String> {
        let sockets: Vec<_> = self
            .sockets
            .iter()
            .map(|(addr, s)| (addr, &s.listener, s.hostnames.as_slice()))
            .collect();
        let streams: Vec<_> = self
            .streams
            .iter()
            .map(|(key, s)| (key, s.gateway_uid.as_str()))
            .collect();
        let tls: Vec<_> = self
            .tls
            .iter()
            .map(|(addr, t)| {
                let hosts: Vec<_> = t.hosts.keys().map(String::as_str).collect();
                (addr, t.gateway_uid.as_str(), hosts)
            })
            .collect();
        let bytes = rmp_serde::to_vec(&(&sockets, &streams, &tls, &self.forwarding))?;
        Ok(version(&bytes))
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
