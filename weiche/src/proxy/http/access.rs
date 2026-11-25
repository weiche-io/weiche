//! record one access event for each parsed downstream HTTP request.
use super::super::super::{
    log,
    logging::{self, Argument},
};
use http::{Method, Request, StatusCode, Uri, Version};
use std::{net::SocketAddr, time::Instant};

/// fields retained until response selection or cancellation.
pub struct Access {
    /// remote endpoint at request arrival.
    peer: SocketAddr,
    /// original downstream method.
    method: Method,
    /// original target, from which only the path is logged.
    uri: Uri,
    /// negotiated downstream protocol.
    version: Version,
    /// monotonic start of request handling.
    start: Instant,
    /// selected status, absent if request handling is cancelled.
    status: Option<StatusCode>,
}

impl Access {
    /// capture the original request before any routing or header normalization.
    pub fn start<B>(peer: SocketAddr, request: &Request<B>) -> Option<Self> {
        logging::event_enabled!(target: "weiche::access", logging::Level::INFO,
            message, peer, method, path, version, status, duration_us)
        .then(|| Self {
            peer,
            method: request.method().clone(),
            uri: request.uri().clone(),
            version: request.version(),
            start: Instant::now(),
            status: None,
        })
    }

    /// finish the record as soon as the proxy selects its downstream response.
    pub fn responded(mut self, status: StatusCode) {
        self.status = Some(status);
    }
}

/// emit a JSON number or null when the stream ended before a response was selected.
impl Argument for Option<StatusCode> {
    type Encoded<'a> = &'a str;

    fn argument(&self) -> &str {
        self.as_ref().map_or("null", StatusCode::as_str)
    }
}

impl Drop for Access {
    fn drop(&mut self) {
        let duration_us = self.start.elapsed().as_micros() as u64;
        log!(info, target: "weiche::access", "http request",
            peer = self.peer.to_string(),
            method = self.method.as_str(),
            path = self.uri.path(),
            version = format!("{:?}", self.version),
            status = self.status,
            duration_us = duration_us,
        );
    }
}
