//! request dispatch and conservative retry over the worker's connection pool.
use super::{
    cluster::Protocol,
    pool::{ConnectionPool, Endpoint, PooledBody, Upload},
};
use http::{Request, Response};
use hyper::body::Incoming;
use std::rc::Rc;

pub struct UpstreamClient {
    pool: Rc<ConnectionPool>,
}

impl UpstreamClient {
    pub fn new(pool: Rc<ConnectionPool>) -> Self {
        Self { pool }
    }

    pub async fn send(
        &self,
        endpoint: Rc<Endpoint>,
        request: Request<Incoming>,
    ) -> anyhow::Result<Response<PooledBody>> {
        if endpoint.key.protocol == Protocol::Http2 {
            return self.send_http2(endpoint, request).await;
        }
        let mut lease = self.pool.acquire(endpoint.clone(), true).await?;
        let response = match lease.sender().try_send_request(request).await {
            Ok(response) => response,
            Err(mut error) => {
                // retry only an untouched request from a reused connection; never replay a sent POST.
                let request = if lease.reused() {
                    error.take_message()
                } else {
                    None
                };
                let Some(request) = request else {
                    return Err(error.into_error().into());
                };
                drop(lease);
                // keep the selected endpoint generation, including while retrying.
                lease = self.pool.acquire(endpoint, false).await?;
                lease.sender().send_request(request).await?
            }
        };
        Ok(response.map(|body| lease.into_body(body)))
    }

    async fn send_http2(
        &self,
        endpoint: Rc<Endpoint>,
        request: Request<Incoming>,
    ) -> anyhow::Result<Response<PooledBody>> {
        let mut lease = self.pool.acquire_http2(&endpoint).await?;
        let result = lease
            .sender()
            .try_send_request(request.map(|body| lease.upload(body)))
            .await;
        let response = match result {
            Ok(response) => response,
            Err(mut error) => {
                // hyper returns the message only when it never dispatched this stream.
                let Some(request) = error.take_message() else {
                    return Err(error.into_error().into());
                };
                endpoint.retire_http2(&lease);
                let request = request.map(Upload::into_inner);
                drop(lease);
                lease = self.pool.acquire_http2(&endpoint).await?;
                lease
                    .sender()
                    .send_request(request.map(|body| lease.upload(body)))
                    .await?
            }
        };
        Ok(response.map(|body| lease.into_body(body)))
    }
}
