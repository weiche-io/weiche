//! request dispatch and conservative retry over the worker's connection pool.
use super::pool::{ConnectionPool, Endpoint, PooledBody};
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
}
