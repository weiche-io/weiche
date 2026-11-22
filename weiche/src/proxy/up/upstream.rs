//! request dispatch and conservative retry over the worker's connection pool.
use super::super::http::Body;
use super::{
    super::super::ir::Protocol,
    deadline::{Deadlines, ResponseDeadline, TimedBody},
    pool::{ConnectionPool, Endpoint, PooledBody},
    upgrade::{Tunnel, Upgrade},
};
use http::{Request, Response, StatusCode};
use std::{rc::Rc, time::Instant};

/// a response with its streaming budget and optional upgraded transport.
pub struct Exchange {
    /// backend response with a lease for the lifetime of its body.
    pub response: Response<TimedBody<PooledBody>>,
    /// dedicated connection after a validated switching response.
    pub tunnel: Option<Tunnel>,
}

/// only requests returned untouched by hyper are eligible for another attempt.
enum Attempt {
    /// response headers received from the selected backend.
    Complete(Exchange),
    /// original request was never dispatched.
    Retry(Request<Body>),
}

/// dispatch requests using the worker's bounded pool.
pub struct UpstreamClient {
    /// worker-local TCP capacity and endpoint generations.
    pool: Rc<ConnectionPool>,
}

impl UpstreamClient {
    /// use the same pool for ordinary traffic and dedicated tunnels.
    pub fn new(pool: Rc<ConnectionPool>) -> Self {
        Self { pool }
    }

    /// retain the selected endpoint and overall deadline across one safe retry.
    pub async fn send(
        &self,
        endpoint: Rc<Endpoint>,
        mut request: Request<Body>,
        mut upgrade: Option<Upgrade>,
        budget: Deadlines,
    ) -> anyhow::Result<Exchange> {
        for first in [true, false] {
            let end = budget.attempt();
            let attempt = async {
                if endpoint.key.protocol == Protocol::Http2 {
                    self.http2(&endpoint, request, end).await
                } else {
                    self.http1(endpoint.clone(), request, upgrade.take(), first, end)
                        .await
                }
            };
            match budget.dispatch(end, attempt).await? {
                Attempt::Complete(mut exchange) => {
                    if let Some(end) = end {
                        exchange.response.extensions_mut().insert(ResponseDeadline(end));
                    }
                    return Ok(exchange);
                }
                Attempt::Retry(untouched) => request = untouched,
            }
        }
        anyhow::bail!("backend closed before dispatch")
    }

    /// dial a fresh connection for every upgrade, retaining its slot across the switch.
    async fn http1(
        &self,
        endpoint: Rc<Endpoint>,
        request: Request<Body>,
        upgrade: Option<Upgrade>,
        first: bool,
        end: Option<Instant>,
    ) -> anyhow::Result<Attempt> {
        let mut lease = self.pool.acquire(endpoint, first && upgrade.is_none()).await?;
        let request = request.map(|body| TimedBody::new(body, end));
        let mut response = match lease.sender().try_send_request(request).await {
            Ok(response) => response,
            Err(mut error) => {
                if lease.reused()
                    && let Some(request) = error.take_message()
                {
                    let (parts, body) = request.into_parts();
                    return Ok(Attempt::Retry(Request::from_parts(
                        parts,
                        body.into_inner()?,
                    )));
                }
                return Err(error.into_error().into());
            }
        };
        let (response, tunnel) = if response.status() == StatusCode::SWITCHING_PROTOCOLS {
            let offer =
                upgrade.ok_or_else(|| anyhow::anyhow!("unexpected backend upgrade"))?;
            let tunnel = offer.accept(&mut response, lease)?;
            (response.map(PooledBody::upgraded), Some(tunnel))
        } else {
            (response.map(|b| lease.into_body(b)), None)
        };
        Ok(Attempt::Complete(Exchange {
            response: response.map(|b| TimedBody::new(b, end)),
            tunnel,
        }))
    }

    /// retire a stale HTTP/2 sender before retrying an undispatched stream.
    async fn http2(
        &self,
        endpoint: &Rc<Endpoint>,
        request: Request<Body>,
        end: Option<Instant>,
    ) -> anyhow::Result<Attempt> {
        let lease = self.pool.acquire_http2(endpoint).await?;
        match lease
            .sender()
            .try_send_request(request.map(|body| TimedBody::new(lease.upload(body), end)))
            .await
        {
            Ok(response) => Ok(Attempt::Complete(Exchange {
                response: response.map(|body| TimedBody::new(lease.into_body(body), end)),
                tunnel: None,
            })),
            Err(mut error) => {
                let Some(request) = error.take_message() else {
                    return Err(error.into_error().into());
                };
                endpoint.retire_http2(&lease);
                let (parts, body) = request.into_parts();
                Ok(Attempt::Retry(Request::from_parts(
                    parts,
                    body.into_inner()?.into_inner(),
                )))
            }
        }
    }
}
