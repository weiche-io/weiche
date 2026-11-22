//! request dispatch and conservative retry over the worker's connection pool.
use super::super::http::{RequestBody, ResponseBody};
use super::{
    super::super::ir::Protocol,
    deadline::{Deadlines, TimedBody},
    pool::{ConnectionPool, Endpoint, PooledBody, ResponseLease},
    upgrade::{Tunnel, Upgrade},
};
use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::Full;
use std::{rc::Rc, time::Instant};

/// a response with its streaming budget and optional upgraded transport.
pub struct Exchange {
    /// downstream response with its local payload or leased backend body.
    pub response: Response<ResponseBody>,
    /// dedicated connection after a validated switching response.
    pub tunnel: Option<Tunnel>,
    /// absolute cutoff for streaming and flushing the response.
    pub deadline: Option<Instant>,
}

impl Exchange {
    /// complete a local response without an upstream transport or attempt deadline.
    pub fn local(status: StatusCode, text: &'static str) -> Self {
        let body = Full::new(Bytes::from_static(text.as_bytes()));
        let mut response = Response::new(ResponseBody::Full(body));
        *response.status_mut() = status;
        Self { response, tunnel: None, deadline: None }
    }

    /// use the same attempt cutoff for backend frames and the downstream flush.
    fn upstream(
        response: Response<PooledBody>,
        tunnel: Option<Tunnel>,
        deadline: Option<Instant>,
    ) -> Self {
        Self {
            response: response
                .map(|body| ResponseBody::Upstream(TimedBody::new(body, deadline))),
            tunnel,
            deadline,
        }
    }
}

/// only requests returned untouched by hyper are eligible for another attempt.
enum Attempt {
    /// response headers received from the selected backend.
    Complete(Exchange),
    /// original request was never dispatched.
    Retry(Request<RequestBody>),
}

impl ConnectionPool {
    /// retain the selected endpoint and overall deadline across one safe retry.
    pub async fn send(
        &self,
        endpoint: Rc<Endpoint>,
        mut request: Request<RequestBody>,
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
                Attempt::Complete(exchange) => return Ok(exchange),
                Attempt::Retry(untouched) => request = untouched,
            }
        }
        anyhow::bail!("backend closed before dispatch")
    }

    /// dial a fresh connection for every upgrade, retaining its slot across the switch.
    async fn http1(
        &self,
        endpoint: Rc<Endpoint>,
        request: Request<RequestBody>,
        upgrade: Option<Upgrade>,
        first: bool,
        end: Option<Instant>,
    ) -> anyhow::Result<Attempt> {
        let mut lease = self.acquire(endpoint, first && upgrade.is_none()).await?;
        let request = request.map(|body| TimedBody::new(body, end));
        let mut response = match lease.sender().try_send_request(request).await {
            Ok(response) => response,
            Err(mut error) => {
                if lease.reused
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
            (response.map(|b| PooledBody::new(b, ResponseLease::Http1(lease))), None)
        };
        Ok(Attempt::Complete(Exchange::upstream(response, tunnel, end)))
    }

    /// retire a stale HTTP/2 sender before retrying an undispatched stream.
    async fn http2(
        &self,
        endpoint: &Rc<Endpoint>,
        request: Request<RequestBody>,
        end: Option<Instant>,
    ) -> anyhow::Result<Attempt> {
        let lease = self.acquire_http2(endpoint).await?;
        match lease
            .sender()
            .try_send_request(request.map(|body| lease.upload(body, end)))
            .await
        {
            Ok(response) => Ok(Attempt::Complete(Exchange::upstream(
                response.map(|b| PooledBody::new(b, ResponseLease::Http2(lease))),
                None,
                end,
            ))),
            Err(mut error) => {
                let Some(request) = error.take_message() else {
                    return Err(error.into_error().into());
                };
                endpoint.retire_http2(&lease);
                let (parts, body) = request.into_parts();
                Ok(Attempt::Retry(Request::from_parts(parts, body.into_inner()?)))
            }
        }
    }
}
