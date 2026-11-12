//! Real TCP tests across a Tokio test backend and the compio data plane.
use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::{BodyExt, Full, StreamBody, combinators::UnsyncBoxBody};
use hyper::{
    body::{Frame, Incoming},
    service::service_fn,
};
use hyper_util::rt::TokioIo;
use std::{
    convert::Infallible,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::watch,
};
use weiche::{
    config::{Route, RoutingTable},
    dataplane::{self, ListenerState},
    routing,
};

type TestBody = UnsyncBoxBody<Bytes, Infallible>;
struct Dropped(Arc<AtomicBool>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

async fn request(address: std::net::SocketAddr, path: &str) -> Response<Incoming> {
    let socket = TcpStream::connect(address).await.unwrap();
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(socket))
        .await
        .unwrap();
    tokio::spawn(async {
        let _ = connection.await;
    });
    sender
        .send_request(
            Request::builder()
                .uri(path)
                .header("Host", "demo.local")
                .body(Full::new(Bytes::new()))
                .unwrap(),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn streams_cancels_and_applies_snapshots_on_real_compio_sockets() {
    let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend_addr = backend.local_addr().unwrap();
    let cancelled = Arc::new(AtomicBool::new(false));
    let cancelled_backend = cancelled.clone();
    let server = tokio::spawn(async move {
        loop {
            let (socket, _) = backend.accept().await.unwrap();
            let cancelled = cancelled_backend.clone();
            tokio::spawn(async move {
                let service = service_fn(move |req: Request<Incoming>| {
                    let cancelled = cancelled.clone();
                    async move {
                        let body: TestBody = if req.uri().path() == "/endless" {
                            let guard = Dropped(cancelled);
                            StreamBody::new(futures::stream::unfold(guard, |guard| async {
                                tokio::time::sleep(Duration::from_millis(100)).await;
                                Some((
                                    Ok::<_, Infallible>(Frame::data(Bytes::from_static(
                                        b"data: alive\n\n",
                                    ))),
                                    guard,
                                ))
                            }))
                            .boxed_unsync()
                        } else if req.uri().path() == "/events" {
                            StreamBody::new(futures::stream::unfold(0, |n| async move {
                                if n == 3 {
                                    return None;
                                }
                                if n > 0 {
                                    tokio::time::sleep(Duration::from_millis(300)).await;
                                }
                                Some((
                                    Ok::<_, Infallible>(Frame::data(Bytes::from(format!(
                                        "data: {n}\n\n"
                                    )))),
                                    n + 1,
                                ))
                            }))
                            .boxed_unsync()
                        } else {
                            Full::new(Bytes::from(req.uri().to_string())).boxed_unsync()
                        };
                        Ok::<_, Infallible>(Response::new(body))
                    }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(socket), service)
                    .await;
            });
        }
    });
    // Bind port zero to avoid reserving a fixed port in the test suite.
    let bind = "127.0.0.1:0".parse().unwrap();
    let table = RoutingTable {
        bind: Some(bind),
        routes: vec![Route {
            hostname: "demo.local".into(),
            path: "/".into(),
            exact: false,
            method: None,
            headers: vec![],
            endpoints: vec![backend_addr],
            invalid_backend: false,
        }],
    };
    let (mut config_tx, config_rx) = routing::channel(table.clone());
    let (state_tx, mut state_rx) = watch::channel(ListenerState::default());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let worker = tokio::task::spawn_blocking(move || {
        compio::runtime::Runtime::new()
            .unwrap()
            .block_on(dataplane::run(config_rx, state_tx, shutdown_rx, 2))
            .unwrap();
    });
    tokio::time::timeout(Duration::from_secs(5), state_rx.changed())
        .await
        .unwrap()
        .unwrap();
    let address = state_rx.borrow().bound.unwrap();
    let response = request(address, "/hello?x=1").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "/hello?x=1"
    );
    let start = Instant::now();
    let response = request(address, "/events").await;
    let mut body = response.into_body();
    assert_eq!(
        body.frame().await.unwrap().unwrap().into_data().unwrap(),
        "data: 0\n\n"
    );
    assert!(start.elapsed() < Duration::from_millis(250));
    assert!(
        body.collect()
            .await
            .unwrap()
            .to_bytes()
            .ends_with(b"data: 2\n\n")
    );
    assert!(start.elapsed() >= Duration::from_millis(600));
    let response = request(address, "/endless").await;
    let mut body = response.into_body();
    body.frame().await.unwrap().unwrap();
    drop(body);
    tokio::time::timeout(Duration::from_secs(3), async {
        while !cancelled.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("downstream cancellation must close the upstream SSE stream");
    let mut invalid = table.clone();
    invalid.routes[0].invalid_backend = true;
    config_tx.publish(invalid);
    assert_eq!(
        request(address, "/").await.status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
    let mut empty = table.clone();
    empty.routes[0].endpoints.clear();
    config_tx.publish(empty);
    assert_eq!(
        request(address, "/").await.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    let mut removed = table;
    removed.routes.clear();
    config_tx.publish(removed);
    assert_eq!(request(address, "/").await.status(), StatusCode::NOT_FOUND);
    shutdown_tx.send_replace(true);
    tokio::time::timeout(Duration::from_secs(7), worker)
        .await
        .unwrap()
        .unwrap();
    server.abort();
}
