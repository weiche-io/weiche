use bytes::Bytes;
use http::{Request, Response};
use http_body_util::{BodyExt, Full, StreamBody, combinators::BoxBody};
use hyper::body::Frame;
use kube::{Client, client::Body};
use std::{convert::Infallible, time::Duration};
use tokio::sync::oneshot;
use weiche::controller::GatewayController;

#[tokio::test]
async fn all_cache_readers_wake_when_initial_lists_are_empty() {
    let api = tower::service_fn(|request: Request<Body>| async move {
        let is_watch = request
            .uri()
            .query()
            .is_some_and(|q| q.contains("watch=true"));
        let body: BoxBody<Bytes, Infallible> = if is_watch {
            StreamBody::new(futures::stream::pending::<Result<Frame<Bytes>, Infallible>>()).boxed()
        } else {
            Full::new(Bytes::from_static(br#"{"apiVersion":"v1","kind":"List","metadata":{"resourceVersion":"1"},"items":[]}"#)).boxed()
        };
        Ok::<_, Infallible>(Response::new(body))
    });
    let controller = GatewayController::new(Client::new(api, "default"));
    let stores = controller.stores();
    let (stop, stopped) = oneshot::channel();
    let task = tokio::spawn(controller.run(async {
        let _ = stopped.await;
    }));
    tokio::time::timeout(Duration::from_secs(2), async {
        tokio::try_join!(
            stores.gateway_classes.wait_until_ready(),
            stores.gateways.wait_until_ready(),
            stores.http_routes.wait_until_ready(),
            stores.services.wait_until_ready(),
            stores.endpoint_slices.wait_until_ready()
        )
        .unwrap();
    })
    .await
    .expect("cache readiness must wake external readers as well as the watch driver");
    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap();
}
