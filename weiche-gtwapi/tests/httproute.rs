use serde_json::json;
use weiche_gtwapi::{GATEWAY_API_VERSION, v1::httproutes::HttpRoute};

#[test]
fn roundtrips_v1_7_standard_retry_fields_and_status() {
    // Retry configuration graduated to Standard in Gateway API v1.7.0.
    // Verify these fields survive typed reads instead of being silently dropped.
    let document = json!({
        "apiVersion": "gateway.networking.k8s.io/v1",
        "kind": "HTTPRoute",
        "metadata": {"name": "retry", "namespace": "default"},
        "spec": {
            "parentRefs": [{"name": "gateway"}],
            "rules": [{
                "name": "backend",
                "backendRefs": [{"name": "service", "port": 8080}],
                "retry": {"attempts": 0, "backoff": "100ms", "codes": [500, 503]}
            }]
        },
        "status": {
            "parents": [{
                "parentRef": {"name": "gateway"},
                "controllerName": "example.com/weiche",
                "conditions": [{
                    "type": "Accepted", "status": "True", "reason": "Accepted",
                    "message": "Route accepted", "observedGeneration": 1,
                    "lastTransitionTime": "2026-11-11T00:00:00Z"
                }]
            }]
        }
    });
    let route: HttpRoute = serde_json::from_value(document.clone()).unwrap();
    let retry = route.spec.rules.as_ref().unwrap()[0]
        .retry
        .as_ref()
        .unwrap();
    assert_eq!(retry.attempts, Some(0));
    assert_eq!(retry.backoff.as_deref(), Some("100ms"));
    assert_eq!(retry.codes.as_deref(), Some(&[500, 503][..]));

    let serialized = serde_json::to_value(route).unwrap();
    for field in ["apiVersion", "kind", "spec", "status"] {
        assert_eq!(
            serialized[field], document[field],
            "roundtrip changed {field}"
        );
    }
    assert_eq!(GATEWAY_API_VERSION, "v1.7.0");
}
