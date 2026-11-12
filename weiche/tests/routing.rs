use http::{HeaderMap, Method};
use kube::{
    Resource,
    runtime::{
        reflector::{self, Store},
        watcher::Event,
    },
};
use serde_json::{Value, json};
use std::{net::IpAddr, sync::Arc};
use weiche::{
    config::{self, RoutingTable, Selection},
    controller::GatewayStores,
};

fn store<K: Resource<DynamicType = ()> + Clone>(objects: Vec<K>) -> Store<K> {
    let (reader, mut writer) = reflector::store();
    for object in objects {
        writer.apply_watcher_event(&Event::Apply(object));
    }
    reader
}
fn fixture(route: Value) -> GatewayStores {
    GatewayStores {
        gateway_classes: store(vec![serde_json::from_value(json!({"metadata":{"name":"weiche"}, "spec":{"controllerName":config::CONTROLLER_NAME}})).unwrap()]),
        gateways: store(vec![serde_json::from_value(json!({"metadata":{"namespace":"demo", "name":"gateway"}, "spec":{"gatewayClassName":"weiche", "listeners":[{"name":"http", "port":8080, "protocol":"HTTP", "hostname":"*.example.com"}]}})).unwrap()]),
        http_routes: store(vec![serde_json::from_value(route).unwrap()]),
        services: store(vec![serde_json::from_value(json!({"metadata":{"namespace":"demo","name":"echo"}, "spec":{"ports":[{"name":"http","port":80,"targetPort":9000}]}})).unwrap()]),
        endpoint_slices: store(vec![serde_json::from_value(json!({"metadata":{"namespace":"demo","name":"echo-1","labels":{"kubernetes.io/service-name":"echo"}}, "addressType":"IPv4", "ports":[{"name":"http","port":9000}], "endpoints":[{"addresses":["10.0.0.1"]},{"addresses":["10.0.0.2"],"conditions":{"ready":false}},{"addresses":["10.0.0.3"],"conditions":{"terminating":true}}]})).unwrap()]),
    }
}
fn route() -> Value {
    json!({"metadata":{"namespace":"demo","name":"echo"}, "spec":{"parentRefs":[{"name":"gateway","sectionName":"http"}], "hostnames":["api.example.com"],"rules":[{"matches":[{"path":{"type":"PathPrefix","value":"/api/"}}],"backendRefs":[{"name":"echo","port":80}]}]}})
}
fn selection() -> Selection {
    Selection {
        namespace: "demo".into(),
        gateway: "gateway".into(),
        listener: "http".into(),
        bind_ip: "127.0.0.1".parse::<IpAddr>().unwrap(),
    }
}
fn matches(table: &RoutingTable, host: &str, path: &str) -> bool {
    table
        .find(host, path, &Method::GET, &HeaderMap::new())
        .is_some()
}

#[test]
fn compiles_attachment_and_ready_endpoint_port() {
    let compiled = config::compile(&fixture(route()), &selection());
    assert!(compiled.accepted && compiled.reports[0].accepted && compiled.reports[0].resolved);
    assert_eq!(
        compiled.table.routes[0].endpoints,
        vec!["10.0.0.1:9000".parse().unwrap()]
    );
    assert!(matches(&compiled.table, "api.example.com", "/api"));
    assert!(matches(&compiled.table, "api.example.com", "/api/v1"));
    assert!(!matches(&compiled.table, "api.example.com", "/apix"));
    assert!(!matches(&compiled.table, "other.example.com", "/api"));
}

#[test]
fn denies_wrong_parent_namespace_and_hostname() {
    for (field, value, reason) in [
        ("sectionName", json!("other"), "NoMatchingParent"),
        ("port", json!(80), "NoMatchingParent"),
    ] {
        let mut r = route();
        r["spec"]["parentRefs"][0][field] = value;
        let compiled = config::compile(&fixture(r), &selection());
        assert!(compiled.table.routes.is_empty());
        assert_eq!(compiled.reports[0].reason, reason);
    }
    let mut r = route();
    r["metadata"]["namespace"] = json!("other");
    r["spec"]["parentRefs"][0]["namespace"] = json!("demo");
    let compiled = config::compile(&fixture(r), &selection());
    assert_eq!(compiled.reports[0].reason, "NotAllowedByListeners");
    let mut r = route();
    r["spec"]["hostnames"] = json!(["example.org"]);
    assert_eq!(
        config::compile(&fixture(r), &selection()).reports[0].reason,
        "NoMatchingListenerHostname"
    );
}

#[test]
fn invalid_refs_keep_the_match_and_do_not_fall_through() {
    for (field, value, reason) in [
        ("name", json!("missing"), "BackendNotFound"),
        ("port", json!(81), "BackendNotFound"),
        ("namespace", json!("other"), "RefNotPermitted"),
        ("kind", json!("Secret"), "InvalidKind"),
    ] {
        let mut r = route();
        r["spec"]["rules"][0]["backendRefs"][0][field] = value;
        let compiled = config::compile(&fixture(r), &selection());
        assert!(compiled.reports[0].accepted);
        assert!(!compiled.reports[0].resolved);
        assert_eq!(compiled.reports[0].refs_reason, reason);
        assert!(compiled.table.routes[0].invalid_backend);
    }
}

#[test]
fn rejects_unsupported_filters_regex_and_weighting() {
    let mut variants = vec![];
    let mut r = route();
    r["spec"]["rules"][0]["filters"] =
        json!([{"type":"RequestRedirect","requestRedirect":{"scheme":"https"}}]);
    variants.push(r);
    let mut r = route();
    r["spec"]["rules"][0]["matches"][0]["path"]["type"] = json!("RegularExpression");
    variants.push(r);
    let mut r = route();
    r["spec"]["rules"][0]["backendRefs"][0]["weight"] = json!(0);
    variants.push(r);
    for r in variants {
        let compiled = config::compile(&fixture(r), &selection());
        assert_eq!(compiled.reports[0].reason, "UnsupportedValue");
        assert!(compiled.table.routes.is_empty());
    }
}

#[test]
fn exact_path_method_and_hostname_precedence() {
    let compiled = config::compile(&fixture(route()), &selection());
    let mut prefix = compiled.table.routes[0].clone();
    prefix.hostname = "*.example.com".into();
    let mut exact = prefix.clone();
    exact.exact = true;
    exact.method = Some("POST".into());
    exact.hostname = "api.example.com".into();
    let table = RoutingTable {
        bind: None,
        routes: vec![prefix, exact],
    };
    assert!(
        table
            .find("api.example.com", "/api", &Method::POST, &HeaderMap::new())
            .unwrap()
            .exact
    );
    assert!(
        !table
            .find("api.example.com", "/api", &Method::GET, &HeaderMap::new())
            .unwrap()
            .exact
    );
    assert!(
        !table
            .find(
                "api.example.com",
                "/api/x",
                &Method::POST,
                &HeaderMap::new()
            )
            .unwrap()
            .exact
    );
    assert!(config::hostname_matches(
        "*.example.com",
        "deep.api.example.com"
    ));
    assert!(!config::hostname_matches("*.example.com", "example.com"));
    assert!(!config::hostname_matches(
        "*.example.com",
        "evil-example.com"
    ));
}

#[test]
fn foreign_class_and_deleted_services_cannot_forward() {
    let mut stores = fixture(route());
    let mut class = (*stores.gateway_classes.state()[0]).clone();
    class.spec.controller_name = "other/controller".into();
    stores.gateway_classes = store(vec![class]);
    assert!(config::compile(&stores, &selection()).table.bind.is_none());
    let mut stores = fixture(route());
    stores.services = store(vec![]);
    assert!(config::compile(&stores, &selection()).table.routes[0].invalid_backend);
    let mut stores = fixture(route());
    stores.endpoint_slices = store(vec![]);
    let compiled = config::compile(&stores, &selection());
    assert!(!compiled.table.routes[0].invalid_backend);
    assert!(compiled.table.routes[0].endpoints.is_empty());
    // An existing request can hold its old immutable configuration during publication.
    let old = Arc::new(compiled.table);
    let next = RoutingTable::default();
    assert_eq!(old.routes.len(), 1);
    assert!(next.routes.is_empty());
}
