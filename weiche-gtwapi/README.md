# weiche-gtwapi

Rust bindings for the official [Gateway API v1.7.0 Standard manifest](https://github.com/kubernetes-sigs/gateway-api/releases/tag/v1.7.0).
The crate version follows upstream. Modules `v1` and `v1beta1` cover all served
Standard CRDs. Use with kube 5 and k8s-openapi 0.29; the application selects the
Kubernetes version feature (`v1_37` in Weiche).

```rust
use weiche_gtwapi::v1::httproutes::HttpRoute;
```

Install CRDs from `manifests/standard-install.yaml`, which preserves upstream
validation and admission policies. `manifests/source.json` records the source,
checksum, and generator version.
