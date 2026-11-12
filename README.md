# Weiche

基于 Rust、compio 和 Kubernetes Gateway API 的网关。当前完成第一阶段的
**HTTP/1.1 明文 demo**：Gateway API → Service / EndpointSlice 服务发现 →
compio / io_uring TCP 转发。TLS、HTTP/3、mTLS 和完整 Gateway API 一致性验证属于后续阶段。

控制面使用 kube-rs / Tokio，数据面运行在独立的 compio 线程。控制面每 500 ms
从资源缓存编译不可变路由表，通过 watch 通道发布；数据面在每个新请求读取最新快照。
请求和响应 body 按帧传输，支持 chunked、SSE 与下游 keep-alive。
下游取消会关闭对应上游连接，SIGINT / SIGTERM 停止接入并最多排空约 6 秒。

## 本地运行 demo

需要 Linux（启用 io_uring）、rootless Podman、kind、kubectl、Python 3，以及 Rust 1.92+。
仓库提供官方 Gateway API v1.7.0 Standard CRD。已在 kind v0.31.0 / Kubernetes
v1.35.0 上验证；`k8s-openapi` 的 `v1_37` 是编译期类型版本，并不要求服务端为 1.37。

在 NixOS 上可使用：

```sh
nix shell nixpkgs#cargo nixpkgs#rustc nixpkgs#gcc nixpkgs#binutils nixpkgs#kind nixpkgs#kubectl nixpkgs#python3 nixpkgs#ripgrep
scripts/demo.sh
```

脚本创建专用的 `kind-weiche-demo` 集群，将 kubeconfig 写到
`target/demo/kubeconfig`，构建并导入本地镜像，安装 CRD、RBAC、网关和两个 echo
Service，然后验证请求转发、双端点轮询、SSE、配置变更、错误状态及端点恢复。
**所有 kubectl 操作明确指定该本地集群，不使用当前 kubeconfig context。**
脚本不推送镜像、不执行 git push；测试后保留集群，端口转发进程自动退出。
重复执行脚本会重新部署并测试。普通 Linux 构建可先使用 `podman build -t
localhost/weiche:demo .` 和 `podman build -f demo/Dockerfile.backend -t
localhost/weiche-echo:demo demo`，再运行 `SKIP_BUILD=1 scripts/demo.sh`。
Nix 构建的快速打包由 `scripts/build-local-image.sh` 完成，echo 镜像通过
`scripts/build-local-backend.sh` 打包本地 Python 标准库，无需额外下载后端镜像。

访问保留的 demo：

```sh
kubectl --kubeconfig "$PWD/target/demo/kubeconfig" --context kind-weiche-demo \
  -n weiche-demo port-forward service/weiche 18080:8080
# 另一个终端
curl --noproxy '*' -H 'Host: echo.demo.local' 'http://127.0.0.1:18080/api/hello?x=1'
curl --noproxy '*' -H 'Host: echo.demo.local' http://127.0.0.1:18080/api/special
curl --noproxy '*' -N -H 'Host: echo.demo.local' http://127.0.0.1:18080/api/events
```

`/api` 前缀指向有两个 Pod 的 `echo-a`；精确路径 `/api/special` 指向 `echo-b`。
前缀按路径段匹配，`/apix` 返回 404。后端保留原始 Host、路径、查询和请求体。

如节点镜像仓库网络不通，可以先从可访问的镜像源拉取 kind 节点镜像，
重新 tag 为 `docker.io/kindest/node:v1.35.0`。`KIND_NODE_IMAGE` 可覆盖首次
创建集群的节点镜像，`DEMO_PORT` 可覆盖本地测试端口（默认 18080）。

删除本地集群：

```sh
KIND_EXPERIMENTAL_PROVIDER=podman kind delete cluster --name weiche-demo
```

## 当前支持范围

- 显式选择一个 Gateway 和其中唯一的 HTTP listener。类的 `controllerName` 必须为
  `weiche.adamantye.cc/gateway-controller`。监听端口来自 Gateway，地址来自 `WEICHE_BIND_IP`。
- HTTPRoute `parentRefs` 的 group、kind、namespace、sectionName、port 校验；
  listener `allowedRoutes` 支持 Same（默认）和 All、HTTPRoute kind。
- listener / route hostname 交集、精确及通配域名、Exact / PathPrefix 路径、方法和精确请求头匹配。
  优先域名精确度、精确路径、前缀长度、方法、请求头数量；相同时按路由创建时间、命名空间/名称、规则顺序。
- 每条规则一个同命名空间的普通 HTTP Service 后端，按 Service port 名称关联
  EndpointSlice 端口，剔除未 ready / terminating / 未 serving 端点，轮询剩余端点。
- 监听 GatewayClass、Gateway、HTTPRoute、Service 和 EndpointSlice 的新增、更新、删除与断线重连。
  为选中的类、网关、路由父引用写入状态；Programmed 只有实际 bind 成功后才为 True。

暂不支持 Namespace selector、跨命名空间后端 / ReferenceGrant、多个 listener、
权重分流、filters、正则/查询匹配、重试/可配置超时、TLS、HTTP/2、HTTP/3、CONNECT 和 Upgrade。
不支持的路由规则设为 `Accepted=False`，不会按简化语义转发。跨命名空间后端设为
`ResolvedRefs=False / RefNotPermitted`。这里没有宣称 Gateway API conformance。

HTTP 错误约定：未匹配 404；无效后端引用 500；没有 ready 端点 503；后端连接/协议失败
502；从连接到收到响应头超过 10 秒为 504（包含上传请求体时间）。响应 body 没有总时限，
可持续流式传输。每请求建立上游连接，尚无连接池、上游重试、生产级限流或指标；
下游连接数上限 1024、请求头上限 100 / 缓冲上限 32 KiB。

本地 Pod 使用 `seccompProfile: Unconfined`，以允许 io_uring 系统调用；容器以非 root
运行并丢弃所有 capabilities。这份 manifest 面向本地 demo，后续应收敛为专用 seccomp profile。

## 配置与开发

`WEICHE_NAMESPACE` 默认 `weiche-demo`，`WEICHE_GATEWAY` 默认 `weiche`，
`WEICHE_LISTENER` 默认 `http`，`WEICHE_BIND_IP` 默认 `0.0.0.0`。
本地运行使用指定的 kubeconfig，Pod 内使用 ServiceAccount：

```sh
KUBECONFIG="$PWD/target/demo/kubeconfig" RUST_LOG=info cargo run -p weiche
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

本地进程需能直接访问后端 Pod IP，通常建议运行在上述 kind 部署中。
RBAC 参见 `demo/gateway.yaml`。库的 `controller::GatewayController` 提供可克隆的缓存 reader，
需同时驱动 `run()`，并等待各 store 的 `wait_until_ready()`；`gateway::run()` 连接控制面、
数据面及状态同步。各资源缓存最终一致，当前编译会扫描缓存，尚未做增量索引优化。

`weiche-gtwapi` 保留官方 Gateway API v1.7.0 Standard 生成绑定，参见
[绑定文档](weiche-gtwapi/README.md)。
