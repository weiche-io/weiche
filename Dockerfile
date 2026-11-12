FROM docker.io/library/rust:1.95-slim-trixie AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY weiche ./weiche
COPY weiche-gtwapi ./weiche-gtwapi
RUN cargo build --locked --release -p weiche

FROM docker.io/library/debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/weiche /usr/local/bin/weiche
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/weiche"]
