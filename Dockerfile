FROM docker.io/library/rust:1.100-alpine AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY weiche ./weiche
COPY weiche-gtwapi ./weiche-gtwapi
# apply crt-static to the application target, not host proc-macro crates.
RUN rust_target="$(rustc -vV | sed -n 's/^host: //p')" \
    && RUSTFLAGS="-C target-feature=+crt-static" cargo build --locked --release --target "$rust_target" -p weiche \
    && cp "target/$rust_target/release/weiche" /weiche

FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /weiche /usr/local/bin/weiche
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/weiche"]
