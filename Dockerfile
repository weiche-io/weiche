FROM docker.io/library/rust:1.100-alpine AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY weiche/Cargo.toml ./weiche/
COPY weiche-gtwapi ./weiche-gtwapi
# prebuild workspace dependencies; apply crt-static only to the target, not host proc-macros.
RUN mkdir -p weiche/src \
    && printf 'fn main() {}\n' > weiche/src/main.rs \
    && touch weiche/src/lib.rs \
    && rust_target="$(rustc -vV | sed -n 's/^host: //p')" \
    && RUSTFLAGS="-C target-feature=+crt-static" cargo build --locked --release --target "$rust_target" -p weiche \
    && cargo clean --release --target "$rust_target" -p weiche \
    && rm -rf weiche/src

COPY weiche/src ./weiche/src
RUN rust_target="$(rustc -vV | sed -n 's/^host: //p')" \
    && RUSTFLAGS="-C target-feature=+crt-static" cargo build --locked --release --target "$rust_target" -p weiche \
    && cp "target/$rust_target/release/weiche" /weiche

FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /weiche /usr/local/bin/weiche
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/weiche"]
