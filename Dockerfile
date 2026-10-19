FROM rust:1-trixie AS build
# Overrides rust-toolchain.toml so the image skips rustfmt and clippy.
ENV RUSTUP_TOOLCHAIN=nightly
RUN rustup toolchain install nightly --profile minimal
WORKDIR /build
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build --release --locked \
    && cp target/release/irc-bridge /usr/local/bin/irc-bridge

FROM debian:trixie-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /usr/local/bin/irc-bridge /usr/local/bin/irc-bridge
USER 1000:1000
WORKDIR /bridge
ENTRYPOINT ["irc-bridge"]
