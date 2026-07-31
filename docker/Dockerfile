# syntax=docker/dockerfile:1.7
ARG RUST_VERSION=1.89.0
ARG DEBIAN_VERSION=13.6
ARG VEILID_VERSION=0.5.5
ARG VEILID_COMMIT=bbb212de91b492fb9a55b6d5b9084965c05c17cf

FROM rust:${RUST_VERSION}-trixie AS veilid-builder
ARG VEILID_VERSION
ARG VEILID_COMMIT
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
      capnproto \
      git \
      libdbus-1-dev \
      libsqlite3-dev \
      pkg-config; \
    rm -rf /var/lib/apt/lists/*
WORKDIR /veilid
RUN set -eux; \
    git clone --branch "v${VEILID_VERSION}" --depth 1 --recurse-submodules \
      https://gitlab.com/veilid/veilid.git .; \
    test "$(git rev-parse HEAD)" = "${VEILID_COMMIT}"
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/veilid/target \
    set -eux; \
    cargo build --locked --release -p veilid-server -p veilid-cli; \
    install -m 0755 target/release/veilid-server /tmp/veilid-server; \
    install -m 0755 target/release/veilid-cli /tmp/veilid-cli; \
    /tmp/veilid-server --version | grep -F "${VEILID_VERSION}"; \
    /tmp/veilid-cli --version | grep -F "${VEILID_VERSION}"

FROM rust:${RUST_VERSION}-trixie AS builder
WORKDIR /src
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates
COPY apps ./apps
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/src/target \
    set -eux; \
    cargo build --locked --release -p veilid-http-bridge -p veilid-http-cli; \
    install -m 0755 target/release/veilid-http-bridge /tmp/veilid-http-bridge; \
    install -m 0755 target/release/veilid-http-cli /tmp/veilid-http-cli

FROM debian:${DEBIAN_VERSION}-slim AS runtime
ARG VEILID_VERSION
LABEL org.opencontainers.image.source="https://github.com/magiccodingman/VeilidHttp"
LABEL org.opencontainers.image.description="VeilidHttp server bridge and official veilid-server"
ENV VEILID_VERSION=${VEILID_VERSION} \
    VEILID_EXPECTED_VERSION_PREFIX=${VEILID_VERSION} \
    VEILID_CLIENT_ENDPOINT=127.0.0.1:5959

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
      adduser \
      ca-certificates \
      libdbus-1-3 \
      libsqlite3-0 \
      netcat-openbsd \
      procps \
      tini \
      util-linux; \
    addgroup --system veilid; \
    adduser --system --ingroup veilid --home /var/lib/veilid --no-create-home veilid; \
    rm -rf /var/lib/apt/lists/*

COPY --from=veilid-builder /tmp/veilid-server /usr/local/bin/veilid-server
COPY --from=veilid-builder /tmp/veilid-cli /usr/local/bin/veilid-cli
COPY --from=builder /tmp/veilid-http-bridge /usr/local/bin/veilid-http-bridge
COPY --from=builder /tmp/veilid-http-cli /usr/local/bin/veilid-http-cli
COPY docker/entrypoint.sh docker/healthcheck.sh /usr/local/bin/
COPY docker/veilid-server.conf /etc/veilid-server/veilid-server.conf

RUN set -eux; \
    veilid-server --version | grep -F "${VEILID_VERSION}"; \
    veilid-cli --version | grep -F "${VEILID_VERSION}"; \
    install -d -o veilid -g veilid \
      /data/veilid/protected-store \
      /data/veilid/table-store \
      /data/veilid/block-store \
      /data/bridge/route/history \
      /data/bridge/transfers \
      /data/bridge/completed \
      /data/bridge/spool; \
    chmod +x /usr/local/bin/entrypoint.sh /usr/local/bin/healthcheck.sh

VOLUME ["/data/veilid", "/data/bridge"]
EXPOSE 5150/tcp 5150/udp
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/entrypoint.sh"]
