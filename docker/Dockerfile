# syntax=docker/dockerfile:1.7
ARG RUST_VERSION=1.89.0
ARG DEBIAN_VERSION=13.6

FROM rust:${RUST_VERSION}-trixie AS builder
WORKDIR /src
COPY Cargo.toml rust-toolchain.toml ./
COPY crates ./crates
COPY apps ./apps
RUN cargo build --release -p veilid-http-bridge -p veilid-http-cli

FROM debian:${DEBIAN_VERSION}-slim AS runtime
ARG TARGETARCH
ARG VEILID_RELEASE_CHANNEL=stable
ARG VEILID_VERSION=latest
ENV VEILID_RELEASE_CHANNEL=${VEILID_RELEASE_CHANNEL} \
    VEILID_VERSION=${VEILID_VERSION}

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl gnupg tini procps \
    && curl -fsSL https://packages.veilid.net/gpg/veilid-packages-key.public \
      | gpg --dearmor -o /usr/share/keyrings/veilid-packages-keyring.gpg \
    && echo "deb [arch=${TARGETARCH} signed-by=/usr/share/keyrings/veilid-packages-keyring.gpg] https://packages.veilid.net/apt ${VEILID_RELEASE_CHANNEL} main" \
      > /etc/apt/sources.list.d/veilid.list \
    && apt-get update \
    && if [ "${VEILID_VERSION}" = "latest" ]; then \
         apt-get install -y --no-install-recommends veilid-server veilid-cli; \
       else \
         apt-get install -y --no-install-recommends "veilid-server=${VEILID_VERSION}" "veilid-cli=${VEILID_VERSION}"; \
       fi \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/target/release/veilid-http-bridge /usr/local/bin/
COPY --from=builder /src/target/release/veilid-http-cli /usr/local/bin/
COPY docker/entrypoint.sh docker/healthcheck.sh docker/update-veilid.sh /usr/local/bin/
COPY docker/veilid-server.conf /etc/veilid-server/veilid-server.conf

RUN install -d -o veilid -g veilid \
      /data/veilid/protected-store \
      /data/veilid/table-store \
      /data/veilid/block-store \
      /data/bridge \
    && chmod +x /usr/local/bin/entrypoint.sh /usr/local/bin/healthcheck.sh /usr/local/bin/update-veilid.sh

VOLUME ["/data/veilid", "/data/bridge"]
EXPOSE 5150/tcp 5150/udp
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/entrypoint.sh"]
