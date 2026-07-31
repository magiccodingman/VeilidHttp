# syntax=docker/dockerfile:1.7
ARG RUST_VERSION=1.89.0
ARG DEBIAN_VERSION=13.6

FROM rust:${RUST_VERSION}-trixie AS builder
WORKDIR /src
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates
COPY apps ./apps
RUN cargo build --locked --release -p veilid-http-bridge -p veilid-http-cli

FROM debian:${DEBIAN_VERSION}-slim AS runtime
ARG TARGETARCH
ARG VEILID_RELEASE_CHANNEL=stable
ARG VEILID_VERSION=0.5.5
LABEL org.opencontainers.image.source="https://github.com/magiccodingman/VeilidHttp"
LABEL org.opencontainers.image.description="VeilidHttp server bridge and official veilid-server"
ENV VEILID_RELEASE_CHANNEL=${VEILID_RELEASE_CHANNEL} \
    VEILID_VERSION=${VEILID_VERSION} \
    VEILID_EXPECTED_VERSION_PREFIX=${VEILID_VERSION} \
    VEILID_CLIENT_ENDPOINT=127.0.0.1:5959

# Veilid is selected at image-build time. Debian repositories may prepend an epoch and
# append a package revision, so resolve the exact available package whose upstream
# version is VEILID_VERSION, require CLI/server to share that full Debian version, and
# pin both. The running container never mutates itself with apt upgrades.
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
      ca-certificates curl gnupg tini procps netcat-openbsd; \
    curl -fsSL https://packages.veilid.net/gpg/veilid-packages-key.public \
      | gpg --dearmor -o /usr/share/keyrings/veilid-packages-keyring.gpg; \
    echo "deb [arch=${TARGETARCH} signed-by=/usr/share/keyrings/veilid-packages-keyring.gpg] https://packages.veilid.net/apt ${VEILID_RELEASE_CHANNEL} main" \
      > /etc/apt/sources.list.d/veilid.list; \
    apt-get update; \
    server_versions="$(apt-cache madison veilid-server | awk '{ print $3 }')"; \
    cli_versions="$(apt-cache madison veilid-cli | awk '{ print $3 }')"; \
    printf 'Available veilid-server versions:\n%s\n' "${server_versions}"; \
    printf 'Available veilid-cli versions:\n%s\n' "${cli_versions}"; \
    resolved_version=''; \
    for version in ${server_versions}; do \
      upstream_version="${version#*:}"; \
      case "${upstream_version}" in \
        "${VEILID_VERSION}"|"${VEILID_VERSION}"-*|"${VEILID_VERSION}"+*|"${VEILID_VERSION}"~*) \
          resolved_version="${version}"; \
          break; \
          ;; \
      esac; \
    done; \
    if [ -z "${resolved_version}" ]; then \
      echo "No veilid-server package matches upstream version ${VEILID_VERSION}" >&2; \
      exit 1; \
    fi; \
    if ! printf '%s\n' "${cli_versions}" | grep -Fx -- "${resolved_version}" >/dev/null; then \
      echo "veilid-cli does not provide the selected server version ${resolved_version}" >&2; \
      exit 1; \
    fi; \
    echo "Installing Veilid Debian version ${resolved_version}"; \
    apt-get install -y --no-install-recommends \
      "veilid-server=${resolved_version}" \
      "veilid-cli=${resolved_version}"; \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/target/release/veilid-http-bridge /usr/local/bin/
COPY --from=builder /src/target/release/veilid-http-cli /usr/local/bin/
COPY docker/entrypoint.sh docker/healthcheck.sh /usr/local/bin/
COPY docker/veilid-server.conf /etc/veilid-server/veilid-server.conf

RUN install -d -o veilid -g veilid \
      /data/veilid/protected-store \
      /data/veilid/table-store \
      /data/veilid/block-store \
      /data/bridge/route/history \
      /data/bridge/transfers \
      /data/bridge/completed \
      /data/bridge/spool \
    && chmod +x /usr/local/bin/entrypoint.sh /usr/local/bin/healthcheck.sh

VOLUME ["/data/veilid", "/data/bridge"]
EXPOSE 5150/tcp 5150/udp
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/entrypoint.sh"]
