# Configuration

## Bridge

| Variable | Default | Purpose |
|---|---:|---|
| `VHTTP_UPSTREAM_URL` | required | The one HTTP/HTTPS upstream base URL |
| `VHTTP_DATA_DIR` | `/data/bridge` | Route, completion, diagnostic transfer, and reserved spool state |
| `VHTTP_IDLE_TIMEOUT` | `5m` | Reserved no-progress expiry policy |
| `VHTTP_OVERALL_TIMEOUT` | `60m` | Maximum upstream/transaction lifetime |
| `VHTTP_REMOTE_TIMEOUT` | `2m` | `veilid-server` remote API request timeout |
| `VHTTP_FRAME_BYTES` | `30720` | Conservative complete VHTTP frame target below Veilid's 32 KiB ceiling |
| `VHTTP_SEND_WINDOW_FRAMES` | `32` | Maximum retained unacknowledged frames per direction; must be `1..=64` |
| `VHTTP_MAX_PENDING_BYTES` | `8388608` | Per-direction total pending plus in-flight retransmission bytes |
| `VHTTP_MAX_OUT_OF_ORDER_BYTES` | `4194304` | Per-direction compressed bytes retained while sequence gaps exist |
| `VHTTP_MAX_REQUEST_BYTES` | `0` | Logical request body limit; zero means no VHTTP limit |
| `VHTTP_MAX_RESPONSE_BYTES` | `0` | Logical response body limit; zero means no VHTTP limit |
| `VHTTP_MAX_ATOMIC_BODY_BYTES` | `8388608` | Maximum body collected by the optional single-AppCall fast path |
| `VHTTP_COMPLETED_RETENTION` | `15m` | Duration of duplicate-suppression records |
| `VHTTP_COMPLETED_RESPONSE_BYTES` | `1048576` | Largest encoded atomic reply retained for replay; larger replies become tombstones |
| `VHTTP_COMPLETED_MAX_ENTRIES` | `4096` | Maximum completed records retained in memory/on disk |
| `VHTTP_FORWARD_ROUTE_HEADER` | `X-Veilid-Route-Fingerprint` | Trusted downstream route metadata header |
| `VHTTP_ADAPTER_MODE` | `remote` | Normal live mode; `validation` is an explicit configuration-only supervisor |

The process also limits simultaneous upstream executions. The default development-alpha
bound is 128 transactions. The Electron sidecar separately defaults to 64 active browser
requests through `VHTTP_CLIENT_MAX_ACTIVE_REQUESTS`.

The complete configured retransmission window must fit inside
`VHTTP_MAX_PENDING_BYTES`:

```text
VHTTP_FRAME_BYTES × VHTTP_SEND_WINDOW_FRAMES <= VHTTP_MAX_PENDING_BYTES
```

The shared stream engine enforces this invariant for every adapter. Large object size is
not part of the memory equation; active concurrency, windows, compression buffers, and
out-of-order limits are.

The client cannot override upstream scheme, host, or port. Configure the bridge to point
at port 80, 443, a custom port, or an external reverse proxy.

## Veilid server and remote adapter

| Variable | Default | Purpose |
|---|---:|---|
| `VEILID_CLIENT_ENDPOINT` | `127.0.0.1:5959` | Internal client API; never published to the Docker host |
| `VEILID_EXPECTED_VERSION_PREFIX` | `0.5.5` | Refuse a server whose JSON schema version does not match the bridge adapter |
| `VEILID_RELEASE_CHANNEL` | `stable` | Official Debian package channel used at image build time |
| `VEILID_VERSION` | `0.5.5` | Exact server/CLI package version installed into the image |

The bridge remote API dependency and official server package are pinned together. A
running container does not update itself. Build and test a new image to upgrade Veilid.

## Electron sidecar

Electron supplies these values itself for every launch:

| Variable | Purpose |
|---|---|
| `VHTTP_CLIENT_DATA_DIR` | Private native Veilid, imported route, and return-route state |
| `VHTTP_IPC_PATH` | Random private Unix socket or Windows named pipe |
| `VHTTP_IPC_SECRET` | Random one-launch parent authentication secret |
| `VHTTP_CLIENT_MAX_ACTIVE_REQUESTS` | Process-wide browser request limit, default `64` |

A developer may override `VEILID_HTTP_NATIVE_PATH` in the Electron main process to use a
specific debug/release sidecar binary.
