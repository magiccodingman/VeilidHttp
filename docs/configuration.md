# Configuration

## Bridge

| Variable | Default | Purpose |
|---|---|---|
| `VHTTP_UPSTREAM_URL` | required | The one HTTP/HTTPS upstream base URL |
| `VHTTP_DATA_DIR` | `/data/bridge` | Route, journal, transfer, and spool state |
| `VHTTP_IDLE_TIMEOUT` | `5m` | No-progress transaction expiry |
| `VHTTP_OVERALL_TIMEOUT` | `60m` | Maximum transaction lifetime unless policy changes |
| `VHTTP_FRAME_BYTES` | `30720` | Conservative complete Veilid payload target |
| `VHTTP_SEND_WINDOW_FRAMES` | `32` | Maximum unacknowledged frames per direction |
| `VHTTP_FORWARD_ROUTE_HEADER` | `X-Veilid-Route-Fingerprint` | Trusted downstream route header |
| `VHTTP_ADAPTER_MODE` | `remote` | `remote` live target; `validation` explicit dev scaffold |

The client cannot override upstream scheme, host, or port. Configure the bridge to
point at port 80/443/custom, or point it at a reverse proxy.

## Veilid package

| Variable | Default | Purpose |
|---|---|---|
| `VEILID_RELEASE_CHANNEL` | `stable` | Official package channel used during image build |
| `VEILID_VERSION` | `latest` | Exact package version or rolling latest |
| `VEILID_AUTO_UPDATE` | `true` | Upgrade only Veilid packages at startup/periodically |
| `VEILID_UPDATE_INTERVAL_SECONDS` | `3600` | Periodic check interval |

Pin or disable auto-update when deterministic container contents matter more than
receiving upstream Veilid fixes automatically.
