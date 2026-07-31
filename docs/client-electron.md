# Electron Client

## Why Electron

The generic client is a browser runtime, not merely a desktop UI. Electron supplies a
consistent Chromium implementation across Linux, Windows, and macOS with service
workers, Cache Storage, IndexedDB, streams, uploads, downloads, CORS, and explicit
session management.

## Trust split

- **Trusted shell:** RouteBlob input, navigation, status, and site-data management.
- **Untrusted site view:** arbitrary remote HTML/JS/WASM with normal browser powers.
- **Native sidecar:** embedded Veilid node and VHTTP engine, reachable only by Electron
  main through an authenticated private socket/pipe.
- **Loopback origin server:** Electron-main-owned HTTP listener bound only to
  `127.0.0.1`, translating site requests into streamed sidecar IPC.

The untrusted site never receives Node.js, raw Electron IPC, a sidecar handle,
filesystem APIs, process execution, environment variables, or access to the trusted
shell DOM.

Site renderer settings remain:

```text
nodeIntegration: false
contextIsolation: true
sandbox: true
webSecurity: true
allowRunningInsecureContent: false
```

## Browser origin

Each imported RouteBlob is exposed to Chromium as a stable loopback origin:

```text
http://<fingerprint>.veilid.localhost:<stable-port>/
```

The listener binds only to `127.0.0.1` and rejects Host headers that do not exactly match
a valid 26-character site fingerprint plus the configured port. Different fingerprints
therefore remain different browser origins while all overlay traffic still passes
through the trusted main process and native sidecar.

Every same-origin request reaches the VHTTP handler after the site's own service worker
and normal Chromium cache behavior:

```text
site -> service worker/cache -> loopback origin -> binary IPC -> VHTTP -> Veilid
```

A direct `veilid://` custom scheme was prototyped, but Chromium Cache Storage rejects
custom-scheme requests even when the scheme is registered as standard, secure, Fetch-
capable, and service-worker-capable. The loopback-origin model is therefore the actual
V1 implementation, not a degraded test path. Localhost is treated as a trustworthy
browser context, so service workers, Cache Storage, IndexedDB, WebAssembly, streams,
and ordinary CORS remain available without fake certificates or disabled web security.

The CI browser harness launches real Electron and verifies service-worker interception,
Cache Storage, IndexedDB, persistent sessions, cross-origin CORS, WebAssembly,
secure-context behavior, and a streamed response. Packaged Windows and Linux builds
still require final release validation.

## Streaming IPC

Electron and the Rust sidecar use framed MessagePack metadata plus raw payload bytes on:

- A private mode-0600 Unix-domain socket on Linux/macOS.
- A random Windows named pipe.

The main process generates a one-launch authentication secret and passes it only through
the child environment. HTTP request bodies use `StreamData`/`StreamEnd` frames. HTTP
response headers arrive once, followed by raw response stream frames. AbortSignal and
ReadableStream cancellation send an explicit cancel frame.

Both directions honor bounded channel/socket backpressure. There is no Base64 body
encoding and no complete response-body accumulation in Electron.

## Launching a route

```bash
veilid-http --route-base64 '<blob>'
veilid-http --route-file ./server.blob --path /admin
veilid-http ./example.veilidapp
```

If `app.veilidapp` exists next to the executable, it is loaded automatically. This lets
a developer distribute the unchanged signed runtime beside a tiny descriptor. Editing
files inside a signed executable would invalidate its signature; the sibling descriptor
does not.

Descriptor:

```json
{
  "schema": "org.veilidhttp.app/v1",
  "name": "Example Site",
  "routeBlob": "UNPADDED_BASE64URL",
  "startPath": "/"
}
```

Descriptors are untrusted and cannot request native capabilities.

## Browser abilities

Sites keep ordinary Chromium functionality: WebAssembly, workers, service workers,
IndexedDB, cookies, Cache Storage, file inputs, drag/drop, downloads, Blob URLs,
public HTTPS requests, CSP, CORS, and browser-managed persistence.

No arbitrary filesystem access means the page cannot silently enumerate/read paths. A
user may still select files or a download destination just as in a normal browser.
Ordinary public HTTPS calls stay on Chromium's network stack under normal browser rules.
Only requests to validated `*.veilid.localhost` site origins enter VHTTP.

## Cross-origin Veilid requests

Each imported RouteBlob becomes another loopback origin. A call from one Veilid site to
another is cross-origin and Chromium enforces ordinary CORS. The bridge can still expose
an overlay identity such as `veilid://<fingerprint>` in trusted forwarding metadata, but
that value is not the renderer's literal URL and is not authentication. Applications
still use real sessions, tokens, or cryptographic identity where required.

## Site storage

Each route uses `persist:veilid-site-<fingerprint>`. The main process can clear HTTP
cache and origin storage through Electron session APIs. Different site fingerprints do
not share service workers, cookies, IndexedDB, or Cache Storage. An in-memory partition
can be added later for private browsing without changing VHTTP.

A rotated RouteBlob currently produces a different fingerprint/origin. Stable signed
application identity across route rotation remains a future descriptor layer.

## Platform policy

Linux is the primary release target. Windows is secondary after code signing is ready.
macOS source-build instructions are maintained, but no official signed/notarized binary
is planned.
