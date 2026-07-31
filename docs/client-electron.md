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

## Virtual origin

`veilid://<fingerprint>/path` is registered before Electron is ready as standard,
secure, CORS-enabled, Fetch-capable, stream-capable, code-cache-capable, and
service-worker-capable.

Every same-origin network request reaches the VHTTP handler after the site's own service
worker and normal Chromium cache behavior:

```text
site -> service worker/cache -> veilid:// handler -> binary IPC -> VHTTP -> Veilid
```

The CI browser harness launches real Electron and verifies service-worker interception,
Cache Storage, IndexedDB, persistent sessions, cross-origin CORS, WebAssembly,
secure-context behavior, and a streamed custom-protocol response. Packaged Windows and
Linux builds still require final release validation. The architectural fallback remains
a stable-port `*.veilid.localhost` origin, not fake certificates or disabled web
security.

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
a developer distribute your unchanged signed runtime beside a tiny descriptor. Editing
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
Only `veilid://` origins enter VHTTP.

## Cross-origin Veilid requests

Each imported RouteBlob becomes another virtual origin. A call from one Veilid site to
another is cross-origin and Chromium enforces ordinary CORS. The server's forwarded
`Origin` header is not authentication; applications still use real sessions, tokens, or
cryptographic identity where required.

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
