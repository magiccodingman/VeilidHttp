# Electron Client

## Why Electron

The generic client is a browser runtime, not merely a desktop UI. Electron supplies a
consistent Chromium implementation across Linux, Windows, and macOS with service
workers, Cache Storage, IndexedDB, streams, uploads, downloads, CORS, and explicit
session management.

## Trust split

- Trusted shell: RouteBlob input, navigation, status, and site-data management.
- Untrusted site view: arbitrary remote HTML/JS/WASM with normal browser powers.
- Native sidecar: Veilid and VHTTP engine, reachable only by Electron main.

The untrusted site never receives Node.js, raw Electron IPC, a sidecar handle,
filesystem APIs, process execution, or access to the trusted shell DOM.

## Virtual origin

`veilid://<fingerprint>/path` is registered before Electron is ready as standard,
secure, CORS-enabled, Fetch-capable, stream-capable, code-cache-capable, and
service-worker-capable.

Every same-origin network request reaches the VHTTP handler after the site's own
service worker and normal Chromium cache behavior:

```text
site -> service worker/cache -> veilid:// protocol -> sidecar -> VHTTP -> Veilid
```

A packaged compatibility gate must still verify the complete PWA behavior before the
scheme is considered production-stable. The architectural fallback is a stable-port
`*.veilid.localhost` origin, not fake certificates.

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

No arbitrary filesystem access means the page cannot silently enumerate/read paths.
A user may still select files or a download location just as in a normal browser.

## Site storage

Each route uses `persist:veilid-site-<fingerprint>`. The main process can clear HTTP
cache and origin storage through Electron session APIs. An in-memory partition can be
added later for private browsing without changing the protocol.

## Platform policy

Linux is the primary release target. Windows is secondary after code signing is ready.
macOS source-build instructions are maintained, but no official signed/notarized
binary is planned.
