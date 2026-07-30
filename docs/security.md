# Security Model

## Private routes by default

VeilidHttp uses imported private RouteBlobs. It does not advertise or target a public
NodeId. Reliable private routing is the default. The server's node identity is not the
application address and must not be documented as one.

## Generic client

Arbitrary loaded sites are untrusted. Electron renderer requirements:

- `nodeIntegration: false`
- `contextIsolation: true`
- `sandbox: true`
- `webSecurity: true`
- no raw `ipcRenderer`
- no navigation to `file:`
- no arbitrary window creation
- no native filesystem/process/network API

Normal user-mediated uploads and downloads remain available. A site can only receive
the files the user chooses, just like a normal browser.

## Origin and CORS

Each RouteBlob fingerprint is a separate `veilid://` host. Same-origin requests are
transported automatically. Cross-origin Veilid calls remain subject to Chromium CORS.
CORS protects conforming browser users; it is not server authentication and can be
forged by a custom client.

## Resource limits

All metadata, frames, decompressed data, active transactions, in-flight windows, and
retained completions require configured bounds. Zstandard decoding must enforce a
logical output limit to prevent decompression bombs.

## Trusted forwarding headers

The bridge removes spoofed route headers before adding its own values. Downstream
systems must not mistake those values for proof of user identity.
