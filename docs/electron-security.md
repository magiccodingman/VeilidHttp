# Electron security boundary

VeilidHttp's Electron client is a browser host for remote web applications. It is not an application plugin host, native package manager, or executable sidecar platform.

## Supported application model

A loaded application may contain ordinary browser content, including:

- HTML, CSS, and JavaScript
- browser WebAssembly, including Blazor WebAssembly
- service workers and PWA assets
- cookies, IndexedDB, Cache Storage, and other origin-scoped browser storage
- `fetch`, WebCrypto, and other normal Chromium APIs

Server-rendered applications and APIs remain on the remote side. Their HTTP traffic is translated through Veilid and rendered or consumed by Chromium in the Electron client.

Each application is assigned a persistent, isolated Chromium session and a loopback origin derived from its RouteBlob fingerprint. Applications do not share cookies, IndexedDB, Cache Storage, service-worker registrations, or permission state merely because they are opened by the same VeilidHttp client.

## Native transport helper

The client includes one fixed VeilidHttp-owned executable named `veilid-http-native`. This process hosts the native Veilid transport and communicates with Electron over a private Unix-domain socket or Windows named pipe using a random launch secret and a bounded binary protocol.

This executable is an internal transport worker. It is not an application-provided sidecar system.

A loaded application cannot:

- choose or replace the executable
- provide command-line arguments
- register another helper process
- execute shell commands
- load native modules or DLLs
- read or write arbitrary filesystem paths
- access the private IPC socket or launch secret
- call the native IPC protocol directly

Packaged builds always launch the bundled transport executable from Electron's resources directory. `VEILID_HTTP_NATIVE_PATH` is honored only during development and is removed from the child process environment.

Any future support for application-provided native companions requires a separate architecture covering provenance, signatures, user consent, capability manifests, process isolation, filesystem and network policy, updates, revocation, and resource limits. It must not be added by extending the current transport protocol informally.

## Loaded-site isolation

Loaded applications run in a `WebContentsView` with:

- context isolation enabled
- Node.js integration disabled
- Chromium sandboxing enabled
- web security enabled
- insecure mixed content disabled
- no preload script
- new-window creation denied
- top-level navigation restricted to the application's own VeilidHttp origin

The trusted Electron shell has a separate preload exposing only route opening, site-data clearing, and site closing. Every privileged IPC handler validates that the sender is the trusted shell's main frame.

## Browser permissions

Until VeilidHttp has a deliberate, user-facing permission model, loaded-site sessions deny privileged browser permission requests and device/display-media access by default.

This does not disable normal origin storage, service workers, WebAssembly, JavaScript, or HTTP requests. It prevents a remote application from silently gaining capabilities such as camera, microphone, geolocation, display capture, USB, serial, HID, Bluetooth, or similarly privileged device access.

A future permission UI should grant narrowly scoped browser permissions to a specific application origin and preserve Chromium-style user control. It must not expose Electron or operating-system APIs directly.

## Loopback transport authentication

Chromium loads each application from a stable loopback origin such as:

```text
http://<route-fingerprint>.veilid.localhost:<port>/
```

Listening on `127.0.0.1` is not treated as authorization. Electron generates a random secret for each launch and injects it into requests at the session network layer. Page JavaScript does not receive the secret and cannot override it. The loopback server rejects requests without the correct secret and removes the authentication header before forwarding HTTP metadata through the native transport.

The loopback server also:

- validates the fingerprint-derived host
- accepts only the supported HTTP methods
- rejects protocol upgrades and WebSockets
- binds only to IPv4 loopback
- strips its private authentication header before Veilid forwarding

This prevents another local browser page or ordinary local process from using the VeilidHttp gateway merely because it can reach the port.

## Explicit non-goals

The current Electron client does not provide:

- arbitrary Electron IPC to loaded sites
- Node.js APIs to loaded sites
- a generic operating-system bridge
- application-provided native sidecars
- executable or shell launching from application content
- unrestricted local socket access
- generic TCP tunneling

Any change that adds one of these capabilities is an architecture and security change, not a routine extension of the current HTTP translation layer.
