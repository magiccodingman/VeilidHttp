# RouteBlobs, Site IDs, and Privacy

A VeilidHttp server is addressed through a private RouteBlob, not its public NodeId.
The RouteBlob is binary route material that another Veilid node imports to obtain a
local RouteId.

## Default privacy

- The receiver publishes a private route.
- The client imports and targets that private RouteId.
- The routing context keeps Veilid's default safety routing enabled for sender privacy.
- Reliable/ordered preferences are used by default.

VeilidHttp does not enable the NodeId-target footgun and never silently falls back to
direct NodeId calls.

## Encoding

Descriptors and CLI output use unpadded Base64URL. Browser origins and directory names
use a deterministic identifier:

```text
first 128 bits of BLAKE3(binary RouteBlob)
        ↓ lowercase unpadded Base32
26 characters
```

The full blob remains the actual route material. The fingerprint is an identifier and
integrity/display convenience, not a replacement for the blob.

## Route rotation

A different RouteBlob produces a different V1 site origin. Preserving browser storage
across route rotation will eventually require a signed stable application identity or
pointer layer. V1 does not pretend a route is a permanent application name.

## Sharing

Share the Base64URL RouteBlob or a `.veilidapp` descriptor. Do not share the NodeId as
an endpoint. Treat descriptors as untrusted data; the generic signed Electron client
does not grant native capabilities based on descriptor contents.
