# Reverse Proxy Integration

HAProxy, NGINX, Caddy, and Traefik are first-class neighbors, not embedded features.

Point the bridge's single `VHTTP_UPSTREAM_URL` at the proxy. The bridge forwards a
normal request and overwrites/adds:

```http
X-Veilid-Route-Fingerprint: <server-route-fingerprint>
X-Veilid-Origin: veilid://<server-route-fingerprint>
```

These values are useful for routing, logging, and policy. They identify the VeilidHttp
route that received the transaction; they do **not** prove the end user's identity and
must not replace authentication.

## Non-loopback upstreams

VeilidHttp intentionally does not invent an upstream secret/key system. When the
configured upstream is not local to the container/host, terminate that boundary with
an established tool:

- Private Docker network.
- NGINX/HAProxy/Caddy with TLS or mTLS.
- VPN/private network.
- Existing application authentication.

That keeps VeilidHttp a translator rather than a second reverse-proxy ecosystem.

## HAProxy example

```haproxy
frontend vhttp_internal
  bind :8080
  http-request deny unless { src 127.0.0.1 }
  default_backend application

backend application
  server app app:5000 check
```

## NGINX example

```nginx
server {
  listen 127.0.0.1:8080;

  location / {
    proxy_pass http://application:5000;
    proxy_http_version 1.1;
    proxy_request_buffering off;
    proxy_buffering off;
    proxy_set_header X-Veilid-Route-Fingerprint $http_x_veilid_route_fingerprint;
    proxy_set_header X-Veilid-Origin $http_x_veilid_origin;
  }
}
```

Disabling proxy buffering in the streaming path prevents the neighbor proxy from
undoing VHTTP's incremental large-object behavior.
