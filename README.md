# aben-lb

An HTTP/1.1 load balancer written in Go with only the standard library. The repository ships two
commands:

- `lb` — the load balancer: accepts client HTTP/1.1 connections and spreads requests across one
  or more configured backend origins using round robin.
- `be` — a deterministic test backend used for development, automated testing, and the end-to-end
  example below.

Requires Go 1.25 or newer.

## Commands

### `lb` — load balancer

```text
lb -listen <host:port> -backend <http://host:port>[,<http://host:port>...]
```

| Flag | Required | Meaning |
|------|----------|---------|
| `-listen` | Yes | TCP address on which client HTTP connections are accepted |
| `-backend` | Yes | Comma-separated list of absolute cleartext HTTP origins |

Requests are distributed with **round robin**: each new request goes to the next backend in the
list, wrapping back to the first after the last. With backends A, B, C and six requests, A gets
requests 1 and 4, B gets 2 and 5, C gets 3 and 6. Every request counts, including a browser's
automatic `/favicon.ico` fetch. Each `event=response` record names the backend that served it.

Each backend value accepts an `http` scheme and authority only; one invalid entry rejects the
whole list. Credentials, non-root paths,
queries, fragments, and `https` targets are rejected before the listener opens. On invalid
configuration the process prints a concise diagnostic and exits with status `2`; on bind or
serving failure it exits with status `1`. Backend reachability is checked per request, not at
startup.

On success `lb` writes a startup record to standard output identifying the listening address and
backend targets, then serves until interrupted.

#### Trying it with two Python servers

`server8080/` and `server8081/` hold a small page that names its port:

```sh
python3 -m http.server 8080 --directory server8080 &
python3 -m http.server 8081 --directory server8081 &
go run ./cmd/lb -listen localhost:8000 -backend http://localhost:8080,http://localhost:8081
for i in 1 2 3 4; do curl -s http://localhost:8000/ | grep Hello; done
```

The responses alternate between port 8080 and 8081. In a browser, caching (`304 Not Modified`
replies) can hide the alternation; add a changing query such as `?visit=1`, `?visit=2`.

### `be` — test backend

```text
be -listen <host:port>
```

| Flag | Required | Meaning |
|------|----------|---------|
| `-listen` | Yes | TCP address on which test HTTP connections are accepted |

For every valid request, `be` logs the client request, returns `200 OK` with the exact body
`Hello From Backend Server`, and logs confirmation that the response was sent. It uses the same
exit-code and shutdown behavior as `lb`.

### Shutdown: exactly five seconds

On `SIGINT` or `SIGTERM`, each command stops accepting new connections, allows active requests
exactly 5 seconds to complete, records a shutdown event, releases its listener, and exits with
status `0`. Requests still running when the drain window ends are terminated.

## HTTP behavior

### Supported

- Cleartext HTTP/1.1 on the client side and toward every backend origin.
- Exactly one request and one response per client connection; the connection then closes and the
  response advertises `Connection: close`.
- Any valid method token, origin-form target, and a body framed by `Content-Length`, valid
  chunked transfer coding, or no body.
- Forwarding preserves the method, raw path and query, original `Host` value, end-to-end header
  names/values/multiplicity, and body bytes unchanged. No `Forwarded` or `X-Forwarded-*` headers
  are added, and failed requests are never retried.
- Backend responses are relayed unchanged: status (including non-success statuses), end-to-end
  headers, and body bytes.
- Hop-by-hop headers (`Connection` and every header it nominates, `Proxy-Connection`,
  `Keep-Alive`, `Proxy-Authenticate`, `Proxy-Authorization`, `Te`, `Trailer`,
  `Transfer-Encoding`, `Upgrade`) are removed case-insensitively in both directions, collecting
  tokens from every `Connection` instance.

### Excluded

Persistent client connections, pipelining, protocol upgrades, CONNECT tunneling, TLS termination,
encrypted pass-through, obsolete folded headers, conflicting framing headers, and long-lived
streaming are unsupported. Algorithms other than round robin, health checks, failover, retries,
and route-based selection are not implemented yet: a down backend still receives its turn and
that request gets a `502`.

## Diagnostics, request IDs, and privacy

`lb` writes line-oriented `event=... key=value` records to standard output. Every accepted request
gets a unique request ID; the accepted-request record and its outcome record (backend status or
failure class) carry the same `id=` value, so an operator can correlate a request with its result.

Privacy rules for the request record:

- Only the root path `/` is logged verbatim. Every non-root path appears as `/[REDACTED]` and a
  present query appears only as `?[REDACTED]` — raw paths and query strings never reach the logs.
- Header names are logged, but values are visible only for `Host`, `User-Agent`, `Accept`,
  `Content-Type`, and `Content-Length`. Every other header value is `[REDACTED]`.
- Request and response bodies are never logged.

Example:

```text
event=startup role=load-balancer listen=127.0.0.1:18080 backend=http://127.0.0.1:18081
event=request id=9f86d081e2a4c3b1 client=127.0.0.1:53712 method=GET target=/[REDACTED]?[REDACTED] headers="Accept: text/plain; Authorization: [REDACTED]; Host: 127.0.0.1:18080; User-Agent: curl/8.5.0"
event=response id=9f86d081e2a4c3b1 status=200
event=shutdown role=load-balancer listen=127.0.0.1:18080
```

## Generated errors

When `lb` itself must answer, the response is exact and deterministic: the listed body, header
`Content-Type: text/plain; charset=utf-8`, the exact `Content-Length`, and connection closure. No
raw network errors, backend addresses, or internal details are ever exposed to clients.

| Condition | Status | Exact body | Logged class |
|-----------|--------|------------|--------------|
| Malformed request line/headers, or incomplete syntax while writable | `400 Bad Request` | `Bad Request\n` | `invalid_request` |
| Backend refused, unreachable, or not connected within 5 seconds | `502 Bad Gateway` | `Bad Gateway\n` | `backend_connection` |
| Backend closed early, sent a malformed response, or another non-timeout forwarding failure | `502 Bad Gateway` | `Bad Gateway\n` | `backend_response` |
| Connected backend sent no response headers for 5 seconds after the full request | `504 Gateway Timeout` | `Gateway Timeout\n` | `backend_timeout` |

Standard protocol-limit responses (such as `431 Request Header Fields Too Large`) are preserved,
not reclassified as `400`. A client that disconnects before completing its request receives no
response; the event is logged as `client_disconnect` and no partial request is forwarded.

## Capacity boundary

At least 100 simultaneous client connections are supported with full success and latency
guarantees. A slow request never serializes an unrelated connection, each response is delivered
only to the connection that originated its request, and a client disconnect or backend failure
never terminates the process or cancels unrelated requests.

The guaranteed envelope ends at 100 simultaneous connections. Above 100 there is no success or
latency target — acceptance follows operating-system resource availability — but the process must
not crash, corrupt request state, or cancel already accepted unrelated work.

## Performance profile

The performance budget is validated on Linux loopback (Go 1.25+, at least 2 logical CPU cores and
512 MiB memory, all processes on one host):

- 100 concurrent one-request connections for a 30-second measured window after a 2-second
  warm-up, at least 1,000 measured requests, no retries.
- Fixed request mix: 70% `GET` without a body, 20% text `POST` with a 1 KiB body, 10% binary
  `POST` with a 64 KiB body.
- At least 99% of attempted measured requests must receive the complete expected backend
  response.
- At least 95% of measured requests must add no more than 100 ms of proxy overhead versus a
  paired direct-backend measurement.
- Before each measured run the backend must pass the same mix directly with ≥ 99.9% success and
  p95 ≤ 50 ms, otherwise the run is invalid rather than failed.
- Five clean-process runs; at least four of five must meet every threshold.

Run it with:

```bash
go test ./tests -run TestPerformanceBudget -count=1
```

## End-to-end example

Terminal 1 — start the test backend:

```bash
go run ./cmd/be -listen 127.0.0.1:18081
```

Terminal 2 — start the load balancer:

```bash
go run ./cmd/lb -listen 127.0.0.1:18080 -backend http://127.0.0.1:18081
```

Terminal 3 — send a request through the proxy:

```bash
curl --http1.1 --include http://127.0.0.1:18080/
```

Expected response:

```text
HTTP/1.1 200 OK
...
Hello From Backend Server
```

`be` logs the request and response confirmation, `lb` logs the accepted request and the `200`
outcome under the same request ID, and the client connection closes after the one response. Stop
both processes with `Ctrl+C`; each drains for at most five seconds, records shutdown, and exits
cleanly.

## Testing

```bash
gofmt -l .
go vet ./...
go test ./...
go test -race ./...
go test ./tests -run TestPerformanceBudget -count=1
```
