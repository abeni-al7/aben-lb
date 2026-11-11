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
   [-health-interval <duration>] [-health-path </path>] [-keepalive-timeout <duration>]
```

| Flag | Required | Meaning |
|------|----------|---------|
| `-listen` | Yes | TCP address on which client HTTP connections are accepted |
| `-backend` | Yes | Comma-separated list of absolute cleartext HTTP origins |
| `-health-interval` | No | How often each backend is health checked, e.g. `10s`, `500ms` (default `10s`) |
| `-health-path` | No | Path requested with `GET` on each backend for the health check (default `/`) |
| `-keepalive-timeout` | No | How long an idle client connection stays open for another request (default `30s`); `0` closes every connection after one request |

Requests are distributed with **round robin** over the **healthy** backends: each new request
goes to the next healthy backend in the list, wrapping back to the first after the last. With
backends A, B, C and six requests, A gets requests 1 and 4, B gets 2 and 5, C gets 3 and 6. Every
request counts, including a browser's automatic `/favicon.ico` fetch. Each `event=response`
record names the backend that served it.

#### Health checks

A background task sends `GET <backend><health-path>` to every backend once per interval, all
backends at the same time. A `200` response means healthy; any other status, a redirect, a
refused connection, or no answer within 5 seconds (or the interval, if shorter) means unhealthy.

- An unhealthy backend is taken out of the rotation; when its check passes again it is added
  back. Each change is logged once, e.g. `event=health backend=localhost:8082 state=down`.
- Backends start out healthy and the first check runs one interval after startup, so a backend
  that dies is noticed within one interval. Requests sent to it during that window get a `502`.
- When no backend is healthy, `lb` answers `503 Service Unavailable` without contacting any.

Each backend value accepts an `http` scheme and authority only; one invalid entry rejects the
whole list. Credentials, non-root paths,
queries, fragments, and `https` targets are rejected before the listener opens. On invalid
configuration the process prints a concise diagnostic and exits with status `2`; on bind or
serving failure it exits with status `1`. Backend reachability is not checked at startup; see
Health checks above.

On success `lb` writes a startup record to standard output identifying the listening address and
backend targets, then serves until interrupted.

#### Trying it with three Python servers

`server8080/`, `server8081/`, and `server8082/` each hold a small page that names its port:

```sh
python3 -m http.server 8080 --directory server8080 &
python3 -m http.server 8081 --directory server8081 &
python3 -m http.server 8082 --directory server8082 &
go run ./cmd/lb -listen localhost:8000 -health-interval 10s \
  -backend http://localhost:8080,http://localhost:8081,http://localhost:8082
for i in 1 2 3 4 5 6; do curl -s http://localhost:8000/ | grep Hello; done
```

The responses rotate through ports 8080, 8081, and 8082. Stop the 8082 server
(`kill $(lsof -t -i :8082 -sTCP:LISTEN)`); within 10 seconds `lb` logs `state=down` and
requests go only to 8080 and 8081. Start it again and within 10 seconds it rejoins.

To send concurrent requests, put the same line in a file several times and let curl run them in
parallel:

```sh
for i in $(seq 8); do echo 'url = "http://localhost:8000"'; done > urls.txt
curl --parallel --parallel-immediate --parallel-max 3 --config urls.txt
```

In a browser, caching (`304 Not Modified` replies) can hide the rotation; add a changing query
such as `?visit=1`, `?visit=2`.

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
- **Keep-alive** (default): a client can send further requests on the same connection, one at a
  time; it is closed after `-keepalive-timeout` with no new request, or after a request that sends
  `Connection: close`. Round robin still applies per request, so successive requests on one
  connection go to different backends. Every request on a connection is validated, not just the
  first. Connections from `lb` to the backends are pooled and reused the same way.
- With `-keepalive-timeout 0`: exactly one request and one response per client connection; the
  connection then closes and the response advertises `Connection: close`.
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

Pipelining (sending a request before the previous response arrives), protocol upgrades, CONNECT tunneling, TLS termination,
encrypted pass-through, obsolete folded headers, conflicting framing headers, and long-lived
streaming are unsupported. Algorithms other than round robin, retries, and route-based selection
are not implemented: a failed request is never re-sent to another backend.

## Diagnostics, request IDs, and privacy

`lb` writes line-oriented `event=... key=value` records to standard output. Every accepted request
gets a unique request ID; the accepted-request record and its outcome record (backend status or
failure class) carry the same `id=` value, so an operator can correlate a request with its result.

Each outcome record also says which backend handled the request (`backend=host:port`) and how
long it took (`duration_ms=`): the time from forwarding the request until the backend's response
headers arrived, or until forwarding failed.

Privacy rules for the request record:

- Only the root path `/` is logged verbatim. Every non-root path appears as `/[REDACTED]` and a
  present query appears only as `?[REDACTED]` — raw paths and query strings never reach the logs.
- Header names are logged, but values are visible only for `Host`, `User-Agent`, `Accept`,
  `Content-Type`, and `Content-Length`. Every other header value is `[REDACTED]`.
- Request and response bodies are never logged.

Example:

```text
event=startup role=load-balancer listen=127.0.0.1:18080 backends=http://127.0.0.1:18081 health_interval=10s health_path=/ keepalive_timeout=30s
event=request id=9f86d081e2a4c3b1 client=127.0.0.1:53712 method=GET target=/[REDACTED]?[REDACTED] headers="Accept: text/plain; Authorization: [REDACTED]; Host: 127.0.0.1:18080; User-Agent: curl/8.5.0"
event=response id=9f86d081e2a4c3b1 backend=127.0.0.1:18081 status=200 duration_ms=1.549
event=health backend=127.0.0.1:18081 state=down
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
| Every backend is failing its health check | `503 Service Unavailable` | `Service Unavailable\n` | `no_healthy_backend` |

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
outcome under the same request ID with the backend and `duration_ms`. curl closes its connection
after the one request; give it several URLs to see it reuse one connection. Stop
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

`TestMultipleBackendsEndToEnd` starts three `be` processes, an `lb`, and 20 concurrent
keep-alive clients. It checks that 300 requests are split exactly 100 per backend, that clients
reuse their connections, that every response record carries `duration_ms`, and that after one
backend is killed the next 60 requests all succeed, split 30/30 across the other two. The older
single-backend tests run `lb` with `-keepalive-timeout 0` because they check the
one-request-per-connection contract.
