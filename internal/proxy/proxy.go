package proxy

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"strconv"
	"sync"
	"time"
)

// backendTimeout bounds both backend connection establishment and the wait
// for backend response headers after the request is fully sent.
const backendTimeout = 5 * time.Second

// Exact generated error bodies required by FR-028.
const (
	badRequestBody     = "Bad Request\n"
	badGatewayBody     = "Bad Gateway\n"
	gatewayTimeoutBody = "Gateway Timeout\n"
)

// Server is the single-backend HTTP/1.1 reverse proxy. Each accepted client
// connection carries exactly one request and one response, then closes.
type Server struct {
	cfg *Config

	outMu sync.Mutex
	out   io.Writer

	httpServer *http.Server
	listener   net.Listener
	done       chan error
}

// NewServer builds an unstarted proxy for cfg that writes activity records
// to out.
func NewServer(cfg *Config, out io.Writer) *Server {
	return &Server{cfg: cfg, out: out, done: make(chan error, 1)}
}

// Start opens the listener, records startup, and serves in the background.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", s.cfg.ListenAddr, err)
	}
	s.listener = ln
	s.httpServer = &http.Server{
		Handler: s.handler(),
		// Stable activity records replace the standard server's raw
		// network error text, which must never reach diagnostics.
		ErrorLog: log.New(io.Discard, "", 0),
	}
	// One request and one response per client connection.
	s.httpServer.SetKeepAlivesEnabled(false)
	s.logf("event=startup role=load-balancer listen=%s backend=%s", ln.Addr(), s.cfg.BackendURL)
	go func() {
		err := s.httpServer.Serve(&validatingListener{Listener: ln, srv: s})
		if err == http.ErrServerClosed {
			err = nil
		}
		s.done <- err
	}()
	return nil
}

// requestIDKey carries the unique request correlation ID in the request
// context so every outcome record can repeat it (FR-013).
type requestIDKey struct{}

func requestIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}

// handler assigns each accepted request a unique ID, writes its redacted
// activity record, and forwards it through the reverse proxy.
func (s *Server) handler() http.Handler {
	rp := s.newReverseProxy()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := newRequestID()
		r = r.WithContext(context.WithValue(r.Context(), requestIDKey{}, id))
		s.logf("%s", formatRequestRecord(id, r))
		defer func() {
			if p := recover(); p != nil {
				// ReverseProxy aborts the handler when the client vanishes
				// during response-body relay (FR-026).
				if p == http.ErrAbortHandler && r.Context().Err() != nil {
					s.logf("event=error id=%s class=client_disconnect", id)
				}
				panic(p)
			}
		}()
		rp.ServeHTTP(w, r)
	})
}

// newReverseProxy forwards requests to the configured backend origin while
// preserving the raw target, host, end-to-end headers, and body.
func (s *Server) newReverseProxy() *httputil.ReverseProxy {
	// One transport is shared by every request; it is concurrency-safe,
	// keeps per-request forwarding state independent, and pools enough idle
	// backend connections for 100 simultaneous clients without churn.
	const maxPooledBackendConns = 128
	transport := &http.Transport{
		DialContext:           (&net.Dialer{Timeout: backendTimeout}).DialContext,
		ResponseHeaderTimeout: backendTimeout,
		MaxIdleConns:          maxPooledBackendConns,
		MaxIdleConnsPerHost:   maxPooledBackendConns,
	}
	return &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.Out.URL.Scheme = s.cfg.BackendURL.Scheme
			pr.Out.URL.Host = s.cfg.BackendURL.Host
			// Preserve the client-supplied authority instead of the backend's.
			pr.Out.Host = pr.In.Host
			// ReverseProxy re-adds "Te: trailers"; the contract removes Te always.
			pr.Out.Header.Del("Te")
			// The proxy neither adds nor strips client forwarding headers.
			for _, name := range []string{"Forwarded", "X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto"} {
				if values, ok := pr.In.Header[name]; ok {
					pr.Out.Header[name] = values
				}
			}
		},
		Transport: transport,
		// Every relayed backend status is recorded with the request ID so
		// interleaved concurrent outcomes stay unambiguous (FR-013).
		ModifyResponse: func(resp *http.Response) error {
			s.logf("event=response id=%s status=%d", requestIDFrom(resp.Request.Context()), resp.StatusCode)
			return nil
		},
		// Forwarding failures stay isolated to their own request: a client
		// that is already gone gets no response, and any other failure gets
		// a classified generated error while unrelated connections continue
		// untouched (FR-014, FR-015, FR-026, FR-028).
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			id := requestIDFrom(r.Context())
			if r.Context().Err() != nil {
				s.logf("event=error id=%s class=client_disconnect", id)
				return
			}
			class, status := classifyForwardingError(err)
			s.logf("event=error id=%s class=%s status=%d", id, class, status)
			writeGeneratedError(w, status)
		},
	}
}

// classifyForwardingError maps a forwarding failure to its stable class and
// generated status: dial failures are backend_connection 502, a timeout
// waiting for response headers is backend_timeout 504, and every other
// failure, including early close and malformed responses, is
// backend_response 502 (FR-014, FR-015).
func classifyForwardingError(err error) (string, int) {
	var opErr *net.OpError
	if errors.As(err, &opErr) && opErr.Op == "dial" {
		return "backend_connection", http.StatusBadGateway
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "backend_timeout", http.StatusGatewayTimeout
	}
	return "backend_response", http.StatusBadGateway
}

// writeGeneratedError sends the exact generated 502 or 504 contract
// response without disclosing internal failure details (FR-028).
func writeGeneratedError(w http.ResponseWriter, status int) {
	body := badGatewayBody
	if status == http.StatusGatewayTimeout {
		body = gatewayTimeoutBody
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(status)
	io.WriteString(w, body)
}

// Addr returns the bound listening address.
func (s *Server) Addr() string {
	return s.listener.Addr().String()
}

// Done reports the serve loop result after shutdown or a serving failure.
func (s *Server) Done() <-chan error {
	return s.done
}

// Shutdown stops accepting connections, drains active requests until ctx
// expires, and records shutdown.
func (s *Server) Shutdown(ctx context.Context) error {
	err := s.httpServer.Shutdown(ctx)
	s.logf("event=shutdown role=load-balancer listen=%s", s.listener.Addr())
	return err
}

func (s *Server) logf(format string, args ...any) {
	s.outMu.Lock()
	defer s.outMu.Unlock()
	fmt.Fprintf(s.out, format+"\n", args...)
}

// validatingListener wraps accepted connections so malformed and incomplete
// requests produce the exact generated 400 contract before the standard
// server parses them (FR-019, FR-028).
type validatingListener struct {
	net.Listener
	srv *Server
}

func (l *validatingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &validatingConn{Conn: conn, srv: l.srv}, nil
}

// validatingConn pre-reads one request's header block on the first server
// read. Valid header blocks are replayed to the standard parser unchanged;
// malformed or incomplete syntax is answered with the exact 400 while a
// pre-request disconnect is only logged, and both make the connection look
// cleanly closed to the server (FR-019, FR-026).
type validatingConn struct {
	net.Conn
	srv      *Server
	checked  bool
	rejected bool
	pending  []byte
}

func (c *validatingConn) Read(p []byte) (int, error) {
	if !c.checked {
		c.checked = true
		c.validate()
	}
	if len(c.pending) > 0 {
		n := copy(p, c.pending)
		c.pending = c.pending[n:]
		return n, nil
	}
	if c.rejected {
		return 0, io.EOF
	}
	return c.Conn.Read(p)
}

func (c *validatingConn) validate() {
	var buf []byte
	tmp := make([]byte, 4096)
	for {
		if end := headerBlockLen(buf); end >= 0 {
			if requestHeadersInvalid(buf[:end]) {
				c.reject()
				return
			}
			c.pending = buf
			return
		}
		if len(buf) >= http.DefaultMaxHeaderBytes {
			// Hand oversized headers to the standard server so distinct
			// protocol-limit statuses are preserved (FR-019).
			c.pending = buf
			return
		}
		n, err := c.Conn.Read(tmp)
		buf = append(buf, tmp[:n]...)
		if err != nil {
			if len(buf) == 0 || err != io.EOF {
				// The client vanished before a request could be parsed.
				c.srv.logf("event=error class=client_disconnect client=%s", c.RemoteAddr())
				c.rejected = true
				return
			}
			// Incomplete syntax while the connection remains writable.
			c.reject()
			return
		}
	}
}

// reject answers with the exact generated 400 contract response and records
// the invalid_request outcome (FR-019, FR-028).
func (c *validatingConn) reject() {
	c.rejected = true
	fmt.Fprintf(c.Conn, "HTTP/1.1 400 Bad Request\r\n"+
		"Content-Type: text/plain; charset=utf-8\r\n"+
		"Content-Length: %d\r\n"+
		"Connection: close\r\n\r\n%s", len(badRequestBody), badRequestBody)
	c.srv.logf("event=error class=invalid_request status=400 client=%s", c.RemoteAddr())
}

// headerBlockLen returns the length of the request header block including
// its terminating empty line, or -1 while the block is still incomplete.
// Both CRLF and bare-LF line endings accepted by the standard parser are
// recognized.
func headerBlockLen(b []byte) int {
	end := -1
	if i := bytes.Index(b, []byte("\n\r\n")); i >= 0 {
		end = i + 3
	}
	if i := bytes.Index(b, []byte("\n\n")); i >= 0 && (end == -1 || i+2 < end) {
		end = i + 2
	}
	return end
}

// requestHeadersInvalid reports whether the standard parser rejects the
// request line or header syntax, or whether a required Host is missing.
func requestHeadersInvalid(block []byte) bool {
	req, err := http.ReadRequest(bufio.NewReader(bytes.NewReader(block)))
	if err != nil {
		return true
	}
	return req.ProtoAtLeast(1, 1) && req.Host == ""
}
