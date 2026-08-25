package proxy

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"sync"
	"time"
)

// backendTimeout bounds both backend connection establishment and the wait
// for backend response headers after the request is fully sent.
const backendTimeout = 5 * time.Second

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
	s.httpServer = &http.Server{Handler: s.newReverseProxy()}
	// One request and one response per client connection.
	s.httpServer.SetKeepAlivesEnabled(false)
	s.logf("event=startup role=load-balancer listen=%s backend=%s", ln.Addr(), s.cfg.BackendURL)
	go func() {
		err := s.httpServer.Serve(ln)
		if err == http.ErrServerClosed {
			err = nil
		}
		s.done <- err
	}()
	return nil
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
		// Forwarding failures stay isolated to their own request: a client
		// that is already gone gets no response, and any other failure gets
		// a generated 502 while unrelated connections continue untouched.
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			if r.Context().Err() != nil {
				return
			}
			w.WriteHeader(http.StatusBadGateway)
		},
	}
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
