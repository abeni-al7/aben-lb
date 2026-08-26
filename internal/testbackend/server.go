// Package testbackend implements the retained deterministic HTTP backend
// used to validate the proxy.
package testbackend

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
)

// ResponseBody is the exact body returned for every valid request.
const ResponseBody = "Hello From Backend Server"

// Server is the deterministic test backend. It listens on a configurable
// address, answers every valid request with 200 and ResponseBody, and writes
// line-oriented activity records to its output.
type Server struct {
	listenAddr string

	outMu sync.Mutex
	out   io.Writer

	httpServer *http.Server
	listener   net.Listener
	done       chan error
}

// New builds an unstarted server that will listen on listenAddr and write
// activity records to out.
func New(listenAddr string, out io.Writer) *Server {
	return &Server{listenAddr: listenAddr, out: out, done: make(chan error, 1)}
}

// Start opens the listener, records startup, and serves in the background.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.listenAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", s.listenAddr, err)
	}
	s.listener = ln
	s.httpServer = &http.Server{Handler: http.HandlerFunc(s.handle)}
	s.logf("event=startup role=test-backend listen=%s", ln.Addr())
	go func() {
		err := s.httpServer.Serve(ln)
		if err == http.ErrServerClosed {
			err = nil
		}
		s.done <- err
	}()
	return nil
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
	s.logf("event=shutdown role=test-backend listen=%s", s.listener.Addr())
	return err
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	s.logf("event=request client=%s method=%s target=%s proto=%s", r.RemoteAddr, r.Method, r.RequestURI, r.Proto)
	// Drain the request body so closing the connection never resets the
	// client with unread bytes still buffered.
	io.Copy(io.Discard, r.Body)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, ResponseBody)
	s.logf("event=response_sent client=%s status=200", r.RemoteAddr)
}

func (s *Server) logf(format string, args ...any) {
	s.outMu.Lock()
	defer s.outMu.Unlock()
	fmt.Fprintf(s.out, format+"\n", args...)
}
