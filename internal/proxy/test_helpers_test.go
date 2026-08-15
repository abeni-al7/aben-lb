package proxy

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// newEphemeralListener opens a loopback TCP listener on an OS-assigned port
// and closes it when the test finishes.
func newEphemeralListener(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("open ephemeral listener: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	return ln
}

// rawHTTPExchange dials addr, writes rawRequest exactly as given, and returns
// every byte the server sends until it closes the connection.
func rawHTTPExchange(t *testing.T, addr, rawRequest string) string {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatalf("set connection deadline: %v", err)
	}
	if _, err := io.WriteString(conn, rawRequest); err != nil {
		t.Fatalf("write raw request: %v", err)
	}
	response, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("read raw response: %v", err)
	}
	return string(response)
}

// startDelayedBackend starts a backend on an ephemeral port that waits for
// delay (or client cancellation) before returning 200 OK with body.
func startDelayedBackend(t *testing.T, delay time.Duration, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(delay):
		case <-r.Context().Done():
			return
		}
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// startSilentBackend starts a backend on an ephemeral port that accepts
// connections and reads request bytes but never writes a response. Cleanup
// closes the listener and every accepted connection and waits for all
// goroutines to exit.
func startSilentBackend(t *testing.T) net.Listener {
	t.Helper()
	ln := newEphemeralListener(t)

	var (
		mu    sync.Mutex
		conns []net.Conn
		wg    sync.WaitGroup
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()
			wg.Add(1)
			go func(c net.Conn) {
				defer wg.Done()
				// Consume request bytes until the connection closes.
				io.Copy(io.Discard, c)
			}(conn)
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		mu.Lock()
		for _, c := range conns {
			c.Close()
		}
		mu.Unlock()
		wg.Wait()
	})
	return ln
}

// TestFixturesCleanUp exercises every fixture so Phase 2 proves they compile,
// behave deterministically, and release their resources via test cleanup.
func TestFixturesCleanUp(t *testing.T) {
	t.Run("ephemeral listener assigns a port", func(t *testing.T) {
		ln := newEphemeralListener(t)
		if port := ln.Addr().(*net.TCPAddr).Port; port == 0 {
			t.Fatalf("expected OS-assigned port, got %d", port)
		}
	})

	t.Run("raw HTTP exchange returns full server output", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, "fixture-ok")
		}))
		t.Cleanup(srv.Close)
		addr := srv.Listener.Addr().String()
		resp := rawHTTPExchange(t, addr, "GET / HTTP/1.1\r\nHost: "+addr+"\r\nConnection: close\r\n\r\n")
		header, body, found := strings.Cut(resp, "\r\n\r\n")
		if !found || !strings.HasPrefix(header, "HTTP/1.1 200") || !strings.Contains(body, "fixture-ok") {
			t.Fatalf("raw exchange returned unexpected response:\n%s", resp)
		}
	})

	t.Run("delayed backend waits before responding", func(t *testing.T) {
		const delay = 100 * time.Millisecond
		srv := startDelayedBackend(t, delay, "delayed-ok")
		start := time.Now()
		resp, err := http.Get(srv.URL)
		if err != nil {
			t.Fatalf("request delayed backend: %v", err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatalf("read delayed backend body: %v", err)
		}
		if elapsed := time.Since(start); elapsed < delay {
			t.Fatalf("response arrived after %v, before the %v delay", elapsed, delay)
		}
		if string(body) != "delayed-ok" {
			t.Fatalf("delayed backend body = %q, want %q", body, "delayed-ok")
		}
	})

	t.Run("silent backend accepts but never responds", func(t *testing.T) {
		ln := startSilentBackend(t)
		conn, err := net.DialTimeout("tcp", ln.Addr().String(), 5*time.Second)
		if err != nil {
			t.Fatalf("dial silent backend: %v", err)
		}
		defer conn.Close()
		if _, err := io.WriteString(conn, "GET / HTTP/1.1\r\nHost: x\r\n\r\n"); err != nil {
			t.Fatalf("write to silent backend: %v", err)
		}
		if err := conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond)); err != nil {
			t.Fatalf("set read deadline: %v", err)
		}
		buf := make([]byte, 1)
		if n, err := conn.Read(buf); err == nil {
			t.Fatalf("silent backend wrote %d byte(s); want no response", n)
		} else if ne, ok := err.(net.Error); !ok || !ne.Timeout() {
			t.Fatalf("expected read timeout from silent backend, got %v", err)
		}
	})
}
