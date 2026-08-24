package testbackend

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// lockedBuffer captures server diagnostics safely while handler goroutines
// write concurrently with test reads.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func startServer(t *testing.T) (*Server, *lockedBuffer) {
	t.Helper()
	out := &lockedBuffer{}
	srv := New("127.0.0.1:0", out)
	if err := srv.Start(); err != nil {
		t.Fatalf("start test backend: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	})
	return srv, out
}

func waitForLog(t *testing.T, out *lockedBuffer, substr string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(out.String(), substr) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("output did not contain %q; output:\n%s", substr, out.String())
}

func TestServerListensOnConfiguredAddress(t *testing.T) {
	srv, out := startServer(t)
	host, port, err := net.SplitHostPort(srv.Addr())
	if err != nil {
		t.Fatalf("listening address %q invalid: %v", srv.Addr(), err)
	}
	if host != "127.0.0.1" {
		t.Fatalf("listening host = %q, want configured 127.0.0.1", host)
	}
	if port == "" || port == "0" {
		t.Fatalf("listening port = %q, want OS-assigned port", port)
	}
	waitForLog(t, out, "event=startup")
	waitForLog(t, out, srv.Addr())
}

func TestServerReturnsExactResponse(t *testing.T) {
	srv, _ := startServer(t)
	resp, err := http.Get("http://" + srv.Addr() + "/any/path")
	if err != nil {
		t.Fatalf("request test backend: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got := string(body); got != "Hello From Backend Server" {
		t.Fatalf("body = %q, want exactly %q", got, "Hello From Backend Server")
	}
}

func TestServerLogsRequestAndResponseConfirmation(t *testing.T) {
	srv, out := startServer(t)
	resp, err := http.Get("http://" + srv.Addr() + "/logged-path")
	if err != nil {
		t.Fatalf("request test backend: %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	waitForLog(t, out, "event=request")
	waitForLog(t, out, "method=GET")
	waitForLog(t, out, "target=/logged-path")
	waitForLog(t, out, "client=127.0.0.1:")
	waitForLog(t, out, "event=response_sent")
	waitForLog(t, out, "status=200")
}

func TestServerShutsDownGracefully(t *testing.T) {
	out := &lockedBuffer{}
	srv := New("127.0.0.1:0", out)
	if err := srv.Start(); err != nil {
		t.Fatalf("start test backend: %v", err)
	}
	addr := srv.Addr()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("graceful shutdown failed: %v", err)
	}

	select {
	case err := <-srv.Done():
		if err != nil {
			t.Fatalf("serve loop exited with error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("serve loop did not exit after shutdown")
	}

	if conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond); err == nil {
		conn.Close()
		t.Fatal("server still accepting connections after shutdown")
	}
	waitForLog(t, out, "event=shutdown")
}
