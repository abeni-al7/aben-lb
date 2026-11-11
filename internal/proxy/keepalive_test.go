package proxy

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

// startKeepAliveProxy starts the proxy with client keep-alive enabled and
// captures its log records.
func startKeepAliveProxy(t *testing.T, backends []string, idle time.Duration) (*Server, *logBuffer) {
	t.Helper()
	cfg, err := NewConfig("127.0.0.1:0", strings.Join(backends, ","))
	if err != nil {
		t.Fatalf("build proxy config: %v", err)
	}
	if err := cfg.SetKeepAliveTimeout(idle); err != nil {
		t.Fatalf("set keep-alive timeout: %v", err)
	}
	out := &logBuffer{}
	srv := NewServer(cfg, out)
	if err := srv.Start(); err != nil {
		t.Fatalf("start proxy: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	})
	return srv, out
}

// dialProxy opens one raw TCP connection to the proxy.
func dialProxy(t *testing.T, srv *Server) (net.Conn, *bufio.Reader) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", srv.Addr(), 5*time.Second)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	return conn, bufio.NewReader(conn)
}

// roundTrip writes one GET on conn and reads exactly one response from it.
func roundTrip(t *testing.T, conn net.Conn, r *bufio.Reader, extraHeader string) (*http.Response, string) {
	t.Helper()
	if _, err := io.WriteString(conn, "GET / HTTP/1.1\r\nHost: lb.test\r\n"+extraHeader+"\r\n"); err != nil {
		t.Fatalf("write request: %v", err)
	}
	resp, err := http.ReadResponse(r, nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp, string(body)
}

// expectClosed checks that the server closes conn within timeout.
func expectClosed(t *testing.T, conn net.Conn, r *bufio.Reader, timeout time.Duration) {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(timeout))
	if _, err := r.ReadByte(); err != io.EOF {
		t.Fatalf("connection not closed by server within %v: read error %v", timeout, err)
	}
}

// TestKeepAliveServesSeveralRequestsOnOneConnection sends four requests on a
// single client connection; round robin still applies per request.
func TestKeepAliveServesSeveralRequestsOnOneConnection(t *testing.T) {
	a := namedBackend(t, "A", nil)
	b := namedBackend(t, "B", nil)
	srv, _ := startKeepAliveProxy(t, []string{a.URL, b.URL}, time.Minute)
	conn, r := dialProxy(t, srv)

	var got []string
	for i := 0; i < 4; i++ {
		resp, body := roundTrip(t, conn, r, "")
		if resp.StatusCode != http.StatusOK || resp.Close {
			t.Fatalf("request %d: status %d close=%v; want 200 with connection kept open", i+1, resp.StatusCode, resp.Close)
		}
		got = append(got, body)
	}
	if want := "A B A B"; strings.Join(got, " ") != want {
		t.Fatalf("bodies on one connection = %q, want %q", strings.Join(got, " "), want)
	}
}

// TestKeepAliveValidatesEveryRequest sends a valid request and then a
// malformed one on the same connection: the second must still get the exact
// generated 400, proving validation is re-armed between requests.
func TestKeepAliveValidatesEveryRequest(t *testing.T) {
	a := namedBackend(t, "A", nil)
	srv, _ := startKeepAliveProxy(t, []string{a.URL}, time.Minute)
	conn, r := dialProxy(t, srv)

	if resp, body := roundTrip(t, conn, r, ""); resp.StatusCode != http.StatusOK || body != "A" {
		t.Fatalf("first request: %d %q, want 200 \"A\"", resp.StatusCode, body)
	}
	if _, err := io.WriteString(conn, "NOT A REQUEST\r\n\r\n"); err != nil {
		t.Fatalf("write malformed request: %v", err)
	}
	rest, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read 400: %v", err)
	}
	if !strings.HasPrefix(string(rest), "HTTP/1.1 400 Bad Request") || !strings.HasSuffix(string(rest), badRequestBody) {
		t.Fatalf("malformed second request got %q, want the generated 400", rest)
	}
}

// TestKeepAliveIdleTimeoutClosesConnection checks an idle connection is
// closed once the keep-alive timeout passes, without an error record.
func TestKeepAliveIdleTimeoutClosesConnection(t *testing.T) {
	a := namedBackend(t, "A", nil)
	srv, out := startKeepAliveProxy(t, []string{a.URL}, 100*time.Millisecond)
	conn, r := dialProxy(t, srv)

	roundTrip(t, conn, r, "")
	start := time.Now()
	expectClosed(t, conn, r, 2*time.Second)
	if waited := time.Since(start); waited < 50*time.Millisecond {
		t.Fatalf("connection closed after %v, before the 100ms idle timeout", waited)
	}
	if strings.Contains(out.String(), "event=error") {
		t.Fatalf("idle close was logged as an error:\n%s", out.String())
	}
}

// TestKeepAliveHonoursConnectionClose checks a client can still ask for the
// connection to be closed after its request.
func TestKeepAliveHonoursConnectionClose(t *testing.T) {
	a := namedBackend(t, "A", nil)
	srv, _ := startKeepAliveProxy(t, []string{a.URL}, time.Minute)
	conn, r := dialProxy(t, srv)

	resp, _ := roundTrip(t, conn, r, "Connection: close\r\n")
	if !resp.Close {
		t.Fatalf("response to Connection: close did not announce close")
	}
	expectClosed(t, conn, r, 2*time.Second)
}

// TestResponseRecordIncludesBackendDuration checks the response record says
// which backend answered and how long it took.
func TestResponseRecordIncludesBackendDuration(t *testing.T) {
	be := startDelayedBackend(t, 50*time.Millisecond, "slow")
	srv, out := startKeepAliveProxy(t, []string{be.URL}, time.Minute)

	resp, err := http.Get("http://" + srv.Addr() + "/")
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	resp.Body.Close()

	records := recordsWithEvent(out.String(), "response")
	if len(records) != 1 {
		t.Fatalf("got %d response records, want 1; logs:\n%s", len(records), out.String())
	}
	if got, want := recordField(t, records[0], "backend"), strings.TrimPrefix(be.URL, "http://"); got != want {
		t.Fatalf("backend = %q, want %q", got, want)
	}
	ms, err := strconv.ParseFloat(recordField(t, records[0], "duration_ms"), 64)
	if err != nil {
		t.Fatalf("duration_ms is not a number: %v", err)
	}
	if ms < 50 || ms > 5000 {
		t.Fatalf("duration_ms = %v, want at least the backend's 50ms delay", ms)
	}
}

func TestSetKeepAliveTimeoutValidation(t *testing.T) {
	cfg, err := NewConfig("127.0.0.1:0", "http://127.0.0.1:9000")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.KeepAliveTimeout != DefaultKeepAliveTimeout {
		t.Fatalf("default keep-alive = %v, want %v", cfg.KeepAliveTimeout, DefaultKeepAliveTimeout)
	}
	if err := cfg.SetKeepAliveTimeout(0); err != nil {
		t.Fatalf("SetKeepAliveTimeout(0) = %v, want nil (keep-alive off)", err)
	}
	if err := cfg.SetKeepAliveTimeout(-time.Second); err == nil {
		t.Fatal("SetKeepAliveTimeout(-1s) succeeded, want error")
	}
}
