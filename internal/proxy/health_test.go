package proxy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const testHealthInterval = 50 * time.Millisecond

// startHealthProxy starts the proxy against backends with a short health
// check interval so tests see state changes quickly.
func startHealthProxy(t *testing.T, backends []string) *Server {
	t.Helper()
	cfg, err := NewConfig("127.0.0.1:0", strings.Join(backends, ","))
	if err != nil {
		t.Fatalf("build proxy config: %v", err)
	}
	if err := cfg.SetHealthCheck(testHealthInterval, "/health"); err != nil {
		t.Fatalf("set health check: %v", err)
	}
	srv := NewServer(cfg, io.Discard)
	if err := srv.Start(); err != nil {
		t.Fatalf("start proxy: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	})
	return srv
}

// namedBackend replies with its name, or with 500 to everything (including
// health checks) while sick is set.
func namedBackend(t *testing.T, name string, sick *atomic.Bool) *httptest.Server {
	t.Helper()
	be := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if sick != nil && sick.Load() {
			http.Error(w, "sick", http.StatusInternalServerError)
			return
		}
		io.WriteString(w, name)
	}))
	t.Cleanup(be.Close)
	return be
}

// waitHealthy waits until backend i's health equals want.
func waitHealthy(t *testing.T, srv *Server, i int, want bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for srv.healthy[i].Load() != want {
		if time.Now().After(deadline) {
			t.Fatalf("backend %d healthy=%v after 2s, want %v", i, !want, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// fetch sends n requests through the proxy and returns status:body for each.
func fetch(t *testing.T, srv *Server, n int) []string {
	t.Helper()
	var got []string
	for i := 0; i < n; i++ {
		resp, err := http.Get("http://" + srv.Addr() + "/")
		if err != nil {
			t.Fatalf("request %d: %v", i+1, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			got = append(got, string(body))
		} else {
			got = append(got, resp.Status)
		}
	}
	return got
}

func TestHealthCheckRemovesAndRestoresBackend(t *testing.T) {
	var sick atomic.Bool
	a := namedBackend(t, "A", nil)
	b := namedBackend(t, "B", &sick)
	c := namedBackend(t, "C", nil)
	srv := startHealthProxy(t, []string{a.URL, b.URL, c.URL})

	if got := strings.Join(fetch(t, srv, 3), " "); got != "A B C" {
		t.Fatalf("all healthy: got %q, want %q", got, "A B C")
	}

	sick.Store(true)
	waitHealthy(t, srv, 1, false)
	got := fetch(t, srv, 4)
	countA, countC := 0, 0
	for _, g := range got {
		switch g {
		case "A":
			countA++
		case "C":
			countC++
		default:
			t.Fatalf("with B unhealthy got %q (all: %v); want only A and C", g, got)
		}
	}
	if countA != 2 || countC != 2 {
		t.Fatalf("with B unhealthy got %v; want A and C twice each", got)
	}

	sick.Store(false)
	waitHealthy(t, srv, 1, true)
	got = fetch(t, srv, 3)
	if !strings.Contains(strings.Join(got, " "), "B") {
		t.Fatalf("after B recovered got %v; want B back in rotation", got)
	}
}

func TestHealthCheckRemovesStoppedBackend(t *testing.T) {
	a := namedBackend(t, "A", nil)
	b := namedBackend(t, "B", nil)
	srv := startHealthProxy(t, []string{a.URL, b.URL})

	b.Close() // the server process is gone: connection refused
	waitHealthy(t, srv, 1, false)
	if got := strings.Join(fetch(t, srv, 3), " "); got != "A A A" {
		t.Fatalf("with B stopped got %q, want %q", got, "A A A")
	}
}

func TestNoHealthyBackendReturns503(t *testing.T) {
	var sick atomic.Bool
	sick.Store(true)
	a := namedBackend(t, "A", &sick)
	srv := startHealthProxy(t, []string{a.URL})
	waitHealthy(t, srv, 0, false)

	resp, err := http.Get("http://" + srv.Addr() + "/")
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable || string(body) != serviceUnavailableBody {
		t.Fatalf("got %d %q, want 503 %q", resp.StatusCode, body, serviceUnavailableBody)
	}
}

func TestSetHealthCheckValidation(t *testing.T) {
	tests := []struct {
		interval time.Duration
		path     string
		wantErr  bool
	}{
		{10 * time.Second, "/", false},
		{time.Second, "/healthz", false},
		{0, "/", true},
		{-time.Second, "/", true},
		{time.Second, "healthz", true},
		{time.Second, "", true},
	}
	for _, tt := range tests {
		cfg, err := NewConfig("127.0.0.1:0", "http://127.0.0.1:9000")
		if err != nil {
			t.Fatal(err)
		}
		err = cfg.SetHealthCheck(tt.interval, tt.path)
		if (err != nil) != tt.wantErr {
			t.Errorf("SetHealthCheck(%v, %q) error = %v, wantErr %v", tt.interval, tt.path, err, tt.wantErr)
		}
	}
}
