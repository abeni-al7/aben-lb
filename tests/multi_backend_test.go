package tests

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// clientRequestMarker is how a be process logs a client request (as opposed
// to a /healthz health check) in its event=request record.
const clientRequestMarker = "target=/work "

// sendConcurrently has `clients` goroutines each send `perClient` GETs of
// /work through lb, reusing connections (keep-alive). It returns how many
// requests succeeded and how many ran on a reused connection.
func sendConcurrently(t *testing.T, lbAddr string, clients, perClient int) (ok, reused int64) {
	t.Helper()
	client := &http.Client{
		Timeout:   10 * time.Second,
		Transport: &http.Transport{MaxIdleConnsPerHost: clients},
	}
	defer client.CloseIdleConnections()
	trace := &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) {
		if info.Reused {
			atomic.AddInt64(&reused, 1)
		}
	}}

	var wg sync.WaitGroup
	errs := make(chan error, clients*perClient)
	for c := 0; c < clients; c++ {
		wg.Go(func() {
			for i := 0; i < perClient; i++ {
				req, _ := http.NewRequest(http.MethodGet, "http://"+lbAddr+"/work", nil)
				req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
				resp, err := client.Do(req)
				if err != nil {
					errs <- err
					continue
				}
				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if resp.StatusCode != http.StatusOK || string(body) != "Hello From Backend Server" {
					errs <- fmt.Errorf("got %d %q", resp.StatusCode, body)
					continue
				}
				atomic.AddInt64(&ok, 1)
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("client request failed: %v", err)
	}
	return ok, reused
}

// TestMultipleBackendsEndToEnd stands up three backend processes, a load
// balancer, and many concurrent keep-alive clients. It checks even
// round-robin distribution, connection reuse, timing in the logs, and that a
// killed backend is taken out of rotation without client errors.
func TestMultipleBackendsEndToEnd(t *testing.T) {
	var backends []*process
	var addrs, urls []string
	for i := 0; i < 3; i++ {
		addr := freeLoopbackAddr(t)
		be := startProcess(t, "be", "-listen", addr)
		be.waitForOutput(addr, 5*time.Second)
		backends = append(backends, be)
		addrs = append(addrs, addr)
		urls = append(urls, "http://"+addr)
	}

	lbAddr := freeLoopbackAddr(t)
	lb := startProcess(t, "lb", "-listen", lbAddr, "-backend", strings.Join(urls, ","),
		"-health-interval", "200ms", "-health-path", "/healthz", "-keepalive-timeout", "30s")
	lb.waitForOutput(lbAddr, 5*time.Second)

	// Phase 1: 20 clients x 15 requests, all backends healthy.
	const clients, perClient = 20, 15
	ok, reused := sendConcurrently(t, lbAddr, clients, perClient)
	if ok != clients*perClient {
		t.Fatalf("%d/%d requests succeeded", ok, clients*perClient)
	}
	for i, be := range backends {
		if got := strings.Count(be.output(), clientRequestMarker); got != 100 {
			t.Errorf("backend %d served %d client requests, want exactly 100 (round robin)", i, got)
		}
	}
	// Each client opens at most a few connections and reuses them after.
	if reused < clients*perClient-3*clients {
		t.Errorf("only %d/%d requests reused a kept-alive connection", reused, clients*perClient)
	}
	if got := strings.Count(lb.output(), "duration_ms="); got < clients*perClient {
		t.Errorf("only %d response records carry duration_ms, want %d", got, clients*perClient)
	}

	// Phase 2: kill backend 2; once the health check notices, every request
	// must succeed on the remaining two, split evenly.
	backends[2].kill()
	lb.waitForOutput("event=health backend="+addrs[2]+" state=down", 5*time.Second)
	before := []int{
		strings.Count(backends[0].output(), clientRequestMarker),
		strings.Count(backends[1].output(), clientRequestMarker),
	}
	ok, _ = sendConcurrently(t, lbAddr, 10, 6)
	if ok != 60 {
		t.Fatalf("after killing a backend %d/60 requests succeeded; lb output:\n%s", ok, lb.output())
	}
	for i := 0; i < 2; i++ {
		if got := strings.Count(backends[i].output(), clientRequestMarker) - before[i]; got != 30 {
			t.Errorf("backend %d served %d of 60 requests after the kill, want 30", i, got)
		}
	}
}
