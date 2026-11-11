package tests

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptrace"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Performance validation profile from the specification: five clean-process
// Linux-loopback runs, a direct-backend health precheck, a 2-second warm-up,
// a 30-second measured window with 100 clients on a fixed 70/20/10 request
// mix, at least 1,000 measured requests, at least 99% success, and at least
// 95% of paired per-entry overheads at or below 100 milliseconds, with four
// of five runs required to pass (SC-002, SC-003).
const (
	perfRuns          = 5
	perfRunsToPass    = 4
	perfClients       = 100
	perfWarmup        = 2 * time.Second
	perfWindow        = 30 * time.Second
	perfMinRequests   = 1000
	perfOverheadLimit = 100 * time.Millisecond

	precheckWindow      = 5 * time.Second
	precheckMinSuccess  = 0.999
	precheckP95Limit    = 50 * time.Millisecond
	expectedBackendBody = "Hello From Backend Server"
)

// perfKind identifies one entry of the deterministic request mix.
type perfKind int

const (
	kindGet        perfKind = iota // 70%: GET without a body
	kindPostText                   // 20%: POST with a 1 KiB text body
	kindPostBinary                 // 10%: POST with a 64 KiB binary body
)

var perfKindNames = map[perfKind]string{
	kindGet:        "GET",
	kindPostText:   "POST-1KiB-text",
	kindPostBinary: "POST-64KiB-binary",
}

// kindForIndex maps a global request index onto the fixed 70/20/10 mix.
func kindForIndex(i uint64) perfKind {
	switch i % 10 {
	case 7, 8:
		return kindPostText
	case 9:
		return kindPostBinary
	default:
		return kindGet
	}
}

var (
	perfTextBody   = bytes.Repeat([]byte("perf-text-body-1"), 64) // 1 KiB
	perfBinaryBody = func() []byte {
		b := make([]byte, 64*1024)
		for i := range b {
			b[i] = byte(i*31 + 7)
		}
		return b
	}()
)

// perfSample records one measured request attempt.
type perfSample struct {
	kind    perfKind
	elapsed time.Duration
	ok      bool
}

// perfClientPool builds one shared client that never reuses connections, so
// every measured request establishes its own one-request connection.
func perfClientPool() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DisableKeepAlives:   true,
			MaxIdleConnsPerHost: -1,
		},
		Timeout: 30 * time.Second,
	}
}

// timedRequest performs one mix request and measures elapsed time from
// completion of the request write through completion of the response-body
// read; ok requires the complete expected backend response.
func timedRequest(client *http.Client, baseURL string, kind perfKind) perfSample {
	var body io.Reader
	switch kind {
	case kindPostText:
		body = bytes.NewReader(perfTextBody)
	case kindPostBinary:
		body = bytes.NewReader(perfBinaryBody)
	}
	method := http.MethodGet
	if kind != kindGet {
		method = http.MethodPost
	}
	req, err := http.NewRequest(method, baseURL+"/", body)
	if err != nil {
		return perfSample{kind: kind}
	}
	if kind == kindPostText {
		req.Header.Set("Content-Type", "text/plain")
	} else if kind == kindPostBinary {
		req.Header.Set("Content-Type", "application/octet-stream")
	}

	start := time.Now()
	// WroteRequest fires on the transport's write goroutine, so the
	// timestamp crosses goroutines and must be atomic.
	var wroteNanos atomic.Int64
	wroteNanos.Store(start.UnixNano())
	trace := &httptrace.ClientTrace{
		WroteRequest: func(httptrace.WroteRequestInfo) { wroteNanos.Store(time.Now().UnixNano()) },
	}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))

	resp, err := client.Do(req)
	if err != nil {
		return perfSample{kind: kind}
	}
	respBody, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	elapsed := time.Since(time.Unix(0, wroteNanos.Load()))
	ok := err == nil && resp.StatusCode == http.StatusOK && string(respBody) == expectedBackendBody
	return perfSample{kind: kind, elapsed: elapsed, ok: ok}
}

// runMix drives the fixed request mix from concurrent clients against
// baseURL for the window and returns every attempted sample without retries.
func runMix(baseURL string, clients int, window time.Duration) []perfSample {
	var next atomic.Uint64
	deadline := time.Now().Add(window)
	perClient := make([][]perfSample, clients)
	var wg sync.WaitGroup
	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func(slot int) {
			defer wg.Done()
			client := perfClientPool()
			for time.Now().Before(deadline) {
				kind := kindForIndex(next.Add(1))
				perClient[slot] = append(perClient[slot], timedRequest(client, baseURL, kind))
			}
		}(c)
	}
	wg.Wait()
	var all []perfSample
	for _, samples := range perClient {
		all = append(all, samples...)
	}
	return all
}

// successRate returns the fraction of attempted samples with the complete
// expected response.
func successRate(samples []perfSample) float64 {
	if len(samples) == 0 {
		return 0
	}
	ok := 0
	for _, s := range samples {
		if s.ok {
			ok++
		}
	}
	return float64(ok) / float64(len(samples))
}

// p95 returns the 95th-percentile elapsed time of successful samples.
func p95(samples []perfSample) time.Duration {
	var elapsed []time.Duration
	for _, s := range samples {
		if s.ok {
			elapsed = append(elapsed, s.elapsed)
		}
	}
	if len(elapsed) == 0 {
		return 0
	}
	for i := 1; i < len(elapsed); i++ {
		for j := i; j > 0 && elapsed[j] < elapsed[j-1]; j-- {
			elapsed[j], elapsed[j-1] = elapsed[j-1], elapsed[j]
		}
	}
	return elapsed[(len(elapsed)*95+99)/100-1]
}

// byKind groups successful sample elapsed times by mix entry.
func byKind(samples []perfSample) map[perfKind][]time.Duration {
	grouped := make(map[perfKind][]time.Duration)
	for _, s := range samples {
		if s.ok {
			grouped[s.kind] = append(grouped[s.kind], s.elapsed)
		}
	}
	return grouped
}

// startPerfProcesses starts clean be and lb processes and returns the lb and
// be base URLs.
func startPerfProcesses(tb testing.TB) (lbURL, beURL string, stop func()) {
	tb.Helper()
	beAddr := freeLoopbackAddr(tb)
	be := startProcess(tb, "be", "-listen", beAddr)
	be.waitForOutput(beAddr, 5*time.Second)

	lbAddr := freeLoopbackAddr(tb)
	lb := startProcess(tb, "lb", "-listen", lbAddr, "-backend", "http://"+beAddr, "-keepalive-timeout", "0")
	lb.waitForOutput(lbAddr, 5*time.Second)

	return "http://" + lbAddr, "http://" + beAddr, func() {
		lb.kill()
		be.kill()
	}
}

// TestPerformanceBudget validates SC-002 and SC-003: five clean-process
// Linux-loopback runs with a direct-backend health precheck, a warm-up, a
// fixed 100-client mix for 30 seconds, a 1,000-request minimum, 99% success,
// and paired per-entry overhead within 100 ms for 95% of requests, passing
// when at least four of five runs meet every threshold.
func TestPerformanceBudget(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skipf("performance profile requires Linux loopback, running on %s", runtime.GOOS)
	}
	if testing.Short() {
		t.Skip("skipping five-run performance validation in short mode")
	}

	passed := 0
	for run := 1; run <= perfRuns; run++ {
		if runPerformanceOnce(t, run) {
			passed++
		}
	}
	if passed < perfRunsToPass {
		t.Fatalf("%d/%d runs met every threshold, want at least %d", passed, perfRuns, perfRunsToPass)
	}
	t.Logf("performance budget met: %d/%d runs passed", passed, perfRuns)
}

// runPerformanceOnce executes one clean-process run and reports whether it
// met every SC-002/SC-003 threshold. Precheck failures invalidate the whole
// test rather than failing it.
func runPerformanceOnce(t *testing.T, run int) bool {
	t.Helper()
	lbURL, beURL, stop := startPerfProcesses(t)
	defer stop()

	// Health precheck: the backend alone must pass the same 100-client mix
	// directly; otherwise the proxy run is invalid rather than failed. The
	// direct samples double as the paired overhead baselines.
	direct := runMix(beURL, perfClients, precheckWindow)
	if rate := successRate(direct); rate < precheckMinSuccess {
		t.Skipf("run %d invalid: direct backend success %.4f below %.4f", run, rate, precheckMinSuccess)
	}
	if p := p95(direct); p > precheckP95Limit {
		t.Skipf("run %d invalid: direct backend p95 %v above %v", run, p, precheckP95Limit)
	}
	directByKind := byKind(direct)

	// Warm-up requests are reported separately and never measured.
	warmup := runMix(lbURL, perfClients, perfWarmup)
	t.Logf("run %d warm-up: %d requests, success %.4f", run, len(warmup), successRate(warmup))

	measured := runMix(lbURL, perfClients, perfWindow)

	pass := true
	if len(measured) < perfMinRequests {
		t.Logf("run %d: %d measured requests, want at least %d", run, len(measured), perfMinRequests)
		pass = false
	}
	rate := successRate(measured)
	if rate < 0.99 {
		t.Logf("run %d: success rate %.4f below 0.99", run, rate)
		pass = false
	}

	// Pair each successful proxied request with a direct-backend request of
	// the same mix entry; overhead is the proxied minus paired direct time.
	withinBudget, paired := 0, 0
	for kind, elapsed := range byKind(measured) {
		baseline := directByKind[kind]
		if len(baseline) == 0 {
			t.Logf("run %d: no direct baseline for %s", run, perfKindNames[kind])
			pass = false
			continue
		}
		for i, proxied := range elapsed {
			paired++
			if proxied-baseline[i%len(baseline)] <= perfOverheadLimit {
				withinBudget++
			}
		}
	}
	if paired == 0 {
		t.Logf("run %d: no successful measured requests to pair", run)
		pass = false
	} else if frac := float64(withinBudget) / float64(paired); frac < 0.95 {
		t.Logf("run %d: only %.4f of paired overheads within %v", run, frac, perfOverheadLimit)
		pass = false
	}

	t.Logf("run %d: measured=%d success=%.4f pairedWithinBudget=%d/%d pass=%v",
		run, len(measured), rate, withinBudget, paired, pass)
	return pass
}

// BenchmarkProxyParallel measures parallel one-request-per-connection GET
// throughput through clean lb and be processes.
func BenchmarkProxyParallel(b *testing.B) {
	lbURL, _, stop := startPerfProcesses(b)
	defer stop()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		client := perfClientPool()
		for pb.Next() {
			sample := timedRequest(client, lbURL, kindGet)
			if !sample.ok {
				b.Fatalf("benchmark request failed")
			}
		}
	})
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/1e6, "ms/req")
}
