package proxy

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

// logBuffer captures proxy diagnostics while the server writes concurrently.
type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *logBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// startObservedProxy starts the proxy on an ephemeral port with captured
// diagnostic output and shuts it down when the test finishes.
func startObservedProxy(t *testing.T, backend string) (*Server, *logBuffer) {
	t.Helper()
	out := &logBuffer{}
	cfg, err := NewConfig("127.0.0.1:0", backend)
	if err != nil {
		t.Fatalf("build proxy config: %v", err)
	}
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

// recordsWithEvent returns every log line carrying the given event field.
func recordsWithEvent(logs, event string) []string {
	var out []string
	for _, line := range strings.Split(logs, "\n") {
		if strings.HasPrefix(line, "event="+event+" ") || line == "event="+event {
			out = append(out, line)
		}
	}
	return out
}

// recordField extracts the value of a space-delimited key=value field.
func recordField(t *testing.T, record, key string) string {
	t.Helper()
	for _, f := range strings.Fields(record) {
		if v, ok := strings.CutPrefix(f, key+"="); ok {
			return v
		}
	}
	t.Fatalf("record %q missing field %s", record, key)
	return ""
}

// TestRequestIDsUniqueAndCorrelated expects every accepted request to log a
// unique request ID and an outcome record carrying the same ID and the
// backend status (FR-011, FR-013).
func TestRequestIDsUniqueAndCorrelated(t *testing.T) {
	backend := &echoBackend{respStatus: http.StatusOK, respBody: []byte("ok")}
	backendSrv := backend.start(t)
	proxySrv, out := startObservedProxy(t, backendSrv.URL)

	const total = 25
	client := oneShotClient()
	for i := 0; i < total; i++ {
		resp, err := client.Get("http://" + proxySrv.Addr() + "/")
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}

	logs := out.String()
	requests := recordsWithEvent(logs, "request")
	if len(requests) != total {
		t.Fatalf("got %d request records, want %d; logs:\n%s", len(requests), total, logs)
	}
	responses := recordsWithEvent(logs, "response")
	if len(responses) != total {
		t.Fatalf("got %d response records, want %d; logs:\n%s", len(responses), total, logs)
	}

	respByID := make(map[string]string, total)
	for _, rec := range responses {
		respByID[recordField(t, rec, "id")] = rec
	}
	seen := make(map[string]bool, total)
	for _, rec := range requests {
		id := recordField(t, rec, "id")
		if id == "" {
			t.Fatalf("request record has empty id: %q", rec)
		}
		if seen[id] {
			t.Fatalf("request ID %s is not unique; logs:\n%s", id, logs)
		}
		seen[id] = true
		outcome, ok := respByID[id]
		if !ok {
			t.Fatalf("request ID %s has no outcome record; logs:\n%s", id, logs)
		}
		if status := recordField(t, outcome, "status"); status != "200" {
			t.Fatalf("outcome status = %s, want 200: %q", status, outcome)
		}
	}
}

// TestRequestRecordFields expects the accepted-request record to identify the
// client address, method, and redacted target (FR-011).
func TestRequestRecordFields(t *testing.T) {
	backend := &echoBackend{}
	backendSrv := backend.start(t)
	proxySrv, out := startObservedProxy(t, backendSrv.URL)

	resp, err := oneShotClient().Get("http://" + proxySrv.Addr() + "/")
	if err != nil {
		t.Fatalf("request through proxy: %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	requests := recordsWithEvent(out.String(), "request")
	if len(requests) != 1 {
		t.Fatalf("got %d request records, want 1; logs:\n%s", len(requests), out.String())
	}
	rec := requests[0]
	if method := recordField(t, rec, "method"); method != "GET" {
		t.Fatalf("method = %s, want GET: %q", method, rec)
	}
	if target := recordField(t, rec, "target"); target != "/" {
		t.Fatalf("target = %s, want /: %q", target, rec)
	}
	client := recordField(t, rec, "client")
	if !strings.HasPrefix(client, "127.0.0.1:") {
		t.Fatalf("client = %s, want a loopback host:port: %q", client, rec)
	}
}

// TestTargetRedaction expects non-root paths and present queries to be
// redacted while the root path stays visible (FR-011).
func TestTargetRedaction(t *testing.T) {
	cases := []struct {
		name       string
		target     string
		wantTarget string
		secrets    []string
	}{
		{"root path stays visible", "/", "/", nil},
		{"non-root path redacted", "/secret/customer-42", "/[REDACTED]", []string{"secret", "customer-42"}},
		{"root query redacted", "/?token=query-secret", "/?[REDACTED]", []string{"token", "query-secret"}},
		{"path and query redacted", "/admin/panel?key=q-secret-2", "/[REDACTED]?[REDACTED]", []string{"admin", "panel", "key", "q-secret-2"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			backend := &echoBackend{}
			backendSrv := backend.start(t)
			proxySrv, out := startObservedProxy(t, backendSrv.URL)

			resp, err := oneShotClient().Get("http://" + proxySrv.Addr() + tc.target)
			if err != nil {
				t.Fatalf("request through proxy: %v", err)
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()

			logs := out.String()
			requests := recordsWithEvent(logs, "request")
			if len(requests) != 1 {
				t.Fatalf("got %d request records, want 1; logs:\n%s", len(requests), logs)
			}
			if target := recordField(t, requests[0], "target"); target != tc.wantTarget {
				t.Fatalf("target = %s, want %s: %q", target, tc.wantTarget, requests[0])
			}
			for _, secret := range tc.secrets {
				if strings.Contains(logs, secret) {
					t.Fatalf("raw target fragment %q leaked into logs:\n%s", secret, logs)
				}
			}
		})
	}
}

// TestHeaderValueAllowlist expects only Host, User-Agent, Accept,
// Content-Type, and Content-Length values to be visible while every other
// header value is [REDACTED] (FR-011, FR-012).
func TestHeaderValueAllowlist(t *testing.T) {
	backend := &echoBackend{}
	backendSrv := backend.start(t)
	proxySrv, out := startObservedProxy(t, backendSrv.URL)

	req, err := http.NewRequest(http.MethodPost, "http://"+proxySrv.Addr()+"/", strings.NewReader("payload"))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Host = "allow.test"
	req.Header.Set("User-Agent", "allowed-agent/1.0")
	req.Header.Set("Accept", "text/allowed")
	req.Header.Set("Content-Type", "application/allowed")
	req.Header.Set("Authorization", "Bearer secret-token")
	req.Header.Set("Cookie", "session=cookie-secret")
	req.Header.Set("X-Api-Key", "sk-custom-secret")
	resp, err := oneShotClient().Do(req)
	if err != nil {
		t.Fatalf("request through proxy: %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	logs := out.String()
	requests := recordsWithEvent(logs, "request")
	if len(requests) != 1 {
		t.Fatalf("got %d request records, want 1; logs:\n%s", len(requests), logs)
	}
	rec := requests[0]
	for _, visible := range []string{
		"Host: allow.test",
		"User-Agent: allowed-agent/1.0",
		"Accept: text/allowed",
		"Content-Type: application/allowed",
		"Content-Length: 7",
	} {
		if !strings.Contains(rec, visible) {
			t.Errorf("allowed header value %q missing from record %q", visible, rec)
		}
	}
	for _, name := range []string{"Authorization", "Cookie", "X-Api-Key"} {
		if !strings.Contains(rec, name+": [REDACTED]") {
			t.Errorf("header %s is not redacted in record %q", name, rec)
		}
	}
	for _, secret := range []string{"secret-token", "cookie-secret", "sk-custom-secret"} {
		if strings.Contains(logs, secret) {
			t.Errorf("header secret %q leaked into logs:\n%s", secret, logs)
		}
	}
}

// TestRequestRecordDeterministicOrder expects identical requests to produce
// identical records apart from the unique ID and client port, proving stable
// field and header ordering under concurrency.
func TestRequestRecordDeterministicOrder(t *testing.T) {
	backend := &echoBackend{}
	backendSrv := backend.start(t)
	proxySrv, out := startObservedProxy(t, backendSrv.URL)

	send := func() {
		req, err := http.NewRequest(http.MethodGet, "http://"+proxySrv.Addr()+"/", nil)
		if err != nil {
			t.Fatalf("build request: %v", err)
		}
		req.Header.Set("User-Agent", "stable-agent")
		req.Header.Set("Accept", "text/stable")
		req.Header.Set("X-Zeta", "last")
		req.Header.Set("X-Alpha", "first")
		resp, err := oneShotClient().Do(req)
		if err != nil {
			t.Fatalf("request through proxy: %v", err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	send()
	send()

	requests := recordsWithEvent(out.String(), "request")
	if len(requests) != 2 {
		t.Fatalf("got %d request records, want 2; logs:\n%s", len(requests), out.String())
	}
	strip := regexp.MustCompile(`(id|client)=\S+ `)
	first := strip.ReplaceAllString(requests[0], "")
	second := strip.ReplaceAllString(requests[1], "")
	if first != second {
		t.Fatalf("identical requests produced different records:\n%q\n%q", first, second)
	}
}

// TestBodiesExcludedFromLogs expects request and response bodies to never
// appear in diagnostic output (FR-012).
func TestBodiesExcludedFromLogs(t *testing.T) {
	const (
		requestSecret  = "top-secret-request-body"
		responseSecret = "top-secret-response-body"
	)
	backend := &echoBackend{respBody: []byte(responseSecret)}
	backendSrv := backend.start(t)
	proxySrv, out := startObservedProxy(t, backendSrv.URL)

	resp, err := oneShotClient().Post("http://"+proxySrv.Addr()+"/", "text/plain", strings.NewReader(requestSecret))
	if err != nil {
		t.Fatalf("request through proxy: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil || string(body) != responseSecret {
		t.Fatalf("relay body = %q err = %v, want %q", body, err, responseSecret)
	}

	logs := out.String()
	if len(recordsWithEvent(logs, "request")) != 1 {
		t.Fatalf("expected one request record; logs:\n%s", logs)
	}
	for _, secret := range []string{requestSecret, responseSecret} {
		if strings.Contains(logs, secret) {
			t.Fatalf("body %q leaked into logs:\n%s", secret, logs)
		}
	}
}

// TestNoSecretLeakageAcrossOutcomes is a cross-story regression: secrets
// carried in paths, queries, headers, and bodies must stay out of the
// diagnostic stream on the success path (US1), under concurrency (US2), and
// on the generated-error path (US3).
func TestNoSecretLeakageAcrossOutcomes(t *testing.T) {
	secrets := []string{
		"leak-path-secret",
		"leak-query-secret",
		"leak-auth-secret",
		"leak-cookie-secret",
		"leak-body-secret",
	}
	sendSecrets := func(t *testing.T, addr string) {
		t.Helper()
		var wg sync.WaitGroup
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				req, err := http.NewRequest(http.MethodPost,
					"http://"+addr+"/leak-path-secret?q=leak-query-secret",
					strings.NewReader("leak-body-secret"))
				if err != nil {
					t.Errorf("build secret request: %v", err)
					return
				}
				req.Header.Set("Authorization", "Bearer leak-auth-secret")
				req.Header.Set("Cookie", "session=leak-cookie-secret")
				resp, err := oneShotClient().Do(req)
				if err != nil {
					return // a refused backend fails at the client; logs still matter
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}()
		}
		wg.Wait()
	}

	t.Run("healthy backend", func(t *testing.T) {
		backend := &echoBackend{respBody: []byte("ok")}
		backendSrv := backend.start(t)
		proxySrv, out := startObservedProxy(t, backendSrv.URL)
		sendSecrets(t, proxySrv.Addr())
		logs := out.String()
		for _, secret := range secrets {
			if strings.Contains(logs, secret) {
				t.Errorf("secret %q leaked on the success path:\n%s", secret, logs)
			}
		}
	})

	t.Run("failing backend", func(t *testing.T) {
		ln := newEphemeralListener(t)
		refusedAddr := ln.Addr().String()
		ln.Close()
		proxySrv, out := startObservedProxy(t, "http://"+refusedAddr)
		sendSecrets(t, proxySrv.Addr())
		// Wait for every generated-error outcome so late records are checked.
		deadline := time.Now().Add(5 * time.Second)
		for len(recordsWithEvent(out.String(), "error")) < 5 && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		logs := out.String()
		if got := len(recordsWithEvent(logs, "error")); got < 5 {
			t.Fatalf("got %d generated-error records, want 5; logs:\n%s", got, logs)
		}
		for _, secret := range secrets {
			if strings.Contains(logs, secret) {
				t.Errorf("secret %q leaked on the generated-error path:\n%s", secret, logs)
			}
		}
	})
}
