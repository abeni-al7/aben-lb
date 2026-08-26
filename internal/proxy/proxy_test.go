package proxy

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// startProxy starts the reverse proxy on an ephemeral port against backend
// and shuts it down when the test finishes.
func startProxy(t *testing.T, backend string) *Server {
	t.Helper()
	cfg, err := NewConfig("127.0.0.1:0", backend)
	if err != nil {
		t.Fatalf("build proxy config: %v", err)
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

// recordedRequest captures what the backend actually received.
type recordedRequest struct {
	method     string
	requestURI string
	host       string
	header     http.Header
	body       []byte
}

// echoBackend records each received request and replies with the configured
// status, headers, and body.
type echoBackend struct {
	mu       sync.Mutex
	requests []recordedRequest

	respStatus int
	respHeader http.Header
	respBody   []byte
}

func (b *echoBackend) start(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("backend read body: %v", err)
		}
		b.mu.Lock()
		b.requests = append(b.requests, recordedRequest{
			method:     r.Method,
			requestURI: r.RequestURI,
			host:       r.Host,
			header:     r.Header.Clone(),
			body:       body,
		})
		b.mu.Unlock()
		for name, values := range b.respHeader {
			for _, v := range values {
				w.Header().Add(name, v)
			}
		}
		status := b.respStatus
		if status == 0 {
			status = http.StatusOK
		}
		w.WriteHeader(status)
		if len(b.respBody) > 0 {
			w.Write(b.respBody)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func (b *echoBackend) recorded(t *testing.T) []recordedRequest {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]recordedRequest, len(b.requests))
	copy(out, b.requests)
	return out
}

// oneShotClient never reuses connections and never follows redirects, which
// matches the one-request-per-connection contract under test.
func oneShotClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{DisableKeepAlives: true},
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Timeout: 30 * time.Second,
	}
}

// patternBytes returns n deterministic non-text bytes for binary payloads.
func patternBytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*31 + 7)
	}
	return b
}

// TestForwardingMatrix covers the SC-001 acceptance matrix: every required
// method, query shape, request body, header multiplicity, response status,
// and response body while asserting unchanged request meaning and an exact
// response relay.
func TestForwardingMatrix(t *testing.T) {
	textBody1KiB := bytes.Repeat([]byte("request-text-body-16"), 64)
	tests := []struct {
		name       string
		method     string
		target     string
		reqBody    []byte
		reqHeader  http.Header
		respStatus int
		respHeader http.Header
		respBody   []byte
		// wantClientBody overrides respBody when HTTP semantics suppress
		// the body toward the client (HEAD).
		wantClientBody []byte
	}{
		{
			name:       "GET no query no body single headers 200 text response",
			method:     http.MethodGet,
			target:     "/",
			reqHeader:  http.Header{"X-Single": {"one"}},
			respStatus: http.StatusOK,
			respHeader: http.Header{"X-Resp-Single": {"alpha"}},
			respBody:   []byte("hello text response"),
		},
		{
			name:           "HEAD no body 200 empty client body",
			method:         http.MethodHead,
			target:         "/head",
			respStatus:     http.StatusOK,
			respBody:       []byte("suppressed for HEAD"),
			wantClientBody: []byte{},
		},
		{
			name:       "POST 1KiB text body multi percent-encoded query repeated headers 200 binary response",
			method:     http.MethodPost,
			target:     "/submit?a=1&b=hello%20world&c=%2Fencoded%2Fpath",
			reqBody:    textBody1KiB,
			reqHeader:  http.Header{"X-Multi": {"first", "second"}, "Content-Type": {"text/plain"}},
			respStatus: http.StatusOK,
			respHeader: http.Header{"Set-Cookie": {"a=1", "b=2"}},
			respBody:   patternBytes(64 * 1024),
		},
		{
			name:       "PUT 64KiB binary body 204 empty response",
			method:     http.MethodPut,
			target:     "/put",
			reqBody:    patternBytes(64 * 1024),
			respStatus: http.StatusNoContent,
		},
		{
			name:       "PATCH text body percent-encoded query 302 text response",
			method:     http.MethodPatch,
			target:     "/patch?q=%E2%9C%93&next=%2Fhome",
			reqBody:    []byte("patch-payload"),
			respStatus: http.StatusFound,
			respHeader: http.Header{"Location": {"/moved"}},
			respBody:   []byte("redirect body"),
		},
		{
			name:       "DELETE no body 404 text response",
			method:     http.MethodDelete,
			target:     "/gone",
			respStatus: http.StatusNotFound,
			respBody:   []byte("resource is gone"),
		},
		{
			name:       "POST 1MiB body 500 with 1MiB response body",
			method:     http.MethodPost,
			target:     "/big",
			reqBody:    patternBytes(1 << 20),
			respStatus: http.StatusInternalServerError,
			respBody:   patternBytes(1 << 20),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backend := &echoBackend{
				respStatus: tt.respStatus,
				respHeader: tt.respHeader,
				respBody:   tt.respBody,
			}
			backendSrv := backend.start(t)
			proxySrv := startProxy(t, backendSrv.URL)

			var bodyReader io.Reader
			if tt.reqBody != nil {
				bodyReader = bytes.NewReader(tt.reqBody)
			}
			req, err := http.NewRequest(tt.method, "http://"+proxySrv.Addr()+tt.target, bodyReader)
			if err != nil {
				t.Fatalf("build request: %v", err)
			}
			for name, values := range tt.reqHeader {
				for _, v := range values {
					req.Header.Add(name, v)
				}
			}
			resp, err := oneShotClient().Do(req)
			if err != nil {
				t.Fatalf("send request through proxy: %v", err)
			}
			gotBody, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				t.Fatalf("read proxied response body: %v", err)
			}

			// Response relay is exact.
			if resp.StatusCode != tt.respStatus {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tt.respStatus)
			}
			wantBody := tt.respBody
			if tt.wantClientBody != nil {
				wantBody = tt.wantClientBody
			}
			if !bytes.Equal(gotBody, wantBody) {
				t.Fatalf("response body mismatch: got %d bytes, want %d bytes", len(gotBody), len(wantBody))
			}
			for name, want := range tt.respHeader {
				if got := resp.Header.Values(name); !equalStrings(got, want) {
					t.Fatalf("response header %s = %v, want %v", name, got, want)
				}
			}

			// Request meaning is preserved.
			recorded := backend.recorded(t)
			if len(recorded) != 1 {
				t.Fatalf("backend received %d requests, want 1", len(recorded))
			}
			got := recorded[0]
			if got.method != tt.method {
				t.Fatalf("backend method = %q, want %q", got.method, tt.method)
			}
			if got.requestURI != tt.target {
				t.Fatalf("backend target = %q, want raw target %q", got.requestURI, tt.target)
			}
			if got.host != proxySrv.Addr() {
				t.Fatalf("backend Host = %q, want preserved client host %q", got.host, proxySrv.Addr())
			}
			wantReqBody := tt.reqBody
			if wantReqBody == nil {
				wantReqBody = []byte{}
			}
			if !bytes.Equal(got.body, wantReqBody) {
				t.Fatalf("backend body mismatch: got %d bytes, want %d bytes", len(got.body), len(wantReqBody))
			}
			for name, want := range tt.reqHeader {
				if gotValues := got.header.Values(name); !equalStrings(gotValues, want) {
					t.Fatalf("backend header %s = %v, want %v", name, gotValues, want)
				}
			}
			for _, forbidden := range []string{"Forwarded", "X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto"} {
				if values := got.header.Values(forbidden); len(values) != 0 {
					t.Fatalf("proxy added %s: %v", forbidden, values)
				}
			}
		})
	}
}

func equalStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// TestChunkedRequestFraming sends a valid chunked request body and expects
// the backend to receive the complete decoded body.
func TestChunkedRequestFraming(t *testing.T) {
	backend := &echoBackend{respStatus: http.StatusOK, respBody: []byte("chunk-ok")}
	backendSrv := backend.start(t)
	proxySrv := startProxy(t, backendSrv.URL)

	raw := "POST /chunked HTTP/1.1\r\n" +
		"Host: chunked.test\r\n" +
		"Transfer-Encoding: chunked\r\n" +
		"\r\n" +
		"5\r\nhello\r\n" +
		"6\r\n world\r\n" +
		"0\r\n\r\n"
	resp := rawHTTPExchange(t, proxySrv.Addr(), raw)
	if !strings.HasPrefix(resp, "HTTP/1.1 200") {
		t.Fatalf("chunked request response:\n%s", resp)
	}

	recorded := backend.recorded(t)
	if len(recorded) != 1 {
		t.Fatalf("backend received %d requests, want 1", len(recorded))
	}
	if got := string(recorded[0].body); got != "hello world" {
		t.Fatalf("backend chunked body = %q, want %q", got, "hello world")
	}
}

// TestHopByHopRequestHeaderRemoval sends every specified hop-by-hop header
// with mixed casing and multiple Connection instances, expecting cumulative
// case-insensitive removal before the backend sees the request.
func TestHopByHopRequestHeaderRemoval(t *testing.T) {
	backend := &echoBackend{respStatus: http.StatusOK, respBody: []byte("ok")}
	backendSrv := backend.start(t)
	proxySrv := startProxy(t, backendSrv.URL)

	raw := "GET / HTTP/1.1\r\n" +
		"Host: example.test\r\n" +
		"CONNECTION: x-hop-one\r\n" +
		"connection: X-HOP-TWO, keep-alive\r\n" +
		"X-Hop-One: nominated-secret-1\r\n" +
		"x-hop-two: nominated-secret-2\r\n" +
		"Keep-Alive: timeout=5\r\n" +
		"PROXY-CONNECTION: keep-alive\r\n" +
		"Proxy-Authenticate: Basic realm=proxy\r\n" +
		"proxy-authorization: Basic aG9wOnNlY3JldA==\r\n" +
		"TE: trailers\r\n" +
		"Upgrade: h2c\r\n" +
		"X-End-To-End: keep-me\r\n" +
		"\r\n"
	resp := rawHTTPExchange(t, proxySrv.Addr(), raw)
	if !strings.HasPrefix(resp, "HTTP/1.1 200") {
		t.Fatalf("hop-by-hop request response:\n%s", resp)
	}

	recorded := backend.recorded(t)
	if len(recorded) != 1 {
		t.Fatalf("backend received %d requests, want 1", len(recorded))
	}
	got := recorded[0]
	removed := []string{
		"Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate",
		"Proxy-Authorization", "Te", "Trailer", "Transfer-Encoding", "Upgrade",
		"X-Hop-One", "X-Hop-Two",
	}
	for _, name := range removed {
		if values := got.header.Values(name); len(values) != 0 {
			t.Fatalf("backend received hop-by-hop header %s: %v", name, values)
		}
	}
	for name, values := range got.header {
		for _, v := range values {
			if strings.Contains(v, "nominated-secret") {
				t.Fatalf("nominated hop-by-hop value leaked via %s: %q", name, v)
			}
		}
	}
	if got.header.Get("X-End-To-End") != "keep-me" {
		t.Fatalf("end-to-end header lost; backend headers: %v", got.header)
	}
	if got.host != "example.test" {
		t.Fatalf("backend Host = %q, want preserved %q", got.host, "example.test")
	}
}

// startScriptedBackend serves one connection with a fixed raw HTTP response
// so hop-by-hop response headers reach the proxy unfiltered.
func startScriptedBackend(t *testing.T, response string) net.Listener {
	t.Helper()
	ln := newEphemeralListener(t)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		// Read request headers before answering.
		buf := make([]byte, 64*1024)
		total := 0
		for !bytes.Contains(buf[:total], []byte("\r\n\r\n")) && total < len(buf) {
			n, err := conn.Read(buf[total:])
			if err != nil {
				return
			}
			total += n
		}
		io.WriteString(conn, response)
	}()
	return ln
}

// TestHopByHopResponseHeaderRemoval expects the proxy to strip standard and
// Connection-nominated hop-by-hop headers, collected case-insensitively from
// every Connection instance, from the backend response.
func TestHopByHopResponseHeaderRemoval(t *testing.T) {
	backendResponse := "HTTP/1.1 200 OK\r\n" +
		"Content-Length: 2\r\n" +
		"Content-Type: text/plain\r\n" +
		"CONNECTION: X-Resp-Hop\r\n" +
		"connection: X-RESP-HOP-TWO\r\n" +
		"X-Resp-Hop: response-nominated-secret\r\n" +
		"x-resp-hop-two: response-nominated-secret-2\r\n" +
		"keep-alive: timeout=5\r\n" +
		"Upgrade: h2c\r\n" +
		"X-Resp-Keep: visible\r\n" +
		"\r\n" +
		"ok"
	ln := startScriptedBackend(t, backendResponse)
	proxySrv := startProxy(t, "http://"+ln.Addr().String())

	resp, err := oneShotClient().Get("http://" + proxySrv.Addr() + "/")
	if err != nil {
		t.Fatalf("request through proxy: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	if resp.StatusCode != http.StatusOK || string(body) != "ok" {
		t.Fatalf("relay status=%d body=%q, want 200 %q", resp.StatusCode, body, "ok")
	}
	for _, name := range []string{"X-Resp-Hop", "X-Resp-Hop-Two", "Keep-Alive", "Upgrade"} {
		if values := resp.Header.Values(name); len(values) != 0 {
			t.Fatalf("client received hop-by-hop response header %s: %v", name, values)
		}
	}
	if resp.Header.Get("X-Resp-Keep") != "visible" {
		t.Fatalf("end-to-end response header lost; headers: %v", resp.Header)
	}
}

// TestUnsupportedSyntaxRejected expects malformed request syntax to produce a
// 400 status without any backend forwarding.
func TestUnsupportedSyntaxRejected(t *testing.T) {
	backend := &echoBackend{respStatus: http.StatusOK}
	backendSrv := backend.start(t)
	proxySrv := startProxy(t, backendSrv.URL)

	resp := rawHTTPExchange(t, proxySrv.Addr(), "INVALID REQUEST\r\nBroken-Header\r\n\r\n")
	if !strings.HasPrefix(resp, "HTTP/1.1 400") {
		t.Fatalf("malformed request response:\n%s", resp)
	}
	if recorded := backend.recorded(t); len(recorded) != 0 {
		t.Fatalf("malformed request reached backend %d time(s)", len(recorded))
	}
}

// startMixedBackend serves the concurrency scenarios by path: token echo,
// body echo, delayed responses, held response headers, client errors,
// abrupt closure for gateway errors, and slow flushed streaming.
func startMixedBackend(t *testing.T, heldArrived chan<- struct{}) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ok":
			io.WriteString(w, "token="+r.Header.Get("X-Token"))
		case "/echo":
			body, _ := io.ReadAll(r.Body)
			w.Write(body)
		case "/slow-response":
			select {
			case <-time.After(500 * time.Millisecond):
			case <-r.Context().Done():
				return
			}
			io.WriteString(w, "slow="+r.Header.Get("X-Token"))
		case "/held":
			if heldArrived != nil {
				heldArrived <- struct{}{}
			}
			// Send no response headers until the proxy or client gives up.
			select {
			case <-time.After(20 * time.Second):
			case <-r.Context().Done():
			}
		case "/client-error":
			http.Error(w, "no such thing", http.StatusNotFound)
		case "/gateway":
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Errorf("hijack gateway connection: %v", err)
				return
			}
			conn.Close()
		case "/stream":
			flusher := w.(http.Flusher)
			for i := 0; i < 20; i++ {
				if _, err := io.WriteString(w, strings.Repeat("s", 1024)); err != nil {
					return
				}
				flusher.Flush()
				select {
				case <-time.After(50 * time.Millisecond):
				case <-r.Context().Done():
					return
				}
			}
		default:
			io.WriteString(w, "default")
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// tokenGet sends one one-shot GET with a unique token and returns the
// response status and body.
func tokenGet(t *testing.T, addr, path, token string) (int, string, error) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, "http://"+addr+path, nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("X-Token", token)
	resp, err := oneShotClient().Do(req)
	if err != nil {
		return 0, "", err
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return 0, "", err
	}
	return resp.StatusCode, string(body), nil
}

// TestConcurrentMixedOutcomes runs slow request bodies, slow backend
// responses, successes, client errors, gateway errors, and client
// disconnects at the same time and expects every unrelated request to
// finish with its own uncontaminated outcome (FR-008, FR-009, FR-010).
func TestConcurrentMixedOutcomes(t *testing.T) {
	backendSrv := startMixedBackend(t, nil)
	proxySrv := startProxy(t, backendSrv.URL)

	var wg sync.WaitGroup
	errs := make(chan error, 128)
	fail := func(format string, args ...any) {
		errs <- fmt.Errorf(format, args...)
	}

	for i := 0; i < 10; i++ {
		token := fmt.Sprintf("ok-%d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			status, body, err := tokenGet(t, proxySrv.Addr(), "/ok", token)
			if err != nil {
				fail("success request %s: %v", token, err)
				return
			}
			if status != http.StatusOK || body != "token="+token {
				fail("success request %s got status=%d body=%q", token, status, body)
			}
		}()
	}

	for i := 0; i < 5; i++ {
		token := fmt.Sprintf("slow-%d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			status, body, err := tokenGet(t, proxySrv.Addr(), "/slow-response", token)
			if err != nil {
				fail("slow response %s: %v", token, err)
				return
			}
			if status != http.StatusOK || body != "slow="+token {
				fail("slow response %s got status=%d body=%q", token, status, body)
			}
		}()
	}

	for i := 0; i < 5; i++ {
		token := fmt.Sprintf("slowbody-%d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Stream the request body slowly across several writes.
			pr, pw := io.Pipe()
			go func() {
				for j := 0; j < 4; j++ {
					io.WriteString(pw, token)
					time.Sleep(50 * time.Millisecond)
				}
				pw.Close()
			}()
			req, err := http.NewRequest(http.MethodPost, "http://"+proxySrv.Addr()+"/echo", pr)
			if err != nil {
				fail("build slow-body request: %v", err)
				return
			}
			req.ContentLength = int64(4 * len(token))
			resp, err := oneShotClient().Do(req)
			if err != nil {
				fail("slow body %s: %v", token, err)
				return
			}
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil || resp.StatusCode != http.StatusOK || string(body) != strings.Repeat(token, 4) {
				fail("slow body %s got status=%d body=%q err=%v", token, resp.StatusCode, body, err)
			}
		}()
	}

	for i := 0; i < 5; i++ {
		token := fmt.Sprintf("err-%d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			status, _, err := tokenGet(t, proxySrv.Addr(), "/client-error", token)
			if err != nil {
				fail("client error request %s: %v", token, err)
				return
			}
			if status != http.StatusNotFound {
				fail("client error request %s got status=%d, want 404", token, status)
			}
		}()
	}

	for i := 0; i < 5; i++ {
		token := fmt.Sprintf("gw-%d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			status, _, err := tokenGet(t, proxySrv.Addr(), "/gateway", token)
			if err != nil {
				fail("gateway request %s: %v", token, err)
				return
			}
			if status < 500 {
				fail("gateway request %s got status=%d, want a 5xx gateway error", token, status)
			}
		}()
	}

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Disconnect while awaiting the response.
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+proxySrv.Addr()+"/held", nil)
			if err != nil {
				fail("build disconnect request: %v", err)
				return
			}
			resp, err := oneShotClient().Do(req)
			if err == nil {
				resp.Body.Close()
			}
		}()
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	status, body, err := tokenGet(t, proxySrv.Addr(), "/ok", "after-mixed")
	if err != nil || status != http.StatusOK || body != "token=after-mixed" {
		t.Fatalf("proxy unavailable after mixed outcomes: status=%d body=%q err=%v", status, body, err)
	}
}

// TestHeldRequestOrdering holds one backend request without response headers
// for the full five-second timeout while 99 independent requests run, and
// expects every fast request to finish before the held request does with at
// least 95% completing within one second (SC-004).
func TestHeldRequestOrdering(t *testing.T) {
	heldArrived := make(chan struct{}, 1)
	backendSrv := startMixedBackend(t, heldArrived)
	proxySrv := startProxy(t, backendSrv.URL)

	heldDone := make(chan time.Time, 1)
	go func() {
		status, _, err := tokenGet(t, proxySrv.Addr(), "/held", "held")
		if err == nil && status < 500 {
			t.Errorf("held request got status=%d, want a 5xx gateway error", status)
		}
		heldDone <- time.Now()
	}()

	select {
	case <-heldArrived:
	case <-time.After(5 * time.Second):
		t.Fatal("held request never reached the backend")
	}

	start := time.Now()
	type fastResult struct {
		finished time.Time
		err      error
	}
	results := make(chan fastResult, 99)
	for i := 0; i < 99; i++ {
		token := fmt.Sprintf("fast-%d", i)
		go func() {
			status, body, err := tokenGet(t, proxySrv.Addr(), "/ok", token)
			if err == nil && (status != http.StatusOK || body != "token="+token) {
				err = fmt.Errorf("status=%d body=%q", status, body)
			}
			results <- fastResult{finished: time.Now(), err: err}
		}()
	}

	withinOneSecond := 0
	var latestFast time.Time
	for i := 0; i < 99; i++ {
		r := <-results
		if r.err != nil {
			t.Errorf("fast request failed behind held request: %v", r.err)
			continue
		}
		if r.finished.Sub(start) <= time.Second {
			withinOneSecond++
		}
		if r.finished.After(latestFast) {
			latestFast = r.finished
		}
	}
	if withinOneSecond < 95 {
		t.Errorf("only %d/99 fast requests finished within 1s of start", withinOneSecond)
	}

	heldFinished := <-heldDone
	if elapsed := heldFinished.Sub(start); elapsed < 4*time.Second {
		t.Errorf("held request finished after %v, want the full five-second timeout", elapsed)
	}
	if !latestFast.Before(heldFinished) {
		t.Errorf("fast requests finished at %v, after the held request at %v", latestFast, heldFinished)
	}
}

// TestClientDisconnectStages disconnects clients before parsing, during
// request-body forwarding, while awaiting response headers, and during
// response-body relay; a concurrent unrelated request must succeed during
// each stage and the proxy must stay available afterward (FR-026).
func TestClientDisconnectStages(t *testing.T) {
	heldArrived := make(chan struct{}, 4)
	backendSrv := startMixedBackend(t, heldArrived)
	proxySrv := startProxy(t, backendSrv.URL)

	assertUnrelatedSucceeds := func(stage string) {
		t.Helper()
		status, body, err := tokenGet(t, proxySrv.Addr(), "/ok", stage)
		if err != nil || status != http.StatusOK || body != "token="+stage {
			t.Fatalf("unrelated request during %s: status=%d body=%q err=%v", stage, status, body, err)
		}
	}

	dial := func() net.Conn {
		t.Helper()
		conn, err := net.DialTimeout("tcp", proxySrv.Addr(), 5*time.Second)
		if err != nil {
			t.Fatalf("dial proxy: %v", err)
		}
		return conn
	}

	t.Run("before parsing", func(t *testing.T) {
		conn := dial()
		io.WriteString(conn, "GET /nev") // partial request line, then vanish
		conn.Close()
		assertUnrelatedSucceeds("before-parsing")
	})

	t.Run("during request body forwarding", func(t *testing.T) {
		conn := dial()
		io.WriteString(conn, "POST /echo HTTP/1.1\r\nHost: dc.test\r\nContent-Length: 4096\r\n\r\npartial-body")
		time.Sleep(50 * time.Millisecond)
		conn.Close()
		assertUnrelatedSucceeds("during-body")
	})

	t.Run("while awaiting response headers", func(t *testing.T) {
		conn := dial()
		io.WriteString(conn, "GET /held HTTP/1.1\r\nHost: dc.test\r\n\r\n")
		select {
		case <-heldArrived:
		case <-time.After(5 * time.Second):
			t.Fatal("held request never reached the backend")
		}
		conn.Close()
		assertUnrelatedSucceeds("awaiting-headers")
	})

	t.Run("during response body relay", func(t *testing.T) {
		conn := dial()
		io.WriteString(conn, "GET /stream HTTP/1.1\r\nHost: dc.test\r\n\r\n")
		// Read part of the streamed response, then vanish mid-relay.
		buf := make([]byte, 512)
		if _, err := io.ReadFull(conn, buf); err != nil {
			t.Fatalf("read start of streamed response: %v", err)
		}
		conn.Close()
		assertUnrelatedSucceeds("during-relay")
	})

	status, body, err := tokenGet(t, proxySrv.Addr(), "/ok", "after-disconnects")
	if err != nil || status != http.StatusOK || body != "token=after-disconnects" {
		t.Fatalf("proxy unavailable after disconnect stages: status=%d body=%q err=%v", status, body, err)
	}
}

// TestResponseReordering expects a later fast request to complete before an
// earlier slow request, and each client to receive only its own response.
func TestResponseReordering(t *testing.T) {
	backendSrv := startMixedBackend(t, nil)
	proxySrv := startProxy(t, backendSrv.URL)

	slowDone := make(chan time.Time, 1)
	go func() {
		status, body, err := tokenGet(t, proxySrv.Addr(), "/slow-response", "reorder-slow")
		if err != nil || status != http.StatusOK || body != "slow=reorder-slow" {
			t.Errorf("slow request: status=%d body=%q err=%v", status, body, err)
		}
		slowDone <- time.Now()
	}()

	// Give the slow request time to reach the backend first.
	time.Sleep(100 * time.Millisecond)
	status, body, err := tokenGet(t, proxySrv.Addr(), "/ok", "reorder-fast")
	fastDone := time.Now()
	if err != nil || status != http.StatusOK || body != "token=reorder-fast" {
		t.Fatalf("fast request: status=%d body=%q err=%v", status, body, err)
	}
	if slowFinished := <-slowDone; !fastDone.Before(slowFinished) {
		t.Fatalf("fast request finished at %v, after slow request at %v", fastDone, slowFinished)
	}
}

// TestAbove100ConnectionsIntegrity runs 150 simultaneous clients and expects
// no crash, no corrupted or cross-contaminated responses, at least 100
// successes, and continued availability (FR-008, FR-027).
func TestAbove100ConnectionsIntegrity(t *testing.T) {
	backendSrv := startMixedBackend(t, nil)
	proxySrv := startProxy(t, backendSrv.URL)

	const clients = 150
	var wg sync.WaitGroup
	var successes atomic.Int64
	mismatches := make(chan string, clients)
	release := make(chan struct{})
	for i := 0; i < clients; i++ {
		token := fmt.Sprintf("above-%d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-release
			status, body, err := tokenGet(t, proxySrv.Addr(), "/ok", token)
			if err != nil {
				return // above 100, refusal is allowed but corruption is not
			}
			if status != http.StatusOK || body != "token="+token {
				mismatches <- fmt.Sprintf("token %s got status=%d body=%q", token, status, body)
				return
			}
			successes.Add(1)
		}()
	}
	close(release)
	wg.Wait()
	close(mismatches)
	for m := range mismatches {
		t.Errorf("corrupted response above 100 connections: %s", m)
	}
	if got := successes.Load(); got < 100 {
		t.Errorf("only %d/%d simultaneous requests succeeded, want at least 100", got, clients)
	}

	status, body, err := tokenGet(t, proxySrv.Addr(), "/ok", "after-above100")
	if err != nil || status != http.StatusOK || body != "token=after-above100" {
		t.Fatalf("proxy unavailable after 150-connection burst: status=%d body=%q err=%v", status, body, err)
	}
}

// TestPerClientDataIsolation posts a unique body from each of 100 concurrent
// clients and expects every client to receive exactly its own echo (FR-009).
func TestPerClientDataIsolation(t *testing.T) {
	backendSrv := startMixedBackend(t, nil)
	proxySrv := startProxy(t, backendSrv.URL)

	const clients = 100
	var wg sync.WaitGroup
	errs := make(chan error, clients)
	for i := 0; i < clients; i++ {
		payload := fmt.Sprintf("isolated-payload-%03d-%s", i, strings.Repeat(fmt.Sprintf("%02x", i), 64))
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := oneShotClient().Post(
				"http://"+proxySrv.Addr()+"/echo", "text/plain", strings.NewReader(payload))
			if err != nil {
				errs <- fmt.Errorf("post %q: %w", payload[:20], err)
				return
			}
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				errs <- fmt.Errorf("read echo for %q: %w", payload[:20], err)
				return
			}
			if string(body) != payload {
				errs <- fmt.Errorf("client received foreign data: want %q, got %q", payload[:20], string(body))
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// TestOneResponsePerConnection expects the proxy to answer exactly one
// request per client connection, advertise closure, and close the socket.
func TestOneResponsePerConnection(t *testing.T) {
	backend := &echoBackend{respStatus: http.StatusOK, respBody: []byte("only-once")}
	backendSrv := backend.start(t)
	proxySrv := startProxy(t, backendSrv.URL)

	oneRequest := "GET / HTTP/1.1\r\nHost: once.test\r\n\r\n"
	resp := rawHTTPExchange(t, proxySrv.Addr(), oneRequest+oneRequest)
	if got := strings.Count(resp, "HTTP/1.1 "); got != 1 {
		t.Fatalf("connection produced %d responses, want exactly 1:\n%s", got, resp)
	}
	if !strings.HasPrefix(resp, "HTTP/1.1 200") {
		t.Fatalf("first response not relayed:\n%s", resp)
	}
	if !strings.Contains(strings.ToLower(resp), "connection: close") {
		t.Fatalf("response does not advertise connection closure:\n%s", resp)
	}
	if recorded := backend.recorded(t); len(recorded) != 1 {
		t.Fatalf("backend received %d requests, want 1", len(recorded))
	}
}

// assertGeneratedError verifies the exact generated error contract: status
// line, exact plain-text body, content metadata, no leaked internals, and
// connection closure (FR-014, FR-015, FR-028).
func assertGeneratedError(t *testing.T, raw string, status int, statusText, body string) {
	t.Helper()
	wantLine := fmt.Sprintf("HTTP/1.1 %d %s", status, statusText)
	if !strings.HasPrefix(raw, wantLine) {
		t.Fatalf("status line does not start with %q:\n%s", wantLine, raw)
	}
	head, gotBody, found := strings.Cut(raw, "\r\n\r\n")
	if !found {
		t.Fatalf("response has no header terminator:\n%s", raw)
	}
	if gotBody != body {
		t.Fatalf("body = %q, want exactly %q", gotBody, body)
	}
	lowerHead := strings.ToLower(head)
	if !strings.Contains(lowerHead, "content-type: text/plain; charset=utf-8") {
		t.Fatalf("missing plain-text content type:\n%s", head)
	}
	if !strings.Contains(lowerHead, fmt.Sprintf("content-length: %d", len(body))) {
		t.Fatalf("missing exact content length %d:\n%s", len(body), head)
	}
	for _, leak := range []string{"refused", "dial", "tcp", "EOF", "127.0.0.1", "timeout awaiting"} {
		if strings.Contains(gotBody, leak) {
			t.Fatalf("generated body leaks internals %q: %q", leak, gotBody)
		}
	}
}

// assertOutcomeClass waits for one error outcome record with the given
// stable class and, when non-zero, the given status field.
func assertOutcomeClass(t *testing.T, out *logBuffer, class string, status int) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		for _, rec := range recordsWithEvent(out.String(), "error") {
			if recordField(t, rec, "class") != class {
				continue
			}
			if status != 0 && recordField(t, rec, "status") != fmt.Sprintf("%d", status) {
				continue
			}
			return rec
		}
		if time.Now().After(deadline) {
			t.Fatalf("no error record with class=%s status=%d; logs:\n%s", class, status, out.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestBackendConnectionRefusedProducesBadGateway expects a refused backend
// connection to produce the exact generated 502 with a stable
// backend_connection failure class and correlated request ID (FR-014).
func TestBackendConnectionRefusedProducesBadGateway(t *testing.T) {
	// Reserve an address, then close it so connections are refused.
	ln := newEphemeralListener(t)
	backendAddr := ln.Addr().String()
	ln.Close()
	proxySrv, out := startObservedProxy(t, "http://"+backendAddr)

	start := time.Now()
	raw := rawHTTPExchange(t, proxySrv.Addr(), "GET / HTTP/1.1\r\nHost: refused.test\r\n\r\n")
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("502 arrived after %v, want within 5s", elapsed)
	}
	assertGeneratedError(t, raw, http.StatusBadGateway, "Bad Gateway", "Bad Gateway\n")

	rec := assertOutcomeClass(t, out, "backend_connection", http.StatusBadGateway)
	requests := recordsWithEvent(out.String(), "request")
	if len(requests) != 1 {
		t.Fatalf("got %d request records, want 1; logs:\n%s", len(requests), out.String())
	}
	if recordField(t, rec, "id") != recordField(t, requests[0], "id") {
		t.Fatalf("outcome ID does not match request ID:\n%q\n%q", requests[0], rec)
	}
}

// TestBackendEarlyCloseProducesBadGateway expects a backend that closes
// before sending complete response headers to produce the exact 502 with a
// backend_response class (FR-014).
func TestBackendEarlyCloseProducesBadGateway(t *testing.T) {
	ln := startScriptedBackend(t, "") // reads the request, then closes
	proxySrv, out := startObservedProxy(t, "http://"+ln.Addr().String())

	raw := rawHTTPExchange(t, proxySrv.Addr(), "GET / HTTP/1.1\r\nHost: early.test\r\n\r\n")
	assertGeneratedError(t, raw, http.StatusBadGateway, "Bad Gateway", "Bad Gateway\n")
	assertOutcomeClass(t, out, "backend_response", http.StatusBadGateway)
}

// TestMalformedBackendResponseProducesBadGateway expects a syntactically
// invalid backend status line to produce the exact 502 with a
// backend_response class (FR-014).
func TestMalformedBackendResponseProducesBadGateway(t *testing.T) {
	ln := startScriptedBackend(t, "BOGUS/9.9 banana\r\n\r\n")
	proxySrv, out := startObservedProxy(t, "http://"+ln.Addr().String())

	raw := rawHTTPExchange(t, proxySrv.Addr(), "GET / HTTP/1.1\r\nHost: malformed.test\r\n\r\n")
	assertGeneratedError(t, raw, http.StatusBadGateway, "Bad Gateway", "Bad Gateway\n")
	assertOutcomeClass(t, out, "backend_response", http.StatusBadGateway)
}

// TestSilentBackendProducesGatewayTimeout expects a connected backend that
// sends no response headers to produce the exact 504 five seconds after the
// request is fully sent, classified as backend_timeout (FR-015, SC-005).
func TestSilentBackendProducesGatewayTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("five-second timeout scenario skipped in short mode")
	}
	ln := startSilentBackend(t)
	proxySrv, out := startObservedProxy(t, "http://"+ln.Addr().String())

	start := time.Now()
	raw := rawHTTPExchange(t, proxySrv.Addr(), "GET / HTTP/1.1\r\nHost: silent.test\r\n\r\n")
	elapsed := time.Since(start)
	if elapsed < 4500*time.Millisecond || elapsed > 8*time.Second {
		t.Fatalf("504 arrived after %v, want the five-second response-header deadline", elapsed)
	}
	assertGeneratedError(t, raw, http.StatusGatewayTimeout, "Gateway Timeout", "Gateway Timeout\n")

	rec := assertOutcomeClass(t, out, "backend_timeout", http.StatusGatewayTimeout)
	requests := recordsWithEvent(out.String(), "request")
	if len(requests) != 1 {
		t.Fatalf("got %d request records, want 1; logs:\n%s", len(requests), out.String())
	}
	if recordField(t, rec, "id") != recordField(t, requests[0], "id") {
		t.Fatalf("outcome ID does not match request ID:\n%q\n%q", requests[0], rec)
	}
}

// TestGatewayErrorsKeepServiceAvailable expects gateway failures to leave
// the proxy fully available to later requests against a healthy backend
// (FR-010).
func TestGatewayErrorsKeepServiceAvailable(t *testing.T) {
	backend := &echoBackend{respBody: []byte("still-alive")}
	backendSrv := backend.start(t)
	proxySrv, out := startObservedProxy(t, backendSrv.URL)

	// A proxy whose backend refuses connections keeps answering with 502.
	refusedLn := newEphemeralListener(t)
	refusedAddr := refusedLn.Addr().String()
	refusedLn.Close()
	refusedProxy, refusedOut := startObservedProxy(t, "http://"+refusedAddr)
	raw := rawHTTPExchange(t, refusedProxy.Addr(), "GET / HTTP/1.1\r\nHost: down.test\r\n\r\n")
	assertGeneratedError(t, raw, http.StatusBadGateway, "Bad Gateway", "Bad Gateway\n")
	assertOutcomeClass(t, refusedOut, "backend_connection", http.StatusBadGateway)

	// The healthy proxy still serves, and the failed proxy still answers.
	resp, err := oneShotClient().Get("http://" + proxySrv.Addr() + "/")
	if err != nil {
		t.Fatalf("healthy proxy unavailable: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || string(body) != "still-alive" {
		t.Fatalf("healthy relay status=%d body=%q", resp.StatusCode, body)
	}
	raw = rawHTTPExchange(t, refusedProxy.Addr(), "GET / HTTP/1.1\r\nHost: down.test\r\n\r\n")
	assertGeneratedError(t, raw, http.StatusBadGateway, "Bad Gateway", "Bad Gateway\n")
	if got := len(recordsWithEvent(out.String(), "error")); got != 0 {
		t.Fatalf("healthy proxy logged %d unrelated error records; logs:\n%s", got, out.String())
	}
}
