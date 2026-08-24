package proxy

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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
