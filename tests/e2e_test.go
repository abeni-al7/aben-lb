// Package tests contains process-level end-to-end and performance tests for
// the lb and be commands.
package tests

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestForwardSingleRequestEndToEnd runs the built be and lb binaries, sends
// one request to /, and expects the exact backend response with no diagnostic
// text mixed into the HTTP body.
func TestForwardSingleRequestEndToEnd(t *testing.T) {
	beAddr := freeLoopbackAddr(t)
	be := startProcess(t, "be", "-listen", beAddr)
	be.waitForOutput(beAddr, 5*time.Second)

	lbAddr := freeLoopbackAddr(t)
	lb := startProcess(t, "lb", "-listen", lbAddr, "-backend", "http://"+beAddr)
	lb.waitForOutput(lbAddr, 5*time.Second)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get("http://" + lbAddr + "/")
	if err != nil {
		t.Fatalf("request through lb: %v\nlb output:\n%s\nbe output:\n%s", err, lb.output(), be.output())
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("read relayed body: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got := string(body); got != "Hello From Backend Server" {
		t.Fatalf("body = %q, want exactly %q with no diagnostic text", got, "Hello From Backend Server")
	}

	// Both commands stay independently runnable after serving the request.
	select {
	case <-be.done:
		t.Fatalf("be exited unexpectedly: %v\n%s", be.err, be.output())
	case <-lb.done:
		t.Fatalf("lb exited unexpectedly: %v\n%s", lb.err, lb.output())
	default:
	}
}

// rawExchange dials addr, writes payload, half-closes the write side when
// halfClose is set, and returns everything read until the server closes.
func rawExchange(t *testing.T, addr, payload string, halfClose bool) string {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	if _, err := io.WriteString(conn, payload); err != nil {
		t.Fatalf("write raw payload: %v", err)
	}
	if halfClose {
		if err := conn.(*net.TCPConn).CloseWrite(); err != nil {
			t.Fatalf("half-close connection: %v", err)
		}
	}
	data, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("read raw response: %v", err)
	}
	return string(data)
}

// assertExactBadRequest verifies the exact generated 400 contract: status
// line, plain-text content metadata, exact body, and closure (FR-019,
// FR-028).
func assertExactBadRequest(t *testing.T, raw string) {
	t.Helper()
	const body = "Bad Request\n"
	if !strings.HasPrefix(raw, "HTTP/1.1 400 Bad Request") {
		t.Fatalf("response is not the generated 400:\n%q", raw)
	}
	head, gotBody, found := strings.Cut(raw, "\r\n\r\n")
	if !found {
		t.Fatalf("400 response has no header terminator:\n%q", raw)
	}
	if gotBody != body {
		t.Fatalf("400 body = %q, want exactly %q", gotBody, body)
	}
	lowerHead := strings.ToLower(head)
	if !strings.Contains(lowerHead, "content-type: text/plain; charset=utf-8") {
		t.Fatalf("400 missing plain-text content type:\n%s", head)
	}
	if !strings.Contains(lowerHead, fmt.Sprintf("content-length: %d", len(body))) {
		t.Fatalf("400 missing exact content length %d:\n%s", len(body), head)
	}
}

// assertServiceStillForwards proves both processes remain available by
// completing a valid forwarded request after a failure scenario.
func assertServiceStillForwards(t *testing.T, lbAddr string) {
	t.Helper()
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get("http://" + lbAddr + "/")
	if err != nil {
		t.Fatalf("service unavailable after failure scenario: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil || resp.StatusCode != http.StatusOK || string(body) != "Hello From Backend Server" {
		t.Fatalf("follow-up request status=%d body=%q err=%v", resp.StatusCode, body, err)
	}
}

// waitForLogField polls lb standard output for a substring so failure
// records written after the socket closes are still observed.
func waitForLogField(t *testing.T, p *process, substr string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(p.output(), substr) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("lb output did not contain %q; output:\n%s", substr, p.output())
}

// TestMalformedRequestsEndToEnd sends invalid raw HTTP to the running lb
// binary and expects the exact generated 400 contract, invalid_request
// diagnostics, client-disconnect classification, preserved protocol-limit
// statuses, connection closure, and continued availability (FR-019, FR-028).
func TestMalformedRequestsEndToEnd(t *testing.T) {
	beAddr := freeLoopbackAddr(t)
	be := startProcess(t, "be", "-listen", beAddr)
	be.waitForOutput(beAddr, 5*time.Second)

	lbAddr := freeLoopbackAddr(t)
	lb := startProcess(t, "lb", "-listen", lbAddr, "-backend", "http://"+beAddr)
	lb.waitForOutput(lbAddr, 5*time.Second)

	t.Run("malformed request line", func(t *testing.T) {
		raw := rawExchange(t, lbAddr, "INVALID REQUEST\r\nHost: x\r\n\r\n", false)
		assertExactBadRequest(t, raw)
		waitForLogField(t, lb, "class=invalid_request")
		assertServiceStillForwards(t, lbAddr)
	})

	t.Run("malformed header syntax", func(t *testing.T) {
		raw := rawExchange(t, lbAddr, "GET / HTTP/1.1\r\nHost: x\r\nBroken-Header-Without-Colon\r\n\r\n", false)
		assertExactBadRequest(t, raw)
		assertServiceStillForwards(t, lbAddr)
	})

	t.Run("incomplete syntax while writable", func(t *testing.T) {
		raw := rawExchange(t, lbAddr, "GET / HTTP/1.1\r\nHost: x\r\n", true)
		assertExactBadRequest(t, raw)
		assertServiceStillForwards(t, lbAddr)
	})

	t.Run("client disconnect before parsing", func(t *testing.T) {
		conn, err := net.DialTimeout("tcp", lbAddr, 5*time.Second)
		if err != nil {
			t.Fatalf("dial lb: %v", err)
		}
		conn.Close()
		waitForLogField(t, lb, "class=client_disconnect")
		assertServiceStillForwards(t, lbAddr)
	})

	t.Run("protocol limit status preserved", func(t *testing.T) {
		conn, err := net.DialTimeout("tcp", lbAddr, 5*time.Second)
		if err != nil {
			t.Fatalf("dial lb: %v", err)
		}
		defer conn.Close()
		if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
			t.Fatalf("set deadline: %v", err)
		}
		// The server may answer and close while the oversized headers are
		// still being written, so write and read concurrently and tolerate
		// a reset once the response has arrived.
		go func() {
			io.WriteString(conn, "GET / HTTP/1.1\r\nHost: x\r\nX-Big: "+strings.Repeat("a", 2<<20)+"\r\n\r\n")
		}()
		buf := make([]byte, 4096)
		var raw string
		for {
			n, err := conn.Read(buf)
			raw += string(buf[:n])
			if err != nil || strings.Contains(raw, "\r\n") {
				break
			}
		}
		if !strings.HasPrefix(raw, "HTTP/1.1 431") {
			t.Fatalf("oversized headers reclassified; response:\n%.200q", raw)
		}
		assertServiceStillForwards(t, lbAddr)
	})

	// No malformed request may leak backend or network internals to stdout
	// HTTP responses, and both processes must still be running.
	select {
	case <-be.done:
		t.Fatalf("be exited during malformed-request handling: %v\n%s", be.err, be.output())
	case <-lb.done:
		t.Fatalf("lb exited during malformed-request handling: %v\n%s", lb.err, lb.output())
	default:
	}
}

// TestCrossStoryRegressionsEndToEnd exercises success, secret-bearing, and
// failure traffic against the real binaries, then interrupts both processes.
// It asserts no secret reaches either process's standard output, generated
// errors disclose no internals, every connection closes, and both processes
// drain and exit cleanly (cross-story regression for US1-US3).
func TestCrossStoryRegressionsEndToEnd(t *testing.T) {
	beAddr := freeLoopbackAddr(t)
	be := startProcess(t, "be", "-listen", beAddr)
	be.waitForOutput(beAddr, 5*time.Second)

	lbAddr := freeLoopbackAddr(t)
	lb := startProcess(t, "lb", "-listen", lbAddr, "-backend", "http://"+beAddr)
	lb.waitForOutput(lbAddr, 5*time.Second)

	// Secret-bearing success request through the running binaries.
	secrets := []string{"e2e-path-secret", "e2e-query-secret", "e2e-header-secret", "e2e-body-secret"}
	req, err := http.NewRequest(http.MethodPost,
		"http://"+lbAddr+"/e2e-path-secret?k=e2e-query-secret",
		strings.NewReader("e2e-body-secret"))
	if err != nil {
		t.Fatalf("build secret request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer e2e-header-secret")
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("secret-bearing request: %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	// Malformed request carrying a secret produces the exact 400 and closes.
	raw := rawExchange(t, lbAddr, "INVALID e2e-query-secret\r\nX-Key: e2e-header-secret\r\n\r\n", false)
	assertExactBadRequest(t, raw)

	// A proxy with a down backend generates a 502 without disclosure.
	downAddr := freeLoopbackAddr(t)
	lbDownAddr := freeLoopbackAddr(t)
	lbDown := startProcess(t, "lb", "-listen", lbDownAddr, "-backend", "http://"+downAddr)
	lbDown.waitForOutput(lbDownAddr, 5*time.Second)
	raw = rawExchange(t, lbDownAddr, "GET / HTTP/1.1\r\nHost: down.test\r\n\r\n", false)
	if !strings.HasPrefix(raw, "HTTP/1.1 502 Bad Gateway") {
		t.Fatalf("down-backend response is not the generated 502:\n%q", raw)
	}
	lowerRaw := strings.ToLower(raw)
	for _, leak := range []string{strings.ToLower(downAddr), "dial tcp", "connection refused", "goroutine"} {
		if strings.Contains(lowerRaw, leak) {
			t.Errorf("generated 502 discloses %q:\n%s", leak, raw)
		}
	}
	if !strings.Contains(lowerRaw, "connection: close") {
		t.Errorf("generated 502 does not advertise closure:\n%s", raw)
	}

	// One-response connection closure: a second pipelined request is ignored.
	oneRequest := "GET / HTTP/1.1\r\nHost: close.test\r\n\r\n"
	raw = rawExchange(t, lbAddr, oneRequest+oneRequest, false)
	if got := strings.Count(raw, "HTTP/1.1 "); got != 1 {
		t.Fatalf("connection produced %d responses, want exactly 1:\n%s", got, raw)
	}

	// Secrets never reach the load balancer's diagnostics. The test backend
	// intentionally logs full request details, so it is not asserted here.
	waitForLogField(t, lb, "class=invalid_request")
	for _, secret := range secrets {
		for _, p := range []struct {
			name string
			out  string
		}{{"lb", lb.output()}, {"lb-down", lbDown.output()}} {
			if strings.Contains(p.out, secret) {
				t.Errorf("secret %q leaked into %s stdout:\n%s", secret, p.name, p.out)
			}
		}
	}

	// Graceful interrupt: every process drains, logs shutdown, and exits 0.
	for _, p := range []struct {
		name string
		proc *process
	}{{"lb", lb}, {"lb-down", lbDown}, {"be", be}} {
		p.proc.interrupt()
		if err := p.proc.await(7 * time.Second); err != nil {
			t.Errorf("%s exited with error after interrupt: %v\nstderr:\n%s",
				p.name, err, p.proc.errorOutput())
		}
		if !strings.Contains(p.proc.output(), "event=shutdown") {
			t.Errorf("%s did not record shutdown; output:\n%s", p.name, p.proc.output())
		}
	}
}
