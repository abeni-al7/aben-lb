// Package tests contains process-level end-to-end and performance tests for
// the lb and be commands.
package tests

import (
	"io"
	"net/http"
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
