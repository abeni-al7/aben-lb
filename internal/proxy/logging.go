package proxy

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync/atomic"
)

// logHeaderAllowlist names the only request headers whose values may appear
// in diagnostics; every other value is replaced by redactedValue (FR-011).
var logHeaderAllowlist = map[string]bool{
	"Host":           true,
	"User-Agent":     true,
	"Accept":         true,
	"Content-Type":   true,
	"Content-Length": true,
}

const redactedValue = "[REDACTED]"

// requestIDFallback disambiguates IDs if the random source ever fails.
var requestIDFallback atomic.Uint64

// newRequestID returns a unique opaque correlation ID for one accepted
// request.
func newRequestID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("fallback-%d", requestIDFallback.Add(1))
	}
	return hex.EncodeToString(b[:])
}

// redactTarget renders a request target with only root-path and query
// presence visible: "/" stays verbatim, any other path becomes /[REDACTED],
// and a present query appears only as ?[REDACTED] (FR-011).
func redactTarget(u *url.URL) string {
	target := "/"
	if u.Path != "/" {
		target = "/" + redactedValue
	}
	if u.RawQuery != "" || u.ForceQuery {
		target += "?" + redactedValue
	}
	return target
}

// formatRequestHeaders renders the request headers in deterministic
// canonical-name order with allowlisted values visible and every other
// value redacted; bodies are never rendered (FR-011, FR-012).
func formatRequestHeaders(host string, header http.Header) string {
	names := make([]string, 0, len(header)+1)
	for name := range header {
		names = append(names, name)
	}
	if host != "" {
		names = append(names, "Host")
	}
	sort.Strings(names)

	parts := make([]string, 0, len(names))
	for _, name := range names {
		values := header[name]
		if name == "Host" {
			values = []string{host}
		}
		rendered := make([]string, len(values))
		for i, v := range values {
			if logHeaderAllowlist[name] {
				rendered[i] = v
			} else {
				rendered[i] = redactedValue
			}
		}
		parts = append(parts, name+": "+strings.Join(rendered, ", "))
	}
	return strings.Join(parts, "; ")
}

// formatRequestRecord renders the accepted-request activity record with
// stable field order for one request (FR-011).
func formatRequestRecord(id string, r *http.Request) string {
	return fmt.Sprintf("event=request id=%s client=%s method=%s target=%s headers=%q",
		id, r.RemoteAddr, r.Method, redactTarget(r.URL), formatRequestHeaders(r.Host, r.Header))
}
