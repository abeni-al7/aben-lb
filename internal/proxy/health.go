package proxy

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// healthCheckTimeout bounds a single health check request, so one hung
// backend cannot stall the checks of the others.
const healthCheckTimeout = 5 * time.Second

// startHealthChecks runs the health checker in the background until
// stopHealthChecks is called.
func (s *Server) startHealthChecks() {
	ctx, cancel := context.WithCancel(context.Background())
	s.stopHealth = cancel
	s.healthDone = make(chan struct{})
	go func() {
		defer close(s.healthDone)
		s.runHealthChecks(ctx)
	}()
}

// stopHealthChecks cancels any in-flight checks and waits for the checker
// goroutine to exit, so no goroutine outlives the server.
func (s *Server) stopHealthChecks() {
	if s.stopHealth == nil {
		return
	}
	s.stopHealth()
	<-s.healthDone
}

// runHealthChecks checks every backend once per interval until ctx is
// cancelled. Backends start out healthy; the first check runs after one
// interval.
func (s *Server) runHealthChecks(ctx context.Context) {
	client := &http.Client{
		Timeout: min(s.cfg.HealthInterval, healthCheckTimeout),
		// A fresh connection per check proves the backend still accepts
		// new connections, and leaves no idle connections behind.
		Transport: &http.Transport{DisableKeepAlives: true},
		// A redirect is not a 200, so it must not be followed.
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	ticker := time.NewTicker(s.cfg.HealthInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Check all backends at the same time, so the round takes as
			// long as the slowest backend rather than the sum of them.
			var wg sync.WaitGroup
			for i, backend := range s.cfg.Backends {
				wg.Go(func() {
					s.setHealthy(i, checkBackend(ctx, client, backend, s.cfg.HealthPath))
				})
			}
			wg.Wait()
		}
	}
}

// checkBackend reports whether a GET of path on backend returns 200.
// Any other status, or no response at all, is unhealthy.
func checkBackend(ctx context.Context, client *http.Client, backend *url.URL, path string) bool {
	target := backend.Scheme + "://" + backend.Host + path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return false
	}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	return resp.StatusCode == http.StatusOK
}

// setHealthy records a backend's latest health and logs only changes, so
// the log shows exactly when a backend left or rejoined the rotation.
func (s *Server) setHealthy(i int, healthy bool) {
	if s.healthy[i].Swap(healthy) == healthy {
		return
	}
	state := "down"
	if healthy {
		state = "up"
	}
	s.logf("event=health backend=%s state=%s", s.cfg.Backends[i].Host, state)
}

// nextBackend picks the backend for a new request using round robin over
// the backends that are currently healthy. It reports false when none are.
func (s *Server) nextBackend() (*url.URL, bool) {
	healthy := make([]*url.URL, 0, len(s.cfg.Backends))
	for i, b := range s.cfg.Backends {
		if s.healthy[i].Load() {
			healthy = append(healthy, b)
		}
	}
	if len(healthy) == 0 {
		return nil, false
	}
	n := s.next.Add(1) - 1
	return healthy[n%uint64(len(healthy))], true
}
