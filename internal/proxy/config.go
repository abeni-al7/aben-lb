// Package proxy implements configuration, logging, health checking, and
// forwarding for the round-robin HTTP/1.1 load balancer.
package proxy

import (
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"
)

// Default health check settings, used unless SetHealthCheck overrides them.
const (
	DefaultHealthInterval = 10 * time.Second
	DefaultHealthPath     = "/"
	// DefaultKeepAliveTimeout is how long an idle client connection is kept
	// open waiting for its next request.
	DefaultKeepAliveTimeout = 30 * time.Second
)

// Config is the immutable, validated startup configuration for the proxy.
type Config struct {
	// ListenAddr is the TCP address accepting client connections.
	ListenAddr string
	// Backends are the cleartext HTTP origins that requests are spread
	// across in round-robin order. There is always at least one.
	Backends []*url.URL
	// HealthInterval is how often every backend is health checked.
	HealthInterval time.Duration
	// HealthPath is the path requested with GET on each backend; only a
	// 200 response counts as healthy.
	HealthPath string
	// KeepAliveTimeout is how long an idle client connection stays open for
	// another request. Zero disables client keep-alive: every connection
	// then carries exactly one request and closes.
	KeepAliveTimeout time.Duration
}

// SetKeepAliveTimeout validates and applies the client keep-alive timeout.
func (c *Config) SetKeepAliveTimeout(d time.Duration) error {
	if d < 0 {
		return fmt.Errorf("keep-alive timeout must not be negative, got %v", d)
	}
	c.KeepAliveTimeout = d
	return nil
}

// SetHealthCheck validates and applies the health check period and path.
func (c *Config) SetHealthCheck(interval time.Duration, path string) error {
	if interval <= 0 {
		return fmt.Errorf("health check interval must be positive, got %v", interval)
	}
	if !strings.HasPrefix(path, "/") {
		return fmt.Errorf("health check path %q must start with /", path)
	}
	if _, err := url.ParseRequestURI(path); err != nil {
		return fmt.Errorf("health check path %q is not valid: %v", path, err)
	}
	c.HealthInterval = interval
	c.HealthPath = path
	return nil
}

// NewConfig validates the listen address and backend targets before any
// listener is opened. backends is a comma-separated list; each entry must be
// an absolute http:// origin with a host and port and no credentials, path,
// query, or fragment.
func NewConfig(listen, backends string) (*Config, error) {
	if err := validateListenAddr(listen); err != nil {
		return nil, err
	}
	if strings.TrimSpace(backends) == "" {
		return nil, fmt.Errorf("backend URL is required")
	}
	var urls []*url.URL
	for b := range strings.SplitSeq(backends, ",") {
		u, err := validateBackendURL(strings.TrimSpace(b))
		if err != nil {
			return nil, err
		}
		urls = append(urls, u)
	}
	return &Config{
		ListenAddr:     listen,
		Backends:       urls,
		HealthInterval: DefaultHealthInterval,
		HealthPath:     DefaultHealthPath,
		// Keep-alive is on by default, like most real load balancers.
		KeepAliveTimeout: DefaultKeepAliveTimeout,
	}, nil
}

func validateListenAddr(listen string) error {
	if listen == "" {
		return fmt.Errorf("listen address is required")
	}
	_, port, err := net.SplitHostPort(listen)
	if err != nil {
		return fmt.Errorf("listen address %q must be host:port: %v", listen, err)
	}
	if port == "" {
		return fmt.Errorf("listen address %q must include a port", listen)
	}
	return nil
}

func validateBackendURL(backend string) (*url.URL, error) {
	if backend == "" {
		return nil, fmt.Errorf("backend URL is required")
	}
	u, err := url.Parse(backend)
	if err != nil {
		return nil, fmt.Errorf("backend URL %q is not a valid URL: %v", backend, err)
	}
	if u.Scheme != "http" {
		return nil, fmt.Errorf("backend URL %q must use the http scheme", backend)
	}
	if u.User != nil {
		return nil, fmt.Errorf("backend URL must not contain credentials")
	}
	if u.Path != "" && u.Path != "/" {
		return nil, fmt.Errorf("backend URL %q must not contain a path", backend)
	}
	if u.RawQuery != "" || u.ForceQuery {
		return nil, fmt.Errorf("backend URL %q must not contain a query", backend)
	}
	if u.Fragment != "" {
		return nil, fmt.Errorf("backend URL %q must not contain a fragment", backend)
	}
	host, port, err := net.SplitHostPort(u.Host)
	if err != nil {
		return nil, fmt.Errorf("backend URL %q must contain host:port: %v", backend, err)
	}
	if host == "" || port == "" {
		return nil, fmt.Errorf("backend URL %q must contain a host and port", backend)
	}
	// Normalize to a pure origin so forwarding never inherits a base path.
	return &url.URL{Scheme: u.Scheme, Host: u.Host}, nil
}
