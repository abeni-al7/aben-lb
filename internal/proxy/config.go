// Package proxy implements configuration, logging, and forwarding for the
// single-backend HTTP/1.1 reverse proxy.
package proxy

import (
	"fmt"
	"net"
	"net/url"
)

// Config is the immutable, validated startup configuration for the proxy.
type Config struct {
	// ListenAddr is the TCP address accepting client connections.
	ListenAddr string
	// BackendURL is the single cleartext HTTP origin receiving every request.
	BackendURL *url.URL
}

// NewConfig validates the listen address and backend target before any
// listener is opened. The backend must be an absolute http:// origin with a
// host and port and no credentials, path, query, or fragment.
func NewConfig(listen, backend string) (*Config, error) {
	if err := validateListenAddr(listen); err != nil {
		return nil, err
	}
	backendURL, err := validateBackendURL(backend)
	if err != nil {
		return nil, err
	}
	return &Config{ListenAddr: listen, BackendURL: backendURL}, nil
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
