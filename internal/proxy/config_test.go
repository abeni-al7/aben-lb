package proxy

import "testing"

func TestNewConfigListenValidation(t *testing.T) {
	tests := []struct {
		name    string
		listen  string
		wantErr bool
	}{
		{"loopback host and port", "127.0.0.1:8080", false},
		{"wildcard host with port", ":8080", false},
		{"os assigned port", "127.0.0.1:0", false},
		{"missing address", "", true},
		{"host without port", "127.0.0.1", true},
		{"not an address", "not-an-address", true},
		{"empty port", "127.0.0.1:", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := NewConfig(tt.listen, "http://127.0.0.1:9000")
			if tt.wantErr {
				if err == nil {
					t.Fatalf("NewConfig(%q, ...) succeeded; want listen validation error", tt.listen)
				}
				return
			}
			if err != nil {
				t.Fatalf("NewConfig(%q, ...) failed: %v", tt.listen, err)
			}
			if cfg.ListenAddr != tt.listen {
				t.Fatalf("ListenAddr = %q, want %q", cfg.ListenAddr, tt.listen)
			}
		})
	}
}

func TestNewConfigBackendValidation(t *testing.T) {
	tests := []struct {
		name    string
		backend string
		wantErr bool
	}{
		{"origin only", "http://127.0.0.1:9000", false},
		{"origin with root path", "http://127.0.0.1:9000/", false},
		{"missing", "", true},
		{"relative host and port", "127.0.0.1:9000", true},
		{"relative path", "/backend", true},
		{"https scheme", "https://127.0.0.1:9000", true},
		{"credentials", "http://user:secret@127.0.0.1:9000", true},
		{"path prefix", "http://127.0.0.1:9000/api", true},
		{"query", "http://127.0.0.1:9000?token=1", true},
		{"empty query marker", "http://127.0.0.1:9000?", true},
		{"fragment", "http://127.0.0.1:9000#section", true},
		{"missing port", "http://127.0.0.1", true},
		{"missing host", "http://:9000", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := NewConfig("127.0.0.1:0", tt.backend)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("NewConfig(_, %q) succeeded; want backend validation error", tt.backend)
				}
				return
			}
			if err != nil {
				t.Fatalf("NewConfig(_, %q) failed: %v", tt.backend, err)
			}
			if got := cfg.Backends[0].String(); got != "http://127.0.0.1:9000" {
				t.Fatalf("Backends[0] = %q, want normalized %q", got, "http://127.0.0.1:9000")
			}
			if cfg.Backends[0].Path != "" || cfg.Backends[0].RawQuery != "" || cfg.Backends[0].Fragment != "" || cfg.Backends[0].User != nil {
				t.Fatalf("Backends[0] %#v retains non-origin components", cfg.Backends[0])
			}
		})
	}
}

func TestNewConfigMultipleBackends(t *testing.T) {
	cfg, err := NewConfig("127.0.0.1:0", "http://127.0.0.1:8080, http://127.0.0.1:8081/")
	if err != nil {
		t.Fatalf("NewConfig with two backends failed: %v", err)
	}
	want := []string{"http://127.0.0.1:8080", "http://127.0.0.1:8081"}
	if len(cfg.Backends) != len(want) {
		t.Fatalf("got %d backends, want %d", len(cfg.Backends), len(want))
	}
	for i, w := range want {
		if got := cfg.Backends[i].String(); got != w {
			t.Fatalf("Backends[%d] = %q, want %q", i, got, w)
		}
	}

	// One bad entry rejects the whole list, including an empty entry.
	for _, bad := range []string{"http://127.0.0.1:8080,https://127.0.0.1:8081", "http://127.0.0.1:8080,"} {
		if _, err := NewConfig("127.0.0.1:0", bad); err == nil {
			t.Fatalf("NewConfig(_, %q) succeeded; want validation error", bad)
		}
	}
}
