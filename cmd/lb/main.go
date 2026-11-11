// Command lb runs the single-backend HTTP/1.1 reverse proxy.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/abeni-al7/aben-lb/internal/proxy"
)

// drainTimeout bounds graceful shutdown of active requests.
const drainTimeout = 5 * time.Second

func main() {
	os.Exit(run())
}

func run() int {
	listen := flag.String("listen", "", "TCP address (host:port) accepting client HTTP connections")
	backend := flag.String("backend", "", "comma-separated cleartext HTTP origins (http://host:port,...) that requests are spread across in round-robin order")
	healthInterval := flag.Duration("health-interval", proxy.DefaultHealthInterval, "how often each backend is health checked (e.g. 10s, 500ms)")
	healthPath := flag.String("health-path", proxy.DefaultHealthPath, "path requested with GET on each backend; only a 200 response is healthy")
	keepAlive := flag.Duration("keepalive-timeout", proxy.DefaultKeepAliveTimeout, "how long an idle client connection stays open for another request; 0 closes every connection after one request")
	flag.Parse()

	cfg, err := proxy.NewConfig(*listen, *backend)
	if err == nil {
		err = cfg.SetHealthCheck(*healthInterval, *healthPath)
	}
	if err == nil {
		err = cfg.SetKeepAliveTimeout(*keepAlive)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "lb: invalid configuration: %v\n", err)
		return 2
	}

	srv := proxy.NewServer(cfg, os.Stdout)
	if err := srv.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "lb: startup failed: %v\n", err)
		return 1
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-srv.Done():
		if err != nil {
			fmt.Fprintf(os.Stderr, "lb: serving failed: %v\n", err)
			return 1
		}
		return 0
	case <-sig:
		ctx, cancel := context.WithTimeout(context.Background(), drainTimeout)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "lb: shutdown incomplete: %v\n", err)
		}
		<-srv.Done()
		return 0
	}
}
