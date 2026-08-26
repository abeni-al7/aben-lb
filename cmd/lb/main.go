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
	backend := flag.String("backend", "", "absolute cleartext HTTP origin (http://host:port) receiving every request")
	flag.Parse()

	cfg, err := proxy.NewConfig(*listen, *backend)
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
