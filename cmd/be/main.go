// Command be runs the deterministic test backend server.
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/abeni-al7/aben-lb/internal/testbackend"
)

// drainTimeout bounds graceful shutdown of active requests.
const drainTimeout = 5 * time.Second

func main() {
	os.Exit(run())
}

func run() int {
	listen := flag.String("listen", "", "TCP address (host:port) accepting test HTTP connections")
	flag.Parse()

	if err := validateListen(*listen); err != nil {
		fmt.Fprintf(os.Stderr, "be: invalid configuration: %v\n", err)
		return 2
	}

	srv := testbackend.New(*listen, os.Stdout)
	if err := srv.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "be: startup failed: %v\n", err)
		return 1
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-srv.Done():
		if err != nil {
			fmt.Fprintf(os.Stderr, "be: serving failed: %v\n", err)
			return 1
		}
		return 0
	case <-sig:
		ctx, cancel := context.WithTimeout(context.Background(), drainTimeout)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "be: shutdown incomplete: %v\n", err)
		}
		<-srv.Done()
		return 0
	}
}

func validateListen(listen string) error {
	if listen == "" {
		return fmt.Errorf("-listen is required")
	}
	if _, port, err := net.SplitHostPort(listen); err != nil {
		return fmt.Errorf("-listen %q must be host:port: %v", listen, err)
	} else if port == "" {
		return fmt.Errorf("-listen %q must include a port", listen)
	}
	return nil
}
