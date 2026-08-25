package tests

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// binDir holds the lb and be binaries built once by TestMain.
var binDir string

// TestMain builds the lb and be commands once so process-level tests run
// against real binaries without rebuilding per test.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "aben-lb-bin-")
	if err != nil {
		fmt.Fprintf(os.Stderr, "create binary directory: %v\n", err)
		os.Exit(1)
	}
	binDir = dir
	for _, name := range []string{"lb", "be"} {
		build := exec.Command("go", "build", "-o", filepath.Join(dir, name), "./cmd/"+name)
		build.Dir = ".." // repo root relative to tests/
		if out, err := build.CombinedOutput(); err != nil {
			fmt.Fprintf(os.Stderr, "build %s: %v\n%s", name, err, out)
			os.RemoveAll(dir)
			os.Exit(1)
		}
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// lockedBuffer captures process output safely while the process writes
// concurrently with test reads.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// process is a running lb or be command with captured standard output.
type process struct {
	t      testing.TB
	cmd    *exec.Cmd
	stdout *lockedBuffer
	stderr *lockedBuffer
	done   chan struct{}
	err    error
}

// startProcess starts a built command with args, captures its output, and
// guarantees the process is terminated when the test finishes.
func startProcess(t testing.TB, name string, args ...string) *process {
	t.Helper()
	cmd := exec.Command(filepath.Join(binDir, name), args...)
	stdout := &lockedBuffer{}
	stderr := &lockedBuffer{}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s %s: %v", name, strings.Join(args, " "), err)
	}
	p := &process{t: t, cmd: cmd, stdout: stdout, stderr: stderr, done: make(chan struct{})}
	go func() {
		p.err = cmd.Wait()
		close(p.done)
	}()
	t.Cleanup(p.kill)
	return p
}

// interrupt sends SIGINT to trigger graceful shutdown.
func (p *process) interrupt() {
	p.t.Helper()
	if err := p.cmd.Process.Signal(os.Interrupt); err != nil {
		p.t.Fatalf("interrupt process: %v", err)
	}
}

// await blocks until the process exits or timeout elapses and returns the
// exit error (nil for a zero exit status).
func (p *process) await(timeout time.Duration) error {
	p.t.Helper()
	select {
	case <-p.done:
		return p.err
	case <-time.After(timeout):
		p.t.Fatalf("process did not exit within %v; output:\n%s", timeout, p.output())
		return nil
	}
}

// kill force-terminates the process if it has not already exited; safe to
// call multiple times.
func (p *process) kill() {
	select {
	case <-p.done:
	default:
		p.cmd.Process.Kill()
		<-p.done
	}
}

// output returns everything the process has written to standard output.
func (p *process) output() string {
	return p.stdout.String()
}

// errorOutput returns everything the process has written to standard error.
func (p *process) errorOutput() string {
	return p.stderr.String()
}

// waitForOutput polls until substr appears on standard output, failing the
// test if it does not appear within timeout.
func (p *process) waitForOutput(substr string, timeout time.Duration) {
	p.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if strings.Contains(p.output(), substr) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	p.t.Fatalf("output did not contain %q within %v; output:\n%s", substr, timeout, p.output())
}

// freeLoopbackAddr reserves and releases an ephemeral loopback address so
// process tests never depend on fixed ports.
func freeLoopbackAddr(t testing.TB) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve ephemeral address: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

// TestProcessHelpersCleanUp proves the Phase 2 process fixtures build both
// commands, start and terminate them, and leave no running processes.
func TestProcessHelpersCleanUp(t *testing.T) {
	for _, name := range []string{"lb", "be"} {
		if _, err := os.Stat(filepath.Join(binDir, name)); err != nil {
			t.Fatalf("built binary %s missing: %v", name, err)
		}
	}

	addr := freeLoopbackAddr(t)
	if _, _, err := net.SplitHostPort(addr); err != nil {
		t.Fatalf("ephemeral address %q invalid: %v", addr, err)
	}

	p := startProcess(t, "be")
	p.kill()
	<-p.done
	if p.cmd.ProcessState == nil {
		t.Fatal("process still running after kill")
	}
}
