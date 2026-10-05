package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/roshankarande/waku/apps/desktop-go/internal/daemon"
	"github.com/roshankarande/waku/apps/desktop-go/internal/protocol"
	"github.com/roshankarande/waku/apps/desktop-go/internal/storage"
)

type stringList []string

func (s *stringList) String() string         { return strings.Join(*s, ",") }
func (s *stringList) Set(value string) error { *s = append(*s, value); return nil }

func run() error {
	if len(os.Args) == 2 && os.Args[1] == "--waku-release-role-probe" {
		return writeReleaseRoleProbe(os.Stdout)
	}
	var address, dataDir, database string
	var origins stringList
	var cutover, allowShutdown bool
	flag.StringVar(&address, "listen", "127.0.0.1:0", "IP socket address for the daemon")
	flag.StringVar(&dataDir, "data-dir", "", "data directory (defaults to isolated Go development data)")
	flag.StringVar(&database, "database", "", "SQLite path directly inside data-dir")
	flag.Var(&origins, "allowed-origin", "exact browser Origin allowed to connect (repeatable)")
	flag.BoolVar(&cutover, "cutover", false, "reserved for externally supervised legacy-to-Go cutover (currently refused)")
	flag.BoolVar(&allowShutdown, "allow-client-shutdown", false, "allow an authenticated client to stop the daemon")
	flag.Parse()
	if err := validateCutover(cutover); err != nil {
		return err
	}
	if strings.TrimSpace(os.Getenv(protocol.DaemonTokenEnv)) == "" {
		return fmt.Errorf("%s is required", protocol.DaemonTokenEnv)
	}
	if dataDir == "" {
		var err error
		dataDir, err = storage.DevelopmentDataDir()
		if err != nil {
			return err
		}
	}
	absDir, err := filepath.Abs(dataDir)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	server, err := daemon.NewServer(ctx, daemon.ServerConfig{DataDir: absDir, DatabasePath: database, Token: os.Getenv(protocol.DaemonTokenEnv), AllowedOrigins: origins, AllowShutdown: allowShutdown, DaemonVersion: "waku-go-dev"})
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		server.Close()
		return err
	}
	if err := daemon.WriteReadiness(os.Stdout, listener); err != nil {
		listener.Close()
		server.Close()
		return err
	}
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	select {
	case err := <-served:
		_ = server.Close()
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return server.Shutdown(shutdown)
	}
}

func writeReleaseRoleProbe(writer io.Writer) error {
	return json.NewEncoder(writer).Encode(struct {
		SchemaVersion int    `json:"schemaVersion"`
		Protocol      string `json:"protocol"`
		Role          string `json:"role"`
	}{SchemaVersion: 1, Protocol: "waku-release-role-v1", Role: "daemon"})
}

func validateCutover(requested bool) error {
	if requested {
		return errors.New("--cutover is disabled: no verifiable external supervisor proof that the legacy daemon is stopped was supplied")
	}
	return nil
}

func main() {
	if err := run(); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, "waku-go-daemon:", err)
		os.Exit(1)
	}
}
