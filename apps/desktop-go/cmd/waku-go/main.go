// Command waku-go runs the native Go desktop against either a supervised
// local Go daemon or an explicitly managed external protocol-v7 endpoint.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"io"
	"log"
	"os"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/egoist/mygo"
	"github.com/egoist/mygo/ui"
	"github.com/roshankarande/waku/apps/desktop-go/internal/client"
	"github.com/roshankarande/waku/apps/desktop-go/internal/presentation"
	"github.com/roshankarande/waku/apps/desktop-go/internal/views"
)

type stringFlags []string

func (s *stringFlags) String() string         { return "daemon arguments" }
func (s *stringFlags) Set(value string) error { *s = append(*s, value); return nil }

func main() {
	if handled, code := commandContract(os.Args[1:], os.Stdout, os.Stderr); handled {
		if code != 0 {
			os.Exit(code)
		}
		return
	}
	mode := flag.String("mode", "owned", "daemon mode: owned or external")
	address := flag.String("address", "", "external daemon ws:// address")
	token := flag.String("token", os.Getenv("WAKU_DAEMON_TOKEN"), "daemon authentication token (or WAKU_DAEMON_TOKEN)")
	origin := flag.String("origin", "http://localhost", "exact desktop websocket origin")
	daemon := flag.String("daemon", "", "owned Go daemon executable (defaults to packaged sibling)")
	var daemonArgs stringFlags
	flag.Var(&daemonArgs, "daemon-arg", "argument passed to owned daemon (repeatable)")
	flag.Parse()
	var err error

	if *mode == "owned" {
		var prepared []string
		*token, *daemon, prepared, err = prepareOwned(*token, *daemon, *origin, daemonArgs, client.OwnedExecutable)
		daemonArgs = stringFlags(prepared)
		if err != nil {
			log.Fatalf("prepare owned daemon: %v", err)
		}
	}
	if *token == "" {
		log.Fatal("-token or WAKU_DAEMON_TOKEN is required for external mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ownedArgs := append([]string(nil), daemonArgs...)
	opts := client.SupervisorOptions{Address: *address, Token: *token, Origin: *origin, Executable: *daemon, Args: ownedArgs, StartTimeout: 10 * time.Second}
	var supervisor *client.Supervisor
	switch *mode {
	case "owned":
		supervisor, err = client.SpawnOwned(ctx, opts)
	case "external":
		supervisor, err = client.ConnectExternal(ctx, opts)
	default:
		log.Fatalf("unknown -mode %q", *mode)
	}
	if err != nil {
		log.Fatalf("connect daemon: %v", err)
	}

	model := presentation.NewModel(nil, nil)
	shell := views.NewShell(model, nil)
	mygo.App.WhenReady(func() {
		window := mygo.NewWindow(mygo.WindowOptions{Title: "Waku", Width: 1280, Height: 820, Content: ui.View(shell.View)})
		controller := presentation.NewController(model, window.Update)
		shell.Dispatcher = controller
		updates, stop := supervisor.SubscribeClients()
		go func() {
			for next := range updates {
				controller.SetTransport(next)
			}
		}()
		var shutdownStarted, shutdownReady atomic.Bool
		mygo.App.OnBeforeQuit(func(event *mygo.QuitEvent) {
			if shutdownReady.Load() {
				return
			}
			event.PreventDefault()
			if !shutdownStarted.CompareAndSwap(false, true) {
				return
			}
			stop()
			controller.Close()
			go func() {
				_ = supervisor.Close()
				shutdownReady.Store(true)
				mygo.App.Quit()
			}()
		})
	})
	if err := mygo.App.Run(); err != nil {
		log.Fatal(err)
	}
}

const benchmarkInstrumentation = "waku-native-v1"

type releaseRoleProbe struct {
	SchemaVersion int    `json:"schemaVersion"`
	Protocol      string `json:"protocol"`
	Role          string `json:"role"`
}

type benchmarkIdentity struct {
	SchemaVersion   int    `json:"schemaVersion"`
	Protocol        string `json:"protocol"`
	Role            string `json:"role"`
	Implementation  string `json:"implementation"`
	Instrumentation string `json:"instrumentation"`
}

type benchmarkDriverStatus struct {
	SchemaVersion int    `json:"schemaVersion"`
	Protocol      string `json:"protocol"`
	Status        string `json:"status"`
	RunNonce      string `json:"runNonce,omitempty"`
	Blocker       string `json:"blocker,omitempty"`
}

// commandContract handles release inspection without initializing MyGo,
// connecting a daemon, reading fixtures, or creating benchmark artifacts.
func commandContract(args []string, stdout, stderr io.Writer) (bool, int) {
	if len(args) == 1 && args[0] == "--waku-release-role-probe" {
		_ = json.NewEncoder(stdout).Encode(releaseRoleProbe{SchemaVersion: 1, Protocol: "waku-release-role-v1", Role: "desktop"})
		return true, 0
	}
	if len(args) == 1 && args[0] == "--waku-benchmark-identity" {
		_ = json.NewEncoder(stdout).Encode(benchmarkIdentity{SchemaVersion: 1, Protocol: "waku-benchmark-driver-v1", Role: "desktop", Implementation: "go", Instrumentation: benchmarkInstrumentation})
		return true, 0
	}
	if len(args) == 0 || args[0] != "--waku-benchmark-driver" {
		return false, 0
	}
	flags := flag.NewFlagSet("waku-benchmark-driver", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var nonce, workload, fixture, events, samples, environment string
	var warmups, iterations int
	flags.StringVar(&nonce, "run-nonce", "", "runner nonce")
	flags.StringVar(&workload, "workload", "", "workload ID")
	flags.StringVar(&fixture, "fixture", "", "fixture path")
	flags.IntVar(&warmups, "warmups", 0, "warmup count")
	flags.IntVar(&iterations, "iterations", 0, "iteration count")
	flags.StringVar(&events, "events", "", "event output")
	flags.StringVar(&samples, "samples", "", "sample output")
	flags.StringVar(&environment, "environment", "", "environment output")
	if err := flags.Parse(args[1:]); err != nil || flags.NArg() != 0 || nonce == "" || workload == "" || fixture == "" || warmups < 1 || iterations < 1 || events == "" || samples == "" || environment == "" {
		_ = json.NewEncoder(stderr).Encode(benchmarkDriverStatus{SchemaVersion: 1, Protocol: "waku-benchmark-driver-v1", Status: "blocked", Blocker: "invalid or incomplete native benchmark request"})
		return true, 3
	}
	blocker := nativeBenchmarkBlocker()
	_ = json.NewEncoder(stderr).Encode(benchmarkDriverStatus{SchemaVersion: 1, Protocol: "waku-benchmark-driver-v1", Status: "blocked", RunNonce: nonce, Blocker: blocker})
	return true, 3
}

func nativeBenchmarkBlocker() string {
	if runtime.GOOS == "linux" && os.Getenv("DISPLAY") == "" && os.Getenv("WAYLAND_DISPLAY") == "" {
		return "no live native display (DISPLAY and WAYLAND_DISPLAY are unset); no benchmark artifacts were written"
	}
	return "this MyGo revision does not expose compositor-present and native resource observation hooks; no benchmark artifacts were written"
}

func ephemeralToken() (string, error) {
	var value [32]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

func prepareOwned(token, executable, origin string, args []string, resolve func(string) (string, error)) (string, string, []string, error) {
	var err error
	if token == "" {
		token, err = ephemeralToken()
		if err != nil {
			return "", "", nil, err
		}
	}
	if executable == "" {
		executable, err = resolve("waku-go-daemon")
		if err != nil {
			return "", "", nil, err
		}
	}
	// Non-nil, explicit Go-daemon arguments prevent legacy bridge defaults.
	args = append(append([]string(nil), args...), "--listen", "127.0.0.1:0", "--allowed-origin", origin, "--allow-client-shutdown")
	return token, executable, args, nil
}
