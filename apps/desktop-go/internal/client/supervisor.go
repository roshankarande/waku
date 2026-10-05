package client

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/roshankarande/waku/apps/desktop-go/internal/protocol"
)

type Ownership uint8

const (
	ExternalDaemon Ownership = iota
	OwnedDaemon
)

type SupervisorOptions struct {
	Address      string
	Token        string
	Origin       string
	Executable   string
	Args         []string
	Environment  []string
	ReconnectMin time.Duration
	ReconnectMax time.Duration
	StartTimeout time.Duration
}

// ParseAllowedOrigins normalizes the exact HTTP(S) origins accepted by the
// daemon. Paths, credentials, query strings, fragments, and non-web schemes
// are rejected rather than broadened implicitly.
func ParseAllowedOrigins(text string) ([]string, error) {
	var result []string
	seen := make(map[string]bool)
	for _, candidate := range strings.Split(text, ",") {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		u, err := url.Parse(candidate)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
			return nil, fmt.Errorf("browser origin %q must be an exact http:// or https:// origin without a path", candidate)
		}
		host := strings.ToLower(u.Hostname())
		if strings.Contains(host, "%") {
			return nil, fmt.Errorf("browser origin %q contains an IPv6 zone", candidate)
		}
		for _, r := range host {
			if r > 127 || r <= 0x20 {
				return nil, fmt.Errorf("browser origin %q must use an ASCII host", candidate)
			}
		}
		if ip := net.ParseIP(host); ip != nil {
			host = ip.String()
		}
		port := u.Port()
		if port != "" {
			if _, err := strconv.ParseUint(port, 10, 16); err != nil {
				return nil, fmt.Errorf("browser origin %q has an invalid port", candidate)
			}
		}
		if (u.Scheme == "http" && port == "80") || (u.Scheme == "https" && port == "443") {
			port = ""
		}
		if strings.Contains(host, ":") {
			if port != "" {
				host = net.JoinHostPort(host, port)
			} else {
				host = "[" + host + "]"
			}
		} else if port != "" {
			host = net.JoinHostPort(host, port)
		}
		origin := strings.ToLower(u.Scheme) + "://" + host
		if !seen[origin] {
			seen[origin] = true
			result = append(result, origin)
		}
	}
	return result, nil
}

type Supervisor struct {
	opts      SupervisorOptions
	ownership Ownership

	mu          sync.RWMutex
	client      *Client
	process     *exec.Cmd
	processDone chan struct{}
	updates     []chan *Client
	closed      chan struct{}
	closeOnce   sync.Once
}

const maxClientSubscribers = 128

func ConnectExternal(ctx context.Context, opts SupervisorOptions) (*Supervisor, error) {
	if opts.Address == "" {
		return nil, errors.New("external daemon address is required")
	}
	c, err := Connect(ctx, ConnectOptions{Address: opts.Address, Token: opts.Token, Origin: opts.Origin})
	if err != nil {
		return nil, err
	}
	s := newSupervisor(opts, ExternalDaemon, c, nil)
	go s.monitor()
	return s, nil
}

func SpawnOwned(ctx context.Context, opts SupervisorOptions) (*Supervisor, error) {
	if opts.Executable == "" {
		return nil, errors.New("daemon executable is required")
	}
	if strings.TrimSpace(opts.Token) == "" {
		return nil, errors.New("daemon authentication token is empty")
	}
	cmd, ready, err := startOwned(ctx, opts)
	if err != nil {
		return nil, err
	}
	if ready.ProtocolVersion != protocol.Version {
		terminate(cmd)
		return nil, fmt.Errorf("daemon protocol %d does not match desktop protocol %d", ready.ProtocolVersion, protocol.Version)
	}
	address, err := desktopClientAddress(ready.Address)
	if err != nil {
		terminate(cmd)
		return nil, err
	}
	opts.Address = address
	c, err := Connect(ctx, ConnectOptions{Address: address, Token: opts.Token, Origin: opts.Origin})
	if err != nil {
		terminate(cmd)
		return nil, err
	}
	s := newSupervisor(opts, OwnedDaemon, c, cmd)
	s.watchProcess(cmd)
	go s.monitor()
	return s, nil
}

func newSupervisor(opts SupervisorOptions, ownership Ownership, c *Client, cmd *exec.Cmd) *Supervisor {
	if opts.ReconnectMin <= 0 {
		opts.ReconnectMin = 100 * time.Millisecond
	}
	if opts.ReconnectMax <= 0 {
		opts.ReconnectMax = 5 * time.Second
	}
	return &Supervisor{opts: opts, ownership: ownership, client: c, process: cmd, closed: make(chan struct{})}
}

func (s *Supervisor) watchProcess(cmd *exec.Cmd) {
	done := make(chan struct{})
	s.mu.Lock()
	s.processDone = done
	s.mu.Unlock()
	go func() { _ = cmd.Wait(); close(done) }()
}

func (s *Supervisor) Client() *Client      { s.mu.RLock(); defer s.mu.RUnlock(); return s.client }
func (s *Supervisor) Ownership() Ownership { return s.ownership }

func (s *Supervisor) SubscribeClients() (<-chan *Client, func()) {
	ch := make(chan *Client, 1)
	s.mu.Lock()
	select {
	case <-s.closed:
		close(ch)
		s.mu.Unlock()
		return ch, func() {}
	default:
	}
	if len(s.updates) >= maxClientSubscribers {
		close(ch)
		s.mu.Unlock()
		return ch, func() {}
	}
	s.updates = append(s.updates, ch)
	ch <- s.client
	s.mu.Unlock()
	return ch, func() {
		s.mu.Lock()
		for i, candidate := range s.updates {
			if candidate == ch {
				s.updates = append(s.updates[:i], s.updates[i+1:]...)
				close(ch)
				break
			}
		}
		s.mu.Unlock()
	}
}

func (s *Supervisor) monitor() {
	for {
		current := s.Client()
		select {
		case <-current.Done():
		case <-s.closed:
			return
		}
		cursors := current.LastSequences()
		delay := s.opts.ReconnectMin
		for {
			select {
			case <-s.closed:
				return
			case <-time.After(delay):
			}
			if s.ownership == OwnedDaemon {
				s.mu.RLock()
				processDone := s.processDone
				s.mu.RUnlock()
				select {
				case <-processDone:
					s.mu.RLock()
					opts := s.opts
					s.mu.RUnlock()
					ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
					cmd, ready, err := startOwned(ctx, opts)
					cancel()
					if err != nil {
						break
					}
					if ready.ProtocolVersion != protocol.Version {
						terminate(cmd)
						break
					}
					address, err := desktopClientAddress(ready.Address)
					if err != nil {
						terminate(cmd)
						break
					}
					s.mu.Lock()
					s.process = cmd
					s.opts.Address = address
					s.mu.Unlock()
					s.watchProcess(cmd)
				default:
					// A transport loss alone does not authorize a second writer.
					// Reconnect to the existing owned process first.
				}
			}
			s.mu.RLock()
			opts := s.opts
			s.mu.RUnlock()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			replacement, err := Connect(ctx, ConnectOptions{Address: opts.Address, Token: opts.Token, Origin: opts.Origin, ResumeFrom: cursors})
			cancel()
			if err == nil {
				s.replace(replacement)
				break
			}
			if delay < s.opts.ReconnectMax {
				delay *= 2
				if delay > s.opts.ReconnectMax {
					delay = s.opts.ReconnectMax
				}
			}
		}
	}
}

func (s *Supervisor) replace(c *Client) {
	s.mu.Lock()
	select {
	case <-s.closed:
		s.mu.Unlock()
		_ = c.CloseTransport()
		return
	default:
	}
	s.client = c
	alive := s.updates[:0]
	for _, update := range s.updates {
		select {
		case <-update:
		default:
		}
		select {
		case update <- c:
			alive = append(alive, update)
		default:
		}
	}
	s.updates = alive
	s.mu.Unlock()
}

func (s *Supervisor) Close() error {
	s.closeOnce.Do(func() {
		close(s.closed)
		s.mu.Lock()
		c, cmd := s.client, s.process
		for _, update := range s.updates {
			close(update)
		}
		s.updates = nil
		s.mu.Unlock()
		if s.ownership == OwnedDaemon {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			_ = c.RequestShutdown(ctx)
			cancel()
			if cmd != nil {
				s.mu.RLock()
				done := s.processDone
				s.mu.RUnlock()
				select {
				case <-done:
				case <-time.After(time.Second):
					if cmd.Process != nil {
						_ = cmd.Process.Kill()
					}
				}
			}
		} else {
			// Externally managed daemons must never receive the shutdown message.
			_ = c.CloseTransport()
		}
	})
	return nil
}

func startOwned(ctx context.Context, opts SupervisorOptions) (*exec.Cmd, protocol.DaemonReady, error) {
	if err := ctx.Err(); err != nil {
		return nil, protocol.DaemonReady{}, err
	}
	args := opts.Args
	if args == nil {
		args = []string{"--bind", "127.0.0.1:0", "--parent-pid", strconv.Itoa(os.Getpid())}
	}
	cmd := exec.Command(opts.Executable, args...)
	cmd.Env = append(os.Environ(), opts.Environment...)
	cmd.Env = append(cmd.Env, protocol.DaemonTokenEnv+"="+opts.Token)
	if app, err := os.Executable(); err == nil {
		cmd.Env = append(cmd.Env, protocol.AppExecutableEnv+"="+app)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, protocol.DaemonReady{}, err
	}
	cmd.Stdin = nil
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, protocol.DaemonReady{}, fmt.Errorf("start daemon: %w", err)
	}
	timeout := opts.StartTimeout
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	result := make(chan struct {
		ready protocol.DaemonReady
		err   error
	}, 1)
	go func() {
		ready, err := readDaemonReady(stdout, cmd.Process.Pid)
		result <- struct {
			ready protocol.DaemonReady
			err   error
		}{ready, err}
	}()
	select {
	case got := <-result:
		if got.err != nil {
			terminate(cmd)
			return nil, got.ready, fmt.Errorf("read daemon readiness: %w", got.err)
		}
		return cmd, got.ready, nil
	case <-time.After(timeout):
		terminate(cmd)
		return nil, protocol.DaemonReady{}, errors.New("timed out waiting for Waku daemon")
	case <-ctx.Done():
		terminate(cmd)
		return nil, protocol.DaemonReady{}, ctx.Err()
	}
}

func readDaemonReady(reader io.Reader, expectedPID int) (protocol.DaemonReady, error) {
	line, err := bufio.NewReader(io.LimitReader(reader, protocol.MaxHandshakeBytes+1)).ReadBytes('\n')
	if err != nil {
		return protocol.DaemonReady{}, err
	}
	if len(line) > protocol.MaxHandshakeBytes {
		return protocol.DaemonReady{}, errors.New("daemon readiness exceeds handshake maximum")
	}
	var ready protocol.DaemonReady
	if err := protocol.Decode(line, &ready); err != nil {
		return ready, err
	}
	if ready.Address == "" || ready.PID == 0 {
		return ready, errors.New("daemon readiness is missing address or pid")
	}
	if _, err := desktopClientAddress(ready.Address); err != nil {
		return ready, fmt.Errorf("daemon readiness address: %w", err)
	}
	if uint64(expectedPID) > uint64(^uint32(0)) || ready.PID != uint32(expectedPID) {
		return ready, fmt.Errorf("daemon readiness pid %d does not match spawned pid %d", ready.PID, expectedPID)
	}
	if ready.ProtocolVersion != protocol.Version {
		return ready, fmt.Errorf("daemon protocol %d does not match desktop protocol %d", ready.ProtocolVersion, protocol.Version)
	}
	return ready, nil
}

func terminate(cmd *exec.Cmd) {
	if cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	}
}

func desktopClientAddress(address string) (string, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return "", fmt.Errorf("invalid daemon ready address: %w", err)
	}
	ip, err := netip.ParseAddr(host)
	if err != nil || ip.Zone() != "" {
		return "", fmt.Errorf("invalid daemon IP address %q", host)
	}
	if ip.IsUnspecified() {
		if ip.Is4() {
			ip = netip.MustParseAddr("127.0.0.1")
		} else {
			ip = netip.IPv6Loopback()
		}
	}
	if _, err := strconv.ParseUint(port, 10, 16); err != nil {
		return "", fmt.Errorf("invalid daemon port: %w", err)
	}
	return net.JoinHostPort(ip.String(), port), nil
}

// OwnedExecutable resolves a sibling daemon without relying on the working directory.
func OwnedExecutable(name string) (string, error) {
	app, err := os.Executable()
	if err != nil {
		return "", err
	}
	if runtime.GOOS == "windows" && filepath.Ext(name) == "" {
		name += ".exe"
	}
	return filepath.Join(filepath.Dir(app), name), nil
}
