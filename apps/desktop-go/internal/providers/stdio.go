package providers

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"sync"
	"time"
)

// JSONLines is shared by JSON-RPC and provider-specific NDJSON protocols.  A
// Decoder avoids Scanner's token ceiling, and UseNumber preserves opaque
// integer tokens beyond JavaScript's safe range.
type JSONLines struct {
	reader *bufio.Reader
	writer io.Writer
	closer io.Closer
	mu     sync.Mutex
}

const MaxFrameBytes = 16 << 20

func NewJSONLines(reader io.Reader, writer io.Writer, closer io.Closer) *JSONLines {
	return &JSONLines{reader: bufio.NewReader(reader), writer: writer, closer: closer}
}

func (j *JSONLines) Decode(value any) error {
	var frame []byte
	for {
		part, prefix, err := j.reader.ReadLine()
		if err != nil {
			return err
		}
		if len(frame)+len(part) > MaxFrameBytes {
			return fmt.Errorf("provider frame exceeds %d bytes", MaxFrameBytes)
		}
		frame = append(frame, part...)
		if prefix {
			continue
		}
		if len(bytes.TrimSpace(frame)) == 0 {
			frame = frame[:0]
			continue
		}
		decoder := json.NewDecoder(bytes.NewReader(frame))
		decoder.UseNumber()
		if err := decoder.Decode(value); err != nil {
			return err
		}
		var trailing any
		if err := decoder.Decode(&trailing); err != io.EOF {
			return errors.New("provider frame contains trailing JSON")
		}
		return nil
	}
}

func (j *JSONLines) Write(value any) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(encoded) > MaxFrameBytes {
		return fmt.Errorf("provider frame exceeds %d bytes", MaxFrameBytes)
	}
	encoded = append(encoded, '\n')
	for len(encoded) > 0 {
		n, writeErr := j.writer.Write(encoded)
		if n < 0 || n > len(encoded) {
			return io.ErrShortWrite
		}
		if n > 0 {
			encoded = encoded[n:]
		}
		if writeErr != nil {
			return writeErr
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func (j *JSONLines) CloseInput() error {
	if j.closer == nil {
		return nil
	}
	return j.closer.Close()
}

type RPCMessage struct {
	JSONRPC string          `json:"jsonrpc,omitempty"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   json.RawMessage `json:"error,omitempty"`
}

func (m RPCMessage) IsRequest() bool  { return m.Method != "" && len(m.ID) != 0 }
func (m RPCMessage) IsResponse() bool { return m.Method == "" && len(m.ID) != 0 }

// decodeRPCMessage validates the JSON-RPC envelope before callers correlate a
// response or publish a request/notification. Codex currently omits jsonrpc,
// so an absent version remains compatible, but a supplied version must be 2.0.
func decodeRPCMessage(raw []byte) (RPCMessage, error) {
	var fields map[string]json.RawMessage
	if err := decodeUseNumber(raw, &fields); err != nil {
		return RPCMessage{}, err
	}
	if fields == nil {
		return RPCMessage{}, errors.New("provider JSON-RPC envelope must be an object")
	}
	var message RPCMessage
	if err := decodeUseNumber(raw, &message); err != nil {
		return RPCMessage{}, err
	}
	if _, present := fields["jsonrpc"]; present && message.JSONRPC != "2.0" {
		return RPCMessage{}, fmt.Errorf("provider JSON-RPC version %q is invalid", message.JSONRPC)
	}
	methodRaw, hasMethod := fields["method"]
	_, hasID := fields["id"]
	_, hasResult := fields["result"]
	_, hasError := fields["error"]
	if hasMethod {
		if message.Method == "" || string(methodRaw) == "null" {
			return RPCMessage{}, errors.New("provider JSON-RPC method is empty")
		}
		if hasResult || hasError {
			return RPCMessage{}, errors.New("provider JSON-RPC request contains response fields")
		}
		if hasID && !validRPCID(message.ID) {
			return RPCMessage{}, errors.New("provider JSON-RPC request has invalid id")
		}
		return message, nil
	}
	if !hasID || !validRPCID(message.ID) {
		return RPCMessage{}, errors.New("provider JSON-RPC response is missing a valid id")
	}
	if _, hasParams := fields["params"]; hasParams {
		return RPCMessage{}, errors.New("provider JSON-RPC response contains params")
	}
	if hasResult == hasError {
		return RPCMessage{}, errors.New("provider JSON-RPC response must contain exactly one of result or error")
	}
	if hasError {
		if _, err := rpcErrorMessage(message.Error); err != nil {
			return RPCMessage{}, err
		}
	}
	return message, nil
}

func rpcErrorMessage(raw json.RawMessage) (string, error) {
	var value struct {
		Code    json.Number `json:"code"`
		Message string      `json:"message"`
	}
	if err := decodeUseNumber(raw, &value); err != nil {
		return "", fmt.Errorf("provider JSON-RPC error: %w", err)
	}
	if value.Message == "" || value.Code == "" {
		return "", errors.New("provider JSON-RPC error requires code and message")
	}
	if _, err := strconv.ParseInt(value.Code.String(), 10, 64); err != nil {
		return "", fmt.Errorf("provider JSON-RPC error code %q is invalid", value.Code)
	}
	return value.Message, nil
}

// StdioProcess owns exactly one long-lived child. Close asks the provider to
// exit by closing stdin; it does not signal or kill it.
type StdioProcess struct {
	Lines  *JSONLines
	Stderr io.Reader
	wait   chan error
	once   sync.Once
	kill   func() error
	stdout io.Closer
	errout io.Closer

	waitOnce sync.Once
	waitDone chan struct{}
	waitMu   sync.Mutex
	waitErr  error
}

type ProcessSpec struct {
	Path      string
	Args, Env []string
	Directory string
}

func StartStdio(_ context.Context, spec ProcessSpec) (*StdioProcess, error) {
	cmd := exec.Command(spec.Path, spec.Args...)
	cmd.Dir = spec.Directory
	if spec.Env != nil {
		cmd.Env = spec.Env
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("provider stdin: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("provider stdout: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("provider stderr: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start provider: %w", err)
	}
	p := &StdioProcess{Lines: NewJSONLines(stdout, stdin, stdin), Stderr: stderr, wait: make(chan error, 1), kill: cmd.Process.Kill, stdout: stdout, errout: stderr}
	go func() { p.wait <- cmd.Wait(); close(p.wait) }()
	return p, nil
}

func (p *StdioProcess) waitChannel() <-chan struct{} {
	p.waitOnce.Do(func() {
		p.waitDone = make(chan struct{})
		go func() {
			err := <-p.wait
			p.waitMu.Lock()
			p.waitErr = err
			p.waitMu.Unlock()
			close(p.waitDone)
		}()
	})
	return p.waitDone
}

func (p *StdioProcess) Wait() error {
	<-p.waitChannel()
	p.waitMu.Lock()
	defer p.waitMu.Unlock()
	return p.waitErr
}
func (p *StdioProcess) Close() error {
	var err error
	p.once.Do(func() { err = p.Lines.CloseInput() })
	return err
}

// Terminate first requests the normal stdin-EOF shutdown, then forcefully
// terminates and reaps a child that does not cooperate. Closing captured output
// descriptors also unblocks a reader stalled in Decode or stderr draining.
func (p *StdioProcess) Terminate(ctx context.Context) error {
	_ = p.Close()
	done := p.waitChannel()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
	}
	if p.kill != nil {
		_ = p.kill()
	}
	if p.stdout != nil {
		_ = p.stdout.Close()
	}
	if p.errout != nil {
		_ = p.errout.Close()
	}
	select {
	case <-done:
		return nil
	case <-time.After(2 * time.Second):
		return errors.New("provider process did not exit after termination")
	}
}
