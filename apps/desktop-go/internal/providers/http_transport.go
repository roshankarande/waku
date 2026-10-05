package providers

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
)

const (
	maxHTTPBodyBytes = 16 << 20
	maxSSEEventBytes = 4 << 20
)

type boundedHTTP struct {
	base, user, password string
	client               *http.Client
}

type providerHTTPError struct {
	Status int
	Body   string
}

func (e *providerHTTPError) Error() string {
	return fmt.Sprintf("provider HTTP %d: %s", e.Status, e.Body)
}

func validateLoopbackHTTPBase(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "http" || u.User != nil || u.Hostname() == "" || u.Port() == "" || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("provider endpoint is not a canonical loopback HTTP URL")
	}
	ip := net.ParseIP(u.Hostname())
	if ip == nil || !ip.IsLoopback() {
		return errors.New("provider endpoint is not a loopback IP")
	}
	return nil
}

func (h boundedHTTP) request(ctx context.Context, method, path string, body any, out any) error {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return err
		}
		if len(encoded) > maxHTTPBodyBytes {
			return errors.New("provider HTTP request body exceeds limit")
		}
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(h.base, "/")+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if h.user != "" {
		req.SetBasicAuth(h.user, h.password)
	}
	client := h.client
	if client == nil {
		client = http.DefaultClient
	}
	response, err := client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	limited := io.LimitReader(response.Body, maxHTTPBodyBytes+1)
	raw, err := io.ReadAll(limited)
	if err != nil {
		return err
	}
	if len(raw) > maxHTTPBodyBytes {
		return errors.New("provider HTTP response body exceeds limit")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return &providerHTTPError{Status: response.StatusCode, Body: strings.TrimSpace(string(raw))}
	}
	if out == nil || len(bytes.TrimSpace(raw)) == 0 {
		return nil
	}
	if err := decodeUseNumber(raw, out); err != nil {
		return fmt.Errorf("provider HTTP JSON: %w", err)
	}
	return nil
}

func (h boundedHTTP) openSSE(ctx context.Context, path string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(h.base, "/")+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "text/event-stream")
	if h.user != "" {
		req.SetBasicAuth(h.user, h.password)
	}
	client := h.client
	if client == nil {
		client = http.DefaultClient
	}
	response, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		defer response.Body.Close()
		raw, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return nil, fmt.Errorf("provider SSE HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(raw)))
	}
	return response.Body, nil
}

func readSSE(ctx context.Context, body io.Reader, consume func(json.RawMessage) error) error {
	reader := bufio.NewReader(body)
	var data []byte
	named := false
	for {
		line, err := readBoundedSSELine(reader)
		if err != nil {
			if errors.Is(err, io.EOF) && len(data) != 0 {
				if named {
					return errors.New("provider SSE sent an unsupported named event")
				}
				raw := bytes.TrimSpace(data)
				if len(raw) != 0 {
					return consume(cloneRaw(raw))
				}
			}
			return err
		}
		if len(line) == 0 {
			if len(data) != 0 {
				if named {
					return errors.New("provider SSE sent an unsupported named event")
				}
				raw := bytes.TrimSpace(data)
				if len(raw) > maxSSEEventBytes {
					return errors.New("provider SSE event exceeds limit")
				}
				if len(raw) != 0 {
					if err := consume(cloneRaw(raw)); err != nil {
						return err
					}
				}
				data = nil
				named = false
			}
			continue
		}
		if bytes.HasPrefix(line, []byte("data:")) {
			part := bytes.TrimSpace(line[5:])
			if len(data)+len(part)+1 > maxSSEEventBytes {
				return errors.New("provider SSE event exceeds limit")
			}
			if len(data) != 0 {
				data = append(data, '\n')
			}
			data = append(data, part...)
		} else if bytes.HasPrefix(line, []byte("event:")) {
			named = true
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
}

func readBoundedSSELine(reader *bufio.Reader) ([]byte, error) {
	var line []byte
	for {
		part, prefix, err := reader.ReadLine()
		if len(line)+len(part) > maxSSEEventBytes {
			return nil, errors.New("provider SSE line exceeds limit")
		}
		line = append(line, part...)
		if err != nil {
			return line, err
		}
		if !prefix {
			return line, nil
		}
	}
}
