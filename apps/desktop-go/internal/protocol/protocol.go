// Package protocol is the sole encoder and decoder for Waku protocol v7.
package protocol

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
)

const (
	Version             uint32 = 7
	MaxWireMessageBytes        = 48 * 1024 * 1024
	MaxHandshakeBytes          = 64 * 1024
	DaemonTokenEnv             = "WAKU_DAEMON_TOKEN"
	DaemonAddressEnv           = "WAKU_DAEMON_ADDRESS"
	AppExecutableEnv           = "WAKU_APP_EXECUTABLE"
)

type MessageType string

const (
	MessageHello            MessageType = "hello"
	MessageRequest          MessageType = "request"
	MessageShutdown         MessageType = "shutdown"
	MessageRejected         MessageType = "rejected"
	MessageResponse         MessageType = "response"
	MessageEvent            MessageType = "event"
	MessageTaskStateChanged MessageType = "taskStateChanged"
	MessageShuttingDown     MessageType = "shuttingDown"
)

var uuidPattern = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// UUID is the canonical textual UUID used on the wire. It intentionally has
// no dependency on a particular UUID package.
type UUID string

const NilUUID UUID = "00000000-0000-0000-0000-000000000000"

func ParseUUID(s string) (UUID, error) {
	if !uuidPattern.MatchString(s) {
		return "", fmt.Errorf("invalid UUID %q", s)
	}
	return UUID(s), nil
}

func (u UUID) Valid() bool { return uuidPattern.MatchString(string(u)) }

func (u UUID) MarshalJSON() ([]byte, error) {
	if !u.Valid() {
		return nil, fmt.Errorf("invalid UUID %q", u)
	}
	return json.Marshal(string(u))
}

func (u *UUID) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	parsed, err := ParseUUID(s)
	if err != nil {
		return err
	}
	*u = parsed
	return nil
}

type ReplayCursor struct {
	SessionID UUID   `json:"sessionId"`
	RuntimeID UUID   `json:"runtimeId"`
	Epoch     UUID   `json:"epoch"`
	Sequence  uint64 `json:"sequence"`
}

func (c *ReplayCursor) UnmarshalJSON(data []byte) error {
	type alias ReplayCursor
	if err := requireJSONFields(data, "sessionId", "runtimeId", "epoch", "sequence"); err != nil {
		return err
	}
	var v alias
	if err := decode(data, &v); err != nil {
		return err
	}
	*c = ReplayCursor(v)
	return c.Validate()
}

type DaemonReady struct {
	Address         string `json:"address"`
	ProtocolVersion uint32 `json:"protocolVersion"`
	PID             uint32 `json:"pid"`
}

type Hello struct {
	Type            string         `json:"type"`
	ProtocolVersion uint32         `json:"protocolVersion"`
	Token           string         `json:"token"`
	ClientID        UUID           `json:"clientId"`
	ResumeFrom      []ReplayCursor `json:"resumeFrom"`
}

func (h *Hello) UnmarshalJSON(data []byte) error {
	type alias Hello
	if err := requireJSONFields(data, "type", "protocolVersion", "token", "clientId"); err != nil {
		return err
	}
	var object map[string]json.RawMessage
	if err := decode(data, &object); err != nil {
		return err
	}
	if raw, ok := object["resumeFrom"]; ok && bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return errors.New("hello resumeFrom is null")
	}
	var v alias
	if err := decode(data, &v); err != nil {
		return err
	}
	*h = Hello(v)
	return h.Validate()
}

func (h Hello) Validate() error {
	if h.Type != string(MessageHello) {
		return fmt.Errorf("hello has type %q", h.Type)
	}
	if h.ProtocolVersion == 0 {
		return errors.New("hello protocolVersion is zero")
	}
	if strings.TrimSpace(h.Token) == "" {
		return errors.New("hello token is empty")
	}
	if !h.ClientID.Valid() {
		return errors.New("hello clientId is not a UUID")
	}
	for i, cursor := range h.ResumeFrom {
		if err := cursor.Validate(); err != nil {
			return fmt.Errorf("hello resumeFrom[%d]: %w", i, err)
		}
	}
	return nil
}

func (c ReplayCursor) Validate() error {
	if !c.SessionID.Valid() || !c.RuntimeID.Valid() || !c.Epoch.Valid() {
		return errors.New("replay cursor contains an invalid UUID")
	}
	return nil
}

func NewHello(token string, clientID UUID, resume []ReplayCursor) Hello {
	if resume == nil {
		resume = []ReplayCursor{}
	}
	return Hello{Type: "hello", ProtocolVersion: Version, Token: token, ClientID: clientID, ResumeFrom: resume}
}

type Request struct {
	RequestID UUID            `json:"requestId"`
	SessionID UUID            `json:"sessionId"`
	RuntimeID UUID            `json:"runtimeId"`
	Command   json.RawMessage `json:"command"`
}

type RequestMessage struct {
	Type string `json:"type"`
	Request
}

type ClientMessage struct {
	Type    MessageType
	Hello   *Hello
	Request *Request
}

func (m *ClientMessage) UnmarshalJSON(data []byte) error {
	var header struct {
		Type MessageType `json:"type"`
	}
	if err := decode(data, &header); err != nil {
		return err
	}
	*m = ClientMessage{Type: header.Type}
	switch header.Type {
	case MessageHello:
		var value Hello
		if err := decode(data, &value); err != nil {
			return err
		}
		if err := value.Validate(); err != nil {
			return err
		}
		m.Hello = &value
	case MessageRequest:
		var value RequestMessage
		if err := decode(data, &value); err != nil {
			return err
		}
		if !value.RequestID.Valid() || !value.SessionID.Valid() || !value.RuntimeID.Valid() {
			return errors.New("request contains an invalid UUID")
		}
		if _, err := DecodeCommand(value.Command); err != nil {
			return fmt.Errorf("request command: %w", err)
		}
		m.Request = &value.Request
	case MessageShutdown:
	default:
		return fmt.Errorf("unknown client message type %q", header.Type)
	}
	return nil
}

func NewRequest(requestID, sessionID, runtimeID UUID, command any) (RequestMessage, error) {
	if !requestID.Valid() || !sessionID.Valid() || !runtimeID.Valid() {
		return RequestMessage{}, errors.New("request contains an invalid UUID")
	}
	raw, err := json.Marshal(command)
	if err != nil {
		return RequestMessage{}, err
	}
	if _, err := DecodeCommand(raw); err != nil {
		return RequestMessage{}, err
	}
	return RequestMessage{Type: "request", Request: Request{requestID, sessionID, runtimeID, raw}}, nil
}

type Command struct {
	Type string `json:"type"`
}

type Shutdown struct {
	Type string `json:"type"`
}

func ShutdownMessage() Shutdown { return Shutdown{Type: "shutdown"} }

type WireDriverEvent struct {
	Kind    string          `json:"kind"`
	Payload json.RawMessage `json:"payload"`
}

type SequencedEvent struct {
	SessionID UUID            `json:"sessionId"`
	RuntimeID UUID            `json:"runtimeId"`
	Epoch     UUID            `json:"epoch"`
	Sequence  uint64          `json:"sequence"`
	Event     WireDriverEvent `json:"event"`
}

type RpcError struct {
	Message string `json:"message"`
}

type ResponseOutcome struct {
	Status  string          `json:"status"`
	Payload json.RawMessage `json:"payload,omitempty"`
	Error   *RpcError       `json:"error,omitempty"`
}

func (o *ResponseOutcome) UnmarshalJSON(data []byte) error {
	var raw struct {
		Status  string          `json:"status"`
		Payload json.RawMessage `json:"payload"`
		Error   json.RawMessage `json:"error"`
	}
	if err := decode(data, &raw); err != nil {
		return err
	}
	switch raw.Status {
	case "ok":
		if len(raw.Payload) == 0 || string(raw.Payload) == "null" {
			return errors.New("ok response has no payload")
		}
		if _, err := DecodeResponsePayloadAny(raw.Payload); err != nil {
			return fmt.Errorf("response payload: %w", err)
		}
		*o = ResponseOutcome{Status: raw.Status, Payload: cloneJSON(raw.Payload)}
	case "error":
		if len(raw.Error) == 0 || string(raw.Error) == "null" {
			return errors.New("error response has no error")
		}
		if err := requireJSONFields(raw.Error, "message"); err != nil {
			return fmt.Errorf("error response: %w", err)
		}
		var rpc RpcError
		if err := decode(raw.Error, &rpc); err != nil {
			return err
		}
		*o = ResponseOutcome{Status: raw.Status, Error: &rpc}
	default:
		return fmt.Errorf("unknown response status %q", raw.Status)
	}
	return nil
}

func cloneJSON(raw []byte) json.RawMessage { return append(json.RawMessage(nil), raw...) }

type ServerMessage struct {
	Type            string           `json:"type"`
	ProtocolVersion uint32           `json:"protocolVersion,omitempty"`
	DaemonVersion   string           `json:"daemonVersion,omitempty"`
	Message         string           `json:"message,omitempty"`
	RequestID       UUID             `json:"requestId,omitempty"`
	Outcome         *ResponseOutcome `json:"outcome,omitempty"`
	Event           *SequencedEvent  `json:"-"`
	Revision        uint64           `json:"revision,omitempty"`
}

func (m ServerMessage) MarshalJSON() ([]byte, error) {
	if m.Type == string(MessageEvent) || (m.Type == "" && m.Event != nil) {
		if m.Event == nil {
			return nil, errors.New("event message has no event")
		}
		if err := m.Event.Validate(); err != nil {
			return nil, err
		}
		raw, err := json.Marshal(m.Event)
		if err != nil {
			return nil, err
		}
		var object map[string]json.RawMessage
		if err := decode(raw, &object); err != nil {
			return nil, err
		}
		object["type"] = json.RawMessage(`"event"`)
		return json.Marshal(object)
	}
	switch MessageType(m.Type) {
	case MessageHello:
		if m.ProtocolVersion == 0 {
			return nil, errors.New("server hello protocolVersion is zero")
		}
	case MessageRejected, MessageTaskStateChanged, MessageShuttingDown:
	case MessageResponse:
		if !m.RequestID.Valid() {
			return nil, errors.New("response requestId is not a UUID")
		}
		if m.Outcome == nil {
			return nil, errors.New("response has no outcome")
		}
		raw, err := json.Marshal(m.Outcome)
		if err != nil {
			return nil, err
		}
		var checked ResponseOutcome
		if err := decode(raw, &checked); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unknown server message type %q", m.Type)
	}
	type alias ServerMessage
	return json.Marshal(alias(m))
}

func (m *ServerMessage) UnmarshalJSON(data []byte) error {
	var header struct {
		Type string `json:"type"`
	}
	if err := decode(data, &header); err != nil {
		return err
	}
	switch MessageType(header.Type) {
	case MessageEvent:
		if err := requireJSONFields(data, "sessionId", "runtimeId", "epoch", "sequence", "event"); err != nil {
			return err
		}
		var event SequencedEvent
		if err := decode(data, &event); err != nil {
			return err
		}
		if err := event.Validate(); err != nil {
			return err
		}
		*m = ServerMessage{Type: header.Type, Event: &event}
		return nil
	case MessageHello:
		if err := requireJSONFields(data, "protocolVersion", "daemonVersion"); err != nil {
			return err
		}
		var v struct {
			Type            string `json:"type"`
			ProtocolVersion uint32 `json:"protocolVersion"`
			DaemonVersion   string `json:"daemonVersion"`
		}
		if err := decode(data, &v); err != nil {
			return err
		}
		if v.ProtocolVersion == 0 {
			return errors.New("server hello protocolVersion is zero")
		}
		*m = ServerMessage{Type: v.Type, ProtocolVersion: v.ProtocolVersion, DaemonVersion: v.DaemonVersion}
		return nil
	case MessageRejected:
		if err := requireJSONFields(data, "message"); err != nil {
			return err
		}
		var v struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		}
		if err := decode(data, &v); err != nil {
			return err
		}
		*m = ServerMessage{Type: v.Type, Message: v.Message}
		return nil
	case MessageResponse:
		if err := requireJSONFields(data, "requestId", "outcome"); err != nil {
			return err
		}
		var v struct {
			Type      string           `json:"type"`
			RequestID UUID             `json:"requestId"`
			Outcome   *ResponseOutcome `json:"outcome"`
		}
		if err := decode(data, &v); err != nil {
			return err
		}
		if !v.RequestID.Valid() {
			return errors.New("response requestId is not a UUID")
		}
		if v.Outcome == nil {
			return errors.New("response has no outcome")
		}
		*m = ServerMessage{Type: v.Type, RequestID: v.RequestID, Outcome: v.Outcome}
		return nil
	case MessageTaskStateChanged:
		if err := requireJSONFields(data, "revision"); err != nil {
			return err
		}
		var v struct {
			Type     string `json:"type"`
			Revision uint64 `json:"revision"`
		}
		if err := decode(data, &v); err != nil {
			return err
		}
		*m = ServerMessage{Type: v.Type, Revision: v.Revision}
		return nil
	case MessageShuttingDown:
		*m = ServerMessage{Type: header.Type}
		return nil
	default:
		return fmt.Errorf("unknown server message type %q", header.Type)
	}
}

func (e *SequencedEvent) Validate() error {
	if !e.SessionID.Valid() || !e.RuntimeID.Valid() || !e.Epoch.Valid() {
		return errors.New("event contains an invalid UUID")
	}
	if e.Sequence == 0 {
		return errors.New("event sequence is zero")
	}
	if e.Event.Kind == "" {
		return errors.New("event kind is empty")
	}
	if len(e.Event.Payload) == 0 {
		e.Event.Payload = json.RawMessage("null")
	}
	if _, err := DecodeEventPayloadAny(e.Event); err != nil {
		return fmt.Errorf("event %q: %w", e.Event.Kind, err)
	}
	return nil
}

// Optional records all three JSON states. Parent structs that need omission
// should implement MarshalJSON and omit fields for which Present is false.
type Optional[T any] struct {
	Present bool
	Null    bool
	Value   T
}

func Some[T any](value T) Optional[T] { return Optional[T]{Present: true, Value: value} }
func Null[T any]() Optional[T]        { return Optional[T]{Present: true, Null: true} }

// Put adds the field to an object only when it was present on the wire.
func (o Optional[T]) Put(object map[string]any, name string) {
	if !o.Present {
		return
	}
	if o.Null {
		object[name] = nil
	} else {
		object[name] = o.Value
	}
}

func (o *Optional[T]) UnmarshalJSON(data []byte) error {
	o.Present = true
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		o.Null = true
		var zero T
		o.Value = zero
		return nil
	}
	o.Null = false
	return decode(data, &o.Value)
}

func (o Optional[T]) MarshalJSON() ([]byte, error) {
	if o.Null {
		return []byte("null"), nil
	}
	return json.Marshal(o.Value)
}

// Base64Bytes preserves Rust serde's standard padded base64 representation.
type Base64Bytes []byte

func (b Base64Bytes) MarshalJSON() ([]byte, error) {
	return json.Marshal(base64.StdEncoding.EncodeToString(b))
}

func (b *Base64Bytes) UnmarshalJSON(data []byte) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return errors.New("base64 value is null")
	}
	var encoded string
	if err := json.Unmarshal(data, &encoded); err != nil {
		return err
	}
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return err
	}
	*b = decoded
	return nil
}

func Marshal(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	if len(b) > MaxWireMessageBytes {
		return nil, fmt.Errorf("protocol message is %d bytes; maximum is %d", len(b), MaxWireMessageBytes)
	}
	return b, nil
}

func MarshalHello(value Hello) ([]byte, error) {
	if value.ResumeFrom == nil {
		value.ResumeFrom = []ReplayCursor{}
	}
	if err := value.Validate(); err != nil {
		return nil, err
	}
	b, err := Marshal(value)
	if err != nil {
		return nil, err
	}
	if len(b) > MaxHandshakeBytes {
		return nil, fmt.Errorf("daemon hello is %d bytes; handshake maximum is %d", len(b), MaxHandshakeBytes)
	}
	return b, nil
}

func Decode(data []byte, dst any) error {
	if len(data) > MaxWireMessageBytes {
		return fmt.Errorf("protocol message is %d bytes; maximum is %d", len(data), MaxWireMessageBytes)
	}
	return decode(data, dst)
}

func DecodeReader(r io.Reader, dst any) error {
	limited := io.LimitReader(r, MaxWireMessageBytes+1)
	b, err := io.ReadAll(limited)
	if err != nil {
		return err
	}
	if len(b) > MaxWireMessageBytes {
		return errors.New("protocol message exceeds maximum size")
	}
	return decode(b, dst)
}

func decode(data []byte, dst any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	var trailing any
	if err := dec.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("more than one JSON value")
		}
		return err
	}
	return nil
}
