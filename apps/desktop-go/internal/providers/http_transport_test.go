package providers

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestSSEAcceptsLongBoundedLinesAndEOFDispatch(t *testing.T) {
	text := strings.Repeat("x", 128<<10)
	stream := "data: {\"text\":\"" + text + "\"}"
	var got json.RawMessage
	if err := readSSE(context.Background(), strings.NewReader(stream), func(raw json.RawMessage) error { got = cloneRaw(raw); return nil }); err != nil {
		t.Fatal(err)
	}
	var value map[string]json.RawMessage
	if err := decodeUseNumber(got, &value); err != nil || rawString(value["text"]) != text {
		t.Fatalf("long SSE payload was not preserved: bytes=%d err=%v", len(got), err)
	}
}

func TestSSERejectsOversizeEvent(t *testing.T) {
	stream := "data: " + strings.Repeat("x", maxSSEEventBytes+1) + "\n\n"
	if err := readSSE(context.Background(), strings.NewReader(stream), func(json.RawMessage) error { return nil }); err == nil {
		t.Fatal("oversize SSE event accepted")
	}
}
