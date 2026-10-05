package protocol

import "encoding/json"

type PromptCommand struct {
	Type      string `json:"type"`
	Prompt    string `json:"prompt"`
	TurnID    *UUID  `json:"turnId,omitempty"`
	MessageID *UUID  `json:"messageId,omitempty"`
}

func Prompt(prompt string, turnID, messageID *UUID) PromptCommand {
	return PromptCommand{Type: "prompt", Prompt: prompt, TurnID: turnID, MessageID: messageID}
}

type WriteTerminalCommand struct {
	Type string      `json:"type"`
	Data Base64Bytes `json:"data"`
}

func WriteTerminal(data []byte) WriteTerminalCommand {
	return WriteTerminalCommand{Type: "writeTerminal", Data: data}
}

// RawObject decodes opaque provider payloads with json.Number leaves.
func RawObject(raw json.RawMessage) (map[string]any, error) {
	var value map[string]any
	if err := Decode(raw, &value); err != nil {
		return nil, err
	}
	return value, nil
}
