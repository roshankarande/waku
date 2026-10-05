package providers

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	kimiWireWait = time.Second
	kimiWirePoll = 25 * time.Millisecond
	kimiTailMax  = int64(128 << 10)
)

type kimiWireOffset struct {
	home, session, path string
	offset              int64
}

func kimiWireStart(session string) kimiWireOffset {
	start := kimiWireOffset{home: kimiHome(), session: session}
	if path := kimiWireLog(start.home, session); path != "" {
		start.path = path
		if info, err := os.Stat(path); err == nil {
			start.offset = info.Size()
		}
	}
	return start
}

func kimiHome() string {
	if home := os.Getenv("KIMI_CODE_HOME"); home != "" {
		return home
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".kimi-code")
}

func kimiWireLog(home, session string) string {
	if home == "" || session == "" || filepath.Base(session) != session || strings.ContainsAny(session, `/\\`) {
		return ""
	}
	workspaces, err := os.ReadDir(filepath.Join(home, "sessions"))
	if err != nil {
		return ""
	}
	for _, workspace := range workspaces {
		if !workspace.IsDir() || workspace.Type()&os.ModeSymlink != 0 {
			continue
		}
		candidate := filepath.Join(home, "sessions", workspace.Name(), session, "agents", "main", "wire.jsonl")
		info, err := os.Lstat(candidate)
		if err == nil && info.Mode().IsRegular() {
			return candidate
		}
	}
	return ""
}

func kimiTurnFailure(start kimiWireOffset) string {
	if start.home == "" || start.session == "" {
		return ""
	}
	deadline := time.Now().Add(kimiWireWait)
	for {
		path := start.path
		if path == "" {
			path = kimiWireLog(start.home, start.session)
		}
		if path != "" {
			if failure := readKimiFailure(path, start.offset); failure != "" {
				return failure
			}
		}
		if !time.Now().Before(deadline) {
			return ""
		}
		time.Sleep(kimiWirePoll)
	}
}

func readKimiFailure(path string, offset int64) string {
	file, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= offset {
		return ""
	}
	start := offset
	if tail := info.Size() - kimiTailMax; tail > start {
		start = tail
	}
	if _, err := file.Seek(start, 0); err != nil {
		return ""
	}
	var last map[string]json.RawMessage
	scanner := bufio.NewScanner(file)
	buffer := make([]byte, 64<<10)
	scanner.Buffer(buffer, int(kimiTailMax))
	for scanner.Scan() {
		var record map[string]json.RawMessage
		if decodeUseNumber(scanner.Bytes(), &record) == nil && rawString(record["type"]) == "turn.ended" {
			last = record
		}
	}
	if rawString(last["reason"]) != "failed" {
		return ""
	}
	var providerError struct {
		Message string `json:"message"`
	}
	if decodeUseNumber(last["error"], &providerError) != nil {
		return ""
	}
	return strings.TrimSpace(providerError.Message)
}
