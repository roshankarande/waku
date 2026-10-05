package providers

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

func codexActivity(raw json.RawMessage, complete bool) *Activity {
	var item struct {
		ID, Type, Command, Query, Tool, Server string
		Arguments, Result                      json.RawMessage
		Status                                 string
	}
	if decodeUseNumber(raw, &item) != nil {
		return nil
	}
	lower := strings.ToLower(item.Type)
	kind := ""
	switch {
	case strings.Contains(lower, "command"):
		kind = "command"
	case strings.Contains(lower, "filechange") || strings.Contains(lower, "patch"):
		kind = "fileChange"
	case strings.Contains(lower, "websearch"):
		kind = "search"
	case strings.Contains(lower, "plan") || strings.Contains(lower, "todo"):
		kind = "plan"
	case strings.Contains(lower, "tool") || strings.Contains(lower, "collab"):
		kind = classifyTool(item.Tool)
	default:
		return nil
	}
	title := item.Command
	if title == "" && item.Query != "" {
		title = "Search for " + item.Query
	}
	if title == "" && item.Tool != "" {
		var args struct {
			Title string `json:"title"`
		}
		_ = decodeUseNumber(item.Arguments, &args)
		if args.Title != "" {
			title = args.Title
		} else {
			title = splitWords(item.Tool)
		}
	}
	if title == "" {
		title = splitWords(item.Type)
	}
	return &Activity{ID: item.ID, Kind: kind, Title: stripPrivate(title), Tool: item.Tool, MCPServer: item.Server, Complete: complete, Failed: item.Status == "failed", Arguments: cloneRaw(item.Arguments), Output: cloneRaw(item.Result)}
}

func codexBackgroundWork(raw json.RawMessage, complete bool) []*BackgroundWork {
	var item struct {
		ID, Type, Command, ProcessID, Prompt, SenderThreadID string
		ExitCode                                             *int64
		AgentsStates                                         map[string]struct{ Status string } `json:"agentsStates"`
	}
	if decodeUseNumber(raw, &item) != nil {
		return nil
	}
	if item.Type == "commandExecution" && item.ID != "" {
		status := "running"
		if complete {
			status = "completed"
			if item.ExitCode != nil && *item.ExitCode != 0 {
				status = "failed"
			}
		}
		return []*BackgroundWork{{ID: item.ID, Kind: "process", Status: status, Title: stripPrivate(item.Command), ControlID: item.ProcessID, Raw: cloneRaw(raw)}}
	}
	if item.Type == "collabAgentToolCall" {
		ids := make([]string, 0, len(item.AgentsStates))
		for id := range item.AgentsStates {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		works := make([]*BackgroundWork, 0, len(ids))
		for _, id := range ids {
			state := item.AgentsStates[id]
			works = append(works, &BackgroundWork{ID: id, Kind: "subagent", Status: state.Status, Title: stripPrivate(item.Prompt), ParentID: item.SenderThreadID, Raw: cloneRaw(raw)})
		}
		return works
	}
	return nil
}

func classifyTool(name string) string {
	compact := strings.NewReplacer("_", "", "-", "", " ", "").Replace(strings.ToLower(name))
	switch compact {
	case "bash", "command", "execute", "runcommand", "shell", "terminal":
		return "command"
	case "applypatch", "edit", "write", "writefile", "createfile", "deletefile":
		return "fileChange"
	case "read", "readfile", "viewfile":
		return "fileRead"
	case "glob", "grep", "find", "filesearch":
		return "fileSearch"
	case "ls", "listfiles", "readdir":
		return "fileList"
	case "search", "websearch", "webfetch":
		return "search"
	case "todo", "updateplan", "plan":
		return "plan"
	default:
		return "tool"
	}
}

func splitWords(value string) string {
	var out strings.Builder
	for i, r := range value {
		if r == '_' || r == '-' {
			if out.Len() != 0 {
				out.WriteByte(' ')
			}
			continue
		}
		if i > 0 && unicode.IsUpper(r) {
			out.WriteByte(' ')
		}
		if out.Len() == 0 {
			r = unicode.ToUpper(r)
		}
		out.WriteRune(r)
	}
	return out.String()
}

func codexPermission(method string, params map[string]json.RawMessage) *Permission {
	permission := &Permission{Options: []PermissionOption{{ID: "accept", Label: "Allow once", Allow: true}, {ID: "acceptForSession", Label: "Allow for session", Allow: true}, {ID: "decline", Label: "Deny", Allow: false}}}
	if strings.Contains(method, "commandExecution") {
		permission.Title = "Command approval"
		permission.Detail = rawString(params["command"])
		if permission.Detail == "" {
			permission.Detail = "Run a command"
		}
	} else {
		permission.Title = "File change approval"
		permission.Detail = rawString(params["reason"])
		if permission.Detail == "" {
			permission.Detail = "Apply the proposed file changes"
		}
	}
	return permission
}

func codexQuestions(raw json.RawMessage) ([]UserInputQuestion, error) {
	var native []struct {
		ID, Header, Question string
		Options              []struct{ Label, Description string }
		MultiSelect          bool `json:"multiSelect"`
	}
	if err := decodeUseNumber(raw, &native); err != nil {
		return nil, fmt.Errorf("Codex user input questions: %w", err)
	}
	questions := make([]UserInputQuestion, 0, len(native))
	for index, q := range native {
		q.Question = strings.TrimSpace(q.Question)
		if q.Question == "" {
			continue
		}
		if q.ID == "" {
			q.ID = "question-" + strconv.Itoa(index)
		}
		if strings.TrimSpace(q.Header) == "" {
			q.Header = "Question"
		}
		options := make([]UserInputOption, 0, len(q.Options))
		for _, option := range q.Options {
			if label := strings.TrimSpace(option.Label); label != "" {
				options = append(options, UserInputOption{Label: label, Description: strings.TrimSpace(option.Description)})
			}
		}
		questions = append(questions, UserInputQuestion{ID: q.ID, Header: q.Header, Question: q.Question, Options: options, MultiSelect: false})
	}
	if len(questions) == 0 {
		return nil, errors.New("Codex user input request has no valid questions")
	}
	return questions, nil
}
