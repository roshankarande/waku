package main

import (
	"encoding/json"
	"os"

	_ "github.com/egoist/mygo"
)

var releaseRole = "unconfigured"

func main() {
	if len(os.Args) == 2 && os.Args[1] == "--waku-release-role-probe" {
		_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"schemaVersion": 1, "protocol": "waku-release-role-v1", "role": releaseRole})
	}
}
