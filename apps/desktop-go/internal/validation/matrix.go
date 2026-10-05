package validation

import (
	"fmt"
	"sort"
)

type Classification string

const (
	RequiredParity Classification = "requiredParity"
	Experimental   Classification = "experimental"
	Improvement    Classification = "improvement"
)

type MatrixEntry struct {
	Feature        string         `json:"feature"`
	Classification Classification `json:"classification"`
	Destination    string         `json:"destination"`
	Owner          string         `json:"owner"`
	Test           string         `json:"test"`
	Platforms      []string       `json:"platforms"`
	Providers      []string       `json:"providers"`
	Status         Status         `json:"status"`
	Blocker        string         `json:"blocker,omitempty"`
	Notes          string         `json:"notes,omitempty"`
}

type Matrix struct {
	SchemaVersion int           `json:"schemaVersion"`
	Entries       []MatrixEntry `json:"entries"`
}

func (m Matrix) Validate() error {
	if m.SchemaVersion != SchemaVersion {
		return fmt.Errorf("matrix schemaVersion: got %d", m.SchemaVersion)
	}
	seen := map[string]bool{}
	for i, entry := range m.Entries {
		if entry.Feature == "" || entry.Destination == "" || entry.Owner == "" || entry.Test == "" {
			return fmt.Errorf("entries[%d]: feature, destination, owner, and test are required", i)
		}
		if seen[entry.Feature] {
			return fmt.Errorf("entries[%d]: duplicate feature %q", i, entry.Feature)
		}
		seen[entry.Feature] = true
		switch entry.Classification {
		case RequiredParity, Experimental, Improvement:
		default:
			return fmt.Errorf("entries[%d]: invalid classification %q", i, entry.Classification)
		}
		if len(entry.Platforms) == 0 || len(entry.Providers) == 0 {
			return fmt.Errorf("entries[%d]: platforms and providers are required", i)
		}
		switch entry.Status {
		case StatusPass, StatusFail, StatusBlocked, StatusSkipped, StatusUnrun:
		default:
			return fmt.Errorf("entries[%d]: invalid status %q", i, entry.Status)
		}
		if entry.Status == StatusBlocked && entry.Blocker == "" {
			return fmt.Errorf("entries[%d]: blocked entry requires blocker", i)
		}
	}
	return nil
}

func (m Matrix) Features() []string {
	features := make([]string, 0, len(m.Entries))
	for _, entry := range m.Entries {
		features = append(features, entry.Feature)
	}
	sort.Strings(features)
	return features
}
