package benchmarks

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"
)

func TestScenariosAreStableAndNonEmpty(t *testing.T) {
	seen := map[string]bool{}
	for _, scenario := range Scenarios {
		if scenario.ID == "" || seen[scenario.ID] {
			t.Fatalf("invalid scenario ID %q", scenario.ID)
		}
		seen[scenario.ID] = true
		first, second := BuildFixture(scenario), BuildFixture(scenario)
		if first.SHA256 != second.SHA256 || !bytes.Equal(first.Data, second.Data) || len(first.Data) == 0 {
			t.Fatalf("scenario %s is not deterministic", scenario.ID)
		}
	}
}

func TestCheckedInWorkloadDigestsMatchGenerator(t *testing.T) {
	data, err := os.ReadFile("workloads.json")
	if err != nil {
		t.Fatal(err)
	}
	var checked []struct {
		ID, FixtureSHA256 string
		Bytes             int
	}
	if err := json.Unmarshal(data, &checked); err != nil {
		t.Fatal(err)
	}
	if len(checked) != len(Scenarios) {
		t.Fatalf("got %d workload records, want %d", len(checked), len(Scenarios))
	}
	for i, scenario := range Scenarios {
		fixture := BuildFixture(scenario)
		if checked[i].ID != scenario.ID || checked[i].FixtureSHA256 != fixture.SHA256 || checked[i].Bytes != len(fixture.Data) {
			t.Fatalf("workload %d is stale: %+v", i, checked[i])
		}
	}
}

func BenchmarkFixtureConstruction(b *testing.B) {
	for _, scenario := range Scenarios {
		b.Run(scenario.ID, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				fixture := BuildFixture(scenario)
				if len(fixture.Data) == 0 {
					b.Fatal("empty fixture")
				}
			}
		})
	}
}
