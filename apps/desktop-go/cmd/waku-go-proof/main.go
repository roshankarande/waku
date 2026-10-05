// Command waku-go-proof runs the phase-1 native surface fixture. It is not the
// release application: it deliberately has no daemon or live-data access.
package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/egoist/mygo"
	"github.com/egoist/mygo/ui"
	"github.com/roshankarande/waku/apps/desktop-go/internal/presentation"
	"github.com/roshankarande/waku/apps/desktop-go/internal/views"
)

type proofDispatcher struct{}

func (proofDispatcher) Dispatch(command presentation.Command) bool {
	log.Printf("phase-1 proof command: kind=%d session=%s bytes=%d", command.Kind, command.SessionID, len(command.Text))
	return true
}

func main() {
	scenario := flag.String("scenario", "long-transcript", "native proof scenario: long-transcript or streaming-reasoning")
	flag.Parse()

	model := proofModel(*scenario)
	shell := views.NewShell(model, proofDispatcher{})
	mygo.App.WhenReady(func() {
		mygo.NewWindow(mygo.WindowOptions{
			Title:   "Waku Go — phase 1 proof",
			Width:   1280,
			Height:  820,
			Content: ui.View(shell.View),
		})
	})
	if err := mygo.App.Run(); err != nil {
		log.Fatal(err)
	}
}

func proofModel(scenario string) *presentation.Model {
	project := presentation.Project{ID: "proof-project", Name: "Synthetic workspace", PathHint: "/synthetic/waku"}
	session := presentation.Session{
		ID: "proof-session", ProjectID: project.ID, Title: "Native surface proof",
		Status: presentation.SessionIdle, Activity: "Ready", UpdatedAt: time.Now(),
	}
	rows := 10_000
	if scenario == "streaming-reasoning" {
		rows = 128
		session.Status = presentation.SessionRunning
		session.Activity = "Reasoning"
	}
	for i := 0; i < rows; i++ {
		role := presentation.RoleAssistant
		if i%5 == 0 {
			role = presentation.RoleTool
		}
		session.Transcript = append(session.Transcript, presentation.TranscriptItem{
			ID: fmt.Sprintf("proof-%05d", i), Role: role,
			Title: fmt.Sprintf("Fixture row %d", i+1),
			Body:  "Prepared synthetic content. No filesystem, network, process, or database work is performed by this view.",
		})
	}
	if scenario == "streaming-reasoning" {
		session.Transcript = append(session.Transcript, presentation.TranscriptItem{
			ID: "streaming-tail", Role: presentation.RoleReasoning, Title: "Working", Streaming: true,
			Body: "A bounded live reasoning tail exercises variable-height FollowEnd behavior while the full document remains prepared off the UI thread.",
		})
	}
	return presentation.NewModel([]presentation.Project{project}, []presentation.Session{session})
}
