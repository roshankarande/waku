package benchmarks

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/roshankarande/waku/apps/desktop-go/internal/validation"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"time"
)

type ReleaseRunConfig struct {
	BaselineExecutable    string
	CandidateExecutable   string
	GUIBlocker            string
	BaselineBlocker       string
	Budgets               validation.Budgets
	OutputRoot            string
	GUIObserverExecutable string
}

type guiObservation struct {
	SchemaVersion int       `json:"schemaVersion"`
	Protocol      string    `json:"protocol"`
	CapturedAt    time.Time `json:"capturedAt"`
	Platform      string    `json:"platform"`
	Display       string    `json:"display"`
	Renderer      string    `json:"renderer"`
	ObserverPID   int       `json:"observerPid"`
}

type benchmarkDriverStatus struct {
	SchemaVersion int    `json:"schemaVersion"`
	Protocol      string `json:"protocol"`
	Status        string `json:"status"`
	RunNonce      string `json:"runNonce"`
	Blocker       string `json:"blocker"`
}
type ExecutableEvidence struct {
	Path            string `json:"path"`
	SHA256          string `json:"sha256"`
	Role            string `json:"role"`
	Implementation  string `json:"implementation"`
	Instrumentation string `json:"instrumentation"`
}
type ReleaseRunReport struct {
	Status      validation.Status                `json:"status"`
	Checks      []validation.Check               `json:"checks"`
	Comparisons map[string]validation.Comparison `json:"comparisons,omitempty"`
	Executables map[string]ExecutableEvidence    `json:"executables,omitempty"`
	RunRoot     string                           `json:"runRoot,omitempty"`
}

type benchmarkIdentity struct {
	SchemaVersion   int    `json:"schemaVersion"`
	Protocol        string `json:"protocol"`
	Role            string `json:"role"`
	Implementation  string `json:"implementation"`
	Instrumentation string `json:"instrumentation"`
}

func RunReleaseBenchmarks(ctx context.Context, config ReleaseRunConfig) (ReleaseRunReport, error) {
	report := ReleaseRunReport{Status: validation.StatusPass, Comparisons: map[string]validation.Comparison{}, Executables: map[string]ExecutableEvidence{}}
	block := func(name, reason string) {
		report.Checks = append(report.Checks, validation.Check{Name: name, Status: validation.StatusBlocked, Blocker: reason})
		report.Status = validation.StatusBlocked
	}
	if config.BaselineBlocker != "" {
		block("Rust baseline executable", config.BaselineBlocker)
		return report, nil
	}
	observation, observerDigest, err := observeNativeGUI(ctx, config.GUIObserverExecutable)
	if err != nil {
		reason := config.GUIBlocker
		if reason == "" {
			reason = err.Error()
		}
		block("release GUI benchmark capability", reason)
		return report, nil
	}
	report.Checks = append(report.Checks, validation.Check{Name: "external GUI observer", Status: validation.StatusPass, Evidence: fmt.Sprintf("observer=%s platform=%s display=%s renderer=%s", observerDigest, observation.Platform, observation.Display, observation.Renderer)})
	root := config.OutputRoot
	if root != "" {
		if err := os.MkdirAll(root, 0700); err != nil {
			return report, err
		}
	}
	root, err = os.MkdirTemp(root, "waku-release-bench-")
	if err != nil {
		return report, err
	}
	report.RunRoot = root
	executionPaths := map[string]string{}
	for name, path := range map[string]string{"baseline": config.BaselineExecutable, "candidate": config.CandidateExecutable} {
		privatePath, digest, err := snapshotExecutable(path, filepath.Join(root, ".executables"), name)
		if err != nil {
			block(name+" executable", "required executable is unavailable")
			return report, nil
		}
		identity, inspectedDigest, err := inspectBenchmarkExecutable(ctx, privatePath)
		if err != nil {
			block(name+" executable identity", err.Error())
			return report, nil
		}
		wantImplementation := map[string]string{"baseline": "rust", "candidate": "go"}[name]
		if identity.Role != "desktop" || identity.Implementation != wantImplementation || identity.Instrumentation == "" {
			block(name+" executable identity", "command-specific benchmark identity does not prove desktop/"+wantImplementation+" instrumentation")
			return report, nil
		}
		if digest != inspectedDigest {
			return report, errors.New("immutable executable snapshot digest changed")
		}
		executionPaths[name] = privatePath
		report.Executables[name] = ExecutableEvidence{Path: path, SHA256: digest, Role: identity.Role, Implementation: identity.Implementation, Instrumentation: identity.Instrumentation}
	}
	for _, scenario := range Scenarios {
		fixture := BuildFixture(scenario)
		if fixture.SHA256 != validation.KnownWorkloadDigests[scenario.ID] {
			return report, fmt.Errorf("fixture digest drift for %s", scenario.ID)
		}
		work := validation.Workload{ID: scenario.ID, FixtureSHA256: fixture.SHA256, Warmups: 1, Iterations: 5, TranscriptMessages: scenario.TranscriptRows, ReasoningBytes: int64(scenario.ReasoningBytes), DiffLines: scenario.DiffLines, TerminalBytes: int64(scenario.TerminalBytes), StreamBytes: int64(scenario.StreamBytes), StreamIntervalMS: scenario.StreamIntervalMS}
		scenarioRoot, err := os.MkdirTemp(root, scenario.ID+"-")
		if err != nil {
			return report, err
		}
		fixturePath := filepath.Join(scenarioRoot, "fixture.bin")
		if err := os.WriteFile(fixturePath, fixture.Data, 0600); err != nil {
			return report, err
		}
		results := map[string]validation.Result{}
		artifacts := map[string]validation.BaselineArtifacts{}
		for _, item := range []struct{ name, path string }{{"baseline", executionPaths["baseline"]}, {"candidate", executionPaths["candidate"]}} {
			nonce, err := cryptographicNonce()
			if err != nil {
				return report, err
			}
			runRoot := filepath.Join(scenarioRoot, item.name+"-"+nonce)
			if err := os.Mkdir(runRoot, 0700); err != nil {
				return report, err
			}
			eventsPath, samplesPath, environmentPath := filepath.Join(runRoot, "events.ndjson"), filepath.Join(runRoot, "samples.ndjson"), filepath.Join(runRoot, "environment.json")
			for _, output := range []string{eventsPath, samplesPath, environmentPath} {
				if _, err := os.Lstat(output); !os.IsNotExist(err) {
					return report, errors.New("benchmark output path was not fresh")
				}
			}
			started := time.Now().UTC()
			command := exec.CommandContext(ctx, item.path, "--waku-benchmark-driver", "--run-nonce", nonce, "--workload", scenario.ID, "--fixture", fixturePath, "--warmups", "1", "--iterations", "5", "--events", eventsPath, "--samples", samplesPath, "--environment", environmentPath)
			command.Dir = runRoot
			if output, err := command.CombinedOutput(); err != nil {
				if blocked, ok := decodeDriverBlocked(output, err, nonce); ok {
					block(scenario.ID+" "+item.name+" driver", blocked.Blocker)
					return report, nil
				}
				report.Checks = append(report.Checks, validation.Check{Name: scenario.ID + " " + item.name + " driver", Status: validation.StatusFail, Evidence: fmt.Sprintf("%v: %s", err, output)})
				report.Status = validation.StatusFail
				return report, nil
			}
			finished := time.Now().UTC()
			if err := validateFreshOutputs([]string{eventsPath, samplesPath, environmentPath}, started, finished); err != nil {
				return report, err
			}
			result, err := aggregateReleaseEvidence(eventsPath, samplesPath, environmentPath, work, nonce, started, finished, report.Executables[item.name])
			if err != nil {
				return report, err
			}
			results[item.name] = result
			artifacts[item.name] = validation.BaselineArtifacts{EventsPath: eventsPath, SamplesPath: samplesPath}
		}
		comparison, err := validation.CompareResultsWithArtifacts(results["baseline"], results["candidate"], config.Budgets, artifacts["baseline"])
		if err != nil {
			return report, err
		}
		report.Comparisons[scenario.ID] = comparison
		report.Checks = append(report.Checks, comparison.Checks...)
		if comparison.Status == validation.StatusFail {
			report.Status = validation.StatusFail
		} else if comparison.Status != validation.StatusPass && report.Status == validation.StatusPass {
			report.Status = validation.StatusBlocked
		}
	}
	return report, nil
}

func aggregateReleaseEvidence(eventsPath, samplesPath, environmentPath string, work validation.Workload, nonce string, started, finished time.Time, executable ExecutableEvidence) (validation.Result, error) {
	eventData, err := os.ReadFile(eventsPath)
	if err != nil {
		return validation.Result{}, err
	}
	sampleData, err := os.ReadFile(samplesPath)
	if err != nil {
		return validation.Result{}, err
	}
	environmentData, err := os.ReadFile(environmentPath)
	if err != nil {
		return validation.Result{}, err
	}
	var environment validation.Environment
	if err := json.Unmarshal(environmentData, &environment); err != nil {
		return validation.Result{}, err
	}
	if environment.CapturedAt.Before(started) || environment.CapturedAt.After(finished) || environment.Build != executable.Implementation || environment.Instrumentation != executable.Instrumentation {
		return validation.Result{}, errors.New("environment evidence is stale or does not match executable identity")
	}
	events, err := validation.DecodeEvents(bytes.NewReader(eventData))
	if err != nil {
		return validation.Result{}, err
	}
	if err := validateEvidenceRunIDs(events, nil, nonce); err != nil {
		return validation.Result{}, err
	}
	metrics, _, err := validation.MetricsFromEvents(events, environment.Build, work.ID, work.FixtureSHA256)
	if err != nil {
		return validation.Result{}, err
	}
	samples, err := validation.DecodeSamples(bytes.NewReader(sampleData))
	if err != nil {
		return validation.Result{}, err
	}
	if err := validation.ValidateEvidenceCoverage(events, samples, work); err != nil {
		return validation.Result{}, err
	}
	if err := validateEvidenceRunIDs(nil, samples, nonce); err != nil {
		return validation.Result{}, err
	}
	if _, err := validation.AddSamples(&metrics, samples, environment.Build, work.ID, work.FixtureSHA256); err != nil {
		return validation.Result{}, err
	}
	result := validation.NewResult(environment, work)
	result.Metrics = metrics
	result.Checks = validation.InstrumentationChecks(metrics, work.Iterations)
	result.Evidence = validation.RawEvidence{EventsSHA256: digest(eventData), SamplesSHA256: digest(sampleData)}
	if err := result.Validate(); err != nil {
		return validation.Result{}, err
	}
	if result.Metrics.InputToPresentMS.Count < work.Iterations || result.Metrics.FramePresentedMS.Count < work.Iterations || result.Metrics.CPUPercent.Count < work.Iterations || result.Metrics.RSSBytes.Count < work.Iterations {
		return validation.Result{}, errors.New("native presentation and resource evidence is incomplete")
	}
	return result, nil
}
func digest(data []byte) string { return fmt.Sprintf("%x", sha256.Sum256(data)) }

func cryptographicNonce() (string, error) {
	var raw [24]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

func snapshotExecutable(source, directory, name string) (string, string, error) {
	before, err := os.Lstat(source)
	if err != nil || !before.Mode().IsRegular() || before.Mode()&0111 == 0 {
		return "", "", errors.New("source is not a regular executable")
	}
	input, err := os.Open(source)
	if err != nil {
		return "", "", err
	}
	opened, err := input.Stat()
	if err != nil || !os.SameFile(before, opened) {
		input.Close()
		return "", "", errors.New("executable changed before snapshot")
	}
	if err := os.MkdirAll(directory, 0700); err != nil {
		input.Close()
		return "", "", err
	}
	destination := filepath.Join(directory, name)
	output, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, before.Mode().Perm())
	if err != nil {
		input.Close()
		return "", "", err
	}
	hash := sha256.New()
	_, copyErr := io.Copy(io.MultiWriter(output, hash), input)
	closeOut, closeIn := output.Close(), input.Close()
	if copyErr != nil || closeOut != nil || closeIn != nil {
		return "", "", errors.Join(copyErr, closeOut, closeIn)
	}
	after, err := os.Lstat(source)
	if err != nil || !os.SameFile(before, after) || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		return "", "", errors.New("executable changed while snapshotting")
	}
	return destination, hex.EncodeToString(hash.Sum(nil)), nil
}

func observeNativeGUI(ctx context.Context, executable string) (guiObservation, string, error) {
	if executable == "" {
		return guiObservation{}, "", errors.New("external GUI observer executable is required")
	}
	directory, err := os.MkdirTemp("", "waku-gui-observer-")
	if err != nil {
		return guiObservation{}, "", err
	}
	defer os.RemoveAll(directory)
	private, digest, err := snapshotExecutable(executable, directory, "observer")
	if err != nil {
		return guiObservation{}, "", err
	}
	started := time.Now().UTC()
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	command := exec.CommandContext(probeCtx, private, "--waku-gui-observer")
	var stdout bytes.Buffer
	command.Stdout = &stdout
	if err := command.Start(); err != nil {
		return guiObservation{}, "", fmt.Errorf("external GUI observer: %w", err)
	}
	observerPID := command.Process.Pid
	err = command.Wait()
	output := stdout.Bytes()
	finished := time.Now().UTC()
	if err != nil {
		return guiObservation{}, "", fmt.Errorf("external GUI observer: %w", err)
	}
	var observation guiObservation
	decoder := json.NewDecoder(bytes.NewReader(output))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&observation); err != nil {
		return guiObservation{}, "", err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return guiObservation{}, "", errors.New("external GUI observer emitted trailing output")
	}
	if observation.SchemaVersion != validation.SchemaVersion || observation.Protocol != "waku-gui-observer-v1" || observation.Platform != runtime.GOOS || observation.Display == "" || observation.Renderer == "" || observation.ObserverPID != observerPID || observation.CapturedAt.Before(started) || observation.CapturedAt.After(finished) {
		return guiObservation{}, "", errors.New("external GUI observer evidence is invalid or stale")
	}
	return observation, digest, nil
}

func decodeDriverBlocked(output []byte, runErr error, nonce string) (benchmarkDriverStatus, bool) {
	var exitErr *exec.ExitError
	if !errors.As(runErr, &exitErr) || exitErr.ExitCode() != 3 {
		return benchmarkDriverStatus{}, false
	}
	var status benchmarkDriverStatus
	decoder := json.NewDecoder(bytes.NewReader(output))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&status) != nil {
		return benchmarkDriverStatus{}, false
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return benchmarkDriverStatus{}, false
	}
	if status.SchemaVersion != validation.SchemaVersion || status.Protocol != "waku-benchmark-driver-v1" || status.Status != "blocked" || status.RunNonce != nonce || status.Blocker == "" {
		return benchmarkDriverStatus{}, false
	}
	return status, true
}

func validateFreshOutputs(paths []string, started, finished time.Time) error {
	for _, path := range paths {
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() {
			return errors.New("benchmark output is missing, non-regular, or a symlink")
		}
		if info.ModTime().Before(started.Add(-2*time.Second)) || info.ModTime().After(finished.Add(2*time.Second)) {
			return errors.New("benchmark output timestamp is stale or outside the run window")
		}
	}
	return nil
}

func validateEvidenceRunIDs(events []validation.Event, samples []validation.Sample, nonce string) error {
	for _, event := range events {
		if event.RunID != nonce {
			return errors.New("event runId is not exactly nonce-bound")
		}
	}
	for _, sample := range samples {
		if sample.RunID != nonce {
			return errors.New("sample runId is not exactly nonce-bound")
		}
	}
	return nil
}

func inspectBenchmarkExecutable(ctx context.Context, path string) (benchmarkIdentity, string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return benchmarkIdentity{}, "", err
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(data))
	probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	output, err := exec.CommandContext(probeCtx, path, "--waku-benchmark-identity").Output()
	if err != nil {
		return benchmarkIdentity{}, "", fmt.Errorf("benchmark identity probe: %w", err)
	}
	var identity benchmarkIdentity
	decoder := json.NewDecoder(bytes.NewReader(output))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&identity); err != nil {
		return identity, "", err
	}
	if identity.SchemaVersion != validation.SchemaVersion || identity.Protocol != "waku-benchmark-driver-v1" {
		return identity, "", errors.New("invalid benchmark identity protocol")
	}
	goInfo, goErr := buildinfo.ReadFile(path)
	if identity.Implementation == "go" {
		if goErr != nil {
			return identity, "", errors.New("Go candidate identity is not backed by Go build metadata")
		}
		cgo := ""
		for _, setting := range goInfo.Settings {
			if setting.Key == "CGO_ENABLED" {
				cgo = setting.Value
			}
		}
		if cgo != "0" {
			return identity, "", errors.New("Go candidate benchmark is not CGO_ENABLED=0")
		}
	} else if identity.Implementation == "rust" && goErr == nil {
		return identity, "", errors.New("Rust baseline identity is contradicted by Go build metadata")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return identity, "", errors.New("benchmark identity emitted trailing output")
	}
	return identity, digest, nil
}
