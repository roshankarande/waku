package validation

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
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

type UpgradeStep struct {
	Name        string   `json:"name"`
	Command     string   `json:"command"`
	Assertions  []string `json:"assertions"`
	Destructive bool     `json:"destructive"`
}

type processObservation struct {
	PID      int    `json:"pid"`
	ExitCode int    `json:"exitCode"`
	Stdout   []byte `json:"-"`
	Stderr   []byte `json:"-"`
}
type upgradeRunner interface {
	Run(context.Context, string, ...string) (processObservation, error)
}
type osUpgradeRunner struct{}

func (osUpgradeRunner) Run(ctx context.Context, executable string, args ...string) (processObservation, error) {
	command := exec.CommandContext(ctx, executable, args...)
	for index := 0; index+1 < len(args); index++ {
		if args[index] == "--workdir" {
			command.Dir = args[index+1]
			break
		}
	}
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		return processObservation{}, err
	}
	observation := processObservation{PID: command.Process.Pid}
	err := command.Wait()
	observation.Stdout = stdout.Bytes()
	observation.Stderr = stderr.Bytes()
	if err == nil {
		observation.ExitCode = 0
		return observation, nil
	}
	if exit, ok := err.(*exec.ExitError); ok {
		observation.ExitCode = exit.ExitCode()
		return observation, nil
	}
	return processObservation{}, err
}

type UpgradeConfig struct {
	BaselineExecutable  string
	CandidateExecutable string
}
type UpgradePhaseArtifact struct {
	Phase                 string `json:"phase"`
	ExecutablePath        string `json:"executablePath,omitempty"`
	ExecutableSHA256      string `json:"executableSha256,omitempty"`
	PID                   int    `json:"pid,omitempty"`
	ExitCode              int    `json:"exitCode"`
	StdoutSHA256          string `json:"stdoutSha256,omitempty"`
	StderrSHA256          string `json:"stderrSha256,omitempty"`
	BeforeSchemaSHA256    string `json:"beforeSchemaSha256,omitempty"`
	AfterSchemaSHA256     string `json:"afterSchemaSha256,omitempty"`
	BeforeDataSHA256      string `json:"beforeDataSha256,omitempty"`
	AfterDataSHA256       string `json:"afterDataSha256,omitempty"`
	RequestSHA256         string `json:"requestSha256,omitempty"`
	ResponseSHA256        string `json:"responseSha256,omitempty"`
	PreviousInstallSHA256 string `json:"previousInstallSha256,omitempty"`
	RestoredInstallSHA256 string `json:"restoredInstallSha256,omitempty"`
}
type UpgradeReport struct {
	Status        Status                 `json:"status"`
	Checks        []Check                `json:"checks"`
	RunID         string                 `json:"runId"`
	FixtureSHA256 string                 `json:"fixtureSha256"`
	Artifacts     []UpgradePhaseArtifact `json:"artifacts,omitempty"`
}
type dataSnapshot struct {
	SchemaVersion            int
	SchemaSHA256, DataSHA256 string
}
type executableIdentity struct{ Path, Digest string }

// ExecuteUpgradePlan invokes actual validation-probe entrypoints through a
// non-test process runner. The harness, not the probed executable, captures
// PIDs/exits and computes executable, traffic, install, schema, and data hashes.
// Missing binaries or probe support remain blocked/failed evidence.
func ExecuteUpgradePlan(ctx context.Context, root string, config UpgradeConfig) (UpgradeReport, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return UpgradeReport{}, err
	}
	if abs == string(filepath.Separator) {
		return UpgradeReport{}, fmt.Errorf("refusing broad root")
	}
	if err := os.MkdirAll(abs, 0700); err != nil {
		return UpgradeReport{}, err
	}
	session, err := os.MkdirTemp(abs, "waku-validation-")
	if err != nil {
		return UpgradeReport{}, err
	}
	runID, err := randomHex(16)
	if err != nil {
		return UpgradeReport{}, err
	}
	fixture := filepath.Join(session, "immutable-fixture.json")
	payload := []byte("{\"schemaVersion\":1,\"fixtureType\":\"waku-safe-fixture\",\"data\":{\"sessions\":[{\"id\":\"synthetic\",\"draft\":\"never-live\"}],\"generation\":7}}\n")
	if err := os.WriteFile(fixture, payload, 0400); err != nil {
		return UpgradeReport{}, err
	}
	fixtureDigest := sha256.Sum256(payload)
	fixtureSnapshot, err := snapshotDataFile(fixture)
	if err != nil {
		return UpgradeReport{}, err
	}
	working := filepath.Join(session, "working")
	install := filepath.Join(session, "install")
	if err := os.MkdirAll(working, 0700); err != nil {
		return UpgradeReport{}, err
	}
	if err := os.MkdirAll(install, 0700); err != nil {
		return UpgradeReport{}, err
	}
	dataPath := filepath.Join(working, "data.json")
	if err := os.WriteFile(dataPath, append([]byte(nil), payload...), 0600); err != nil {
		return UpgradeReport{}, err
	}
	report := UpgradeReport{Status: StatusPass, RunID: runID, FixtureSHA256: fmt.Sprintf("%x", fixtureDigest)}
	executableRoot := filepath.Join(session, "executables")
	if err := os.Mkdir(executableRoot, 0700); err != nil {
		return UpgradeReport{}, err
	}
	base, baseErr := inspectUpgradeExecutable(ctx, config.BaselineExecutable, executableRoot, "baseline", "rust")
	candidate, candidateErr := inspectUpgradeExecutable(ctx, config.CandidateExecutable, executableRoot, "candidate", "go")
	ready := baseErr == nil && candidateErr == nil && base.Path != candidate.Path && base.Digest != candidate.Digest
	if !ready {
		reason := fmt.Sprintf("baseline=%v candidate=%v; distinct executable paths and digests required", baseErr, candidateErr)
		report.Checks = append(report.Checks, Check{Name: "upgrade executable identities", Status: StatusBlocked, Blocker: reason})
	} else {
		report.Checks = append(report.Checks, Check{Name: "upgrade executable identities", Status: StatusPass, Evidence: fmt.Sprintf("baseline=%s sha256=%s candidate=%s sha256=%s", base.Path, base.Digest, candidate.Path, candidate.Digest)})
	}
	runner := upgradeRunner(osUpgradeRunner{})
	addBlocked := func(name string) {
		report.Checks = append(report.Checks, Check{Name: name, Status: StatusBlocked, Blocker: "executable identity gate is blocked"})
	}
	if !ready {
		for _, name := range []string{"baseline process probe", "candidate process probe", "exclusive writer lock refusal", "migration and recovery", "wire-v7 protocol verification", "application rollback", "rollback reopen verification"} {
			addBlocked(name)
		}
		return finishUpgradeReport(report, fixture, fixtureDigest)
	}
	runIdentity := func(identity executableIdentity, label string) (processObservation, bool) {
		observation, runErr := runner.Run(ctx, identity.Path, "--waku-validation-probe", "identity", "--run-id", runID, "--build-sha", identity.Digest, "--workdir", working)
		check := Check{Name: label + " process probe"}
		var output struct {
			RunID            string `json:"runId"`
			BuildSHA256      string `json:"buildSha256"`
			WorkingDirectory string `json:"workingDirectory"`
			PID              int    `json:"pid"`
		}
		decodeErr := json.Unmarshal(observation.Stdout, &output)
		if runErr == nil && observation.ExitCode == 2 && decodeErr != nil {
			check.Status = StatusBlocked
			check.Blocker = label + " product binary does not implement --waku-validation-probe identity"
			report.Checks = append(report.Checks, check)
			return observation, false
		}
		if runErr != nil || observation.PID <= 0 || observation.ExitCode != 0 || decodeErr != nil || output.RunID != runID || output.BuildSHA256 != identity.Digest || output.WorkingDirectory != working || output.PID != observation.PID {
			check.Status = StatusFail
			check.Evidence = fmt.Sprintf("runner error=%v decode=%v pid=%d exit=%d", runErr, decodeErr, observation.PID, observation.ExitCode)
			report.Checks = append(report.Checks, check)
			return observation, false
		}
		check.Status = StatusPass
		check.Evidence = fmt.Sprintf("path=%s sha256=%s pid=%d exit=0 stdout=%s stderr=%s", identity.Path, identity.Digest, observation.PID, digestBytes(observation.Stdout), digestBytes(observation.Stderr))
		report.Checks = append(report.Checks, check)
		report.Artifacts = append(report.Artifacts, processArtifact(label+"-identity", identity, observation))
		return observation, true
	}
	_, baseOK := runIdentity(base, "baseline")
	_, candidateOK := runIdentity(candidate, "candidate")
	if !baseOK || !candidateOK {
		return finishUpgradeReport(report, fixture, fixtureDigest)
	}
	lockPath := filepath.Join(working, "writer.lock")
	lockCheck := Check{Name: "exclusive writer lock refusal"}
	unlock, holdErr := holdFileLock(lockPath)
	if holdErr != nil {
		lockCheck.Status = StatusBlocked
		lockCheck.Blocker = "real OS file-lock probe unavailable: " + holdErr.Error()
		report.Checks = append(report.Checks, lockCheck)
	} else {
		lockObs, lockErr := runner.Run(ctx, candidate.Path, "--waku-validation-probe", "lock", lockPath, "--run-id", runID, "--build-sha", candidate.Digest, "--workdir", working)
		_ = unlock()
		if lockErr != nil || lockObs.PID <= 0 || lockObs.ExitCode == 0 || !bytes.Contains(lockObs.Stderr, []byte("DATA_DIRECTORY_LOCKED")) {
			lockCheck.Status = StatusFail
			lockCheck.Evidence = fmt.Sprintf("runner error=%v pid=%d exit=%d stderr=%q", lockErr, lockObs.PID, lockObs.ExitCode, lockObs.Stderr)
		} else {
			lockCheck.Status = StatusPass
			lockCheck.Evidence = fmt.Sprintf("harness-held lock=%s pid=%d exit=%d stderrSha256=%s", lockPath, lockObs.PID, lockObs.ExitCode, digestBytes(lockObs.Stderr))
			report.Artifacts = append(report.Artifacts, processArtifact("lock", candidate, lockObs))
		}
		report.Checks = append(report.Checks, lockCheck)
	}
	_ = os.Remove(lockPath)
	beforeMigration, err := snapshotDataFile(dataPath)
	if err != nil {
		return UpgradeReport{}, err
	}
	migrationObs, migrationErr := runner.Run(ctx, candidate.Path, "--waku-validation-probe", "migrate", dataPath)
	afterMigration, snapshotErr := snapshotDataFile(dataPath)
	migrationCheck := Check{Name: "migration and recovery"}
	failureObs, failureErr := runner.Run(ctx, candidate.Path, "--waku-validation-probe", "migrate-failure", dataPath)
	afterFailure, failureSnapshotErr := snapshotDataFile(dataPath)
	if migrationErr != nil || snapshotErr != nil || failureErr != nil || failureSnapshotErr != nil || migrationObs.PID <= 0 || migrationObs.ExitCode != 0 || failureObs.PID <= 0 || failureObs.ExitCode == 0 || afterMigration.SchemaVersion <= beforeMigration.SchemaVersion || afterMigration.DataSHA256 != fixtureSnapshot.DataSHA256 || afterFailure != afterMigration {
		migrationCheck.Status = StatusFail
		migrationCheck.Evidence = fmt.Sprintf("migrateErr=%v failureErr=%v migrateExit=%d failureExit=%d", migrationErr, failureErr, migrationObs.ExitCode, failureObs.ExitCode)
	} else {
		migrationCheck.Status = StatusPass
		migrationCheck.Evidence = fmt.Sprintf("migration pid=%d failure pid=%d; schema %d->%d; logical data sha256=%s", migrationObs.PID, failureObs.PID, beforeMigration.SchemaVersion, afterMigration.SchemaVersion, afterMigration.DataSHA256)
		report.Artifacts = append(report.Artifacts, UpgradePhaseArtifact{Phase: "migration", ExecutablePath: candidate.Path, ExecutableSHA256: candidate.Digest, PID: migrationObs.PID, ExitCode: migrationObs.ExitCode, BeforeSchemaSHA256: beforeMigration.SchemaSHA256, AfterSchemaSHA256: afterMigration.SchemaSHA256, BeforeDataSHA256: beforeMigration.DataSHA256, AfterDataSHA256: afterMigration.DataSHA256})
	}
	report.Checks = append(report.Checks, migrationCheck)
	protocolNonce, nonceErr := randomHex(16)
	if nonceErr != nil {
		return UpgradeReport{}, nonceErr
	}
	clientObs, clientErr := runner.Run(ctx, base.Path, "--waku-validation-probe", "protocol-client", "--run-id", runID, "--build-sha", base.Digest, "--workdir", working, "--nonce", protocolNonce)
	var clientTraffic struct {
		RunID            string          `json:"runId"`
		BuildSHA256      string          `json:"buildSha256"`
		WorkingDirectory string          `json:"workingDirectory"`
		WireVersion      int             `json:"wireVersion"`
		Request          json.RawMessage `json:"request"`
	}
	clientDecodeErr := json.Unmarshal(clientObs.Stdout, &clientTraffic)
	requestPath := filepath.Join(working, "protocol-request.json")
	if clientDecodeErr == nil {
		_ = os.WriteFile(requestPath, clientTraffic.Request, 0600)
	}
	requestHash := digestBytes(clientTraffic.Request)
	serverObs, serverErr := runner.Run(ctx, candidate.Path, "--waku-validation-probe", "protocol-server", requestPath, "--run-id", runID, "--build-sha", candidate.Digest, "--workdir", working)
	var serverTraffic struct {
		RunID            string          `json:"runId"`
		BuildSHA256      string          `json:"buildSha256"`
		WorkingDirectory string          `json:"workingDirectory"`
		WireVersion      int             `json:"wireVersion"`
		RequestSHA256    string          `json:"requestSha256"`
		RequestNonce     string          `json:"requestNonce"`
		Response         json.RawMessage `json:"response"`
	}
	serverDecodeErr := json.Unmarshal(serverObs.Stdout, &serverTraffic)
	protocolCheck := Check{Name: "wire-v7 protocol verification"}
	var requestEnvelope struct {
		Nonce string `json:"nonce"`
	}
	requestEnvelopeErr := json.Unmarshal(clientTraffic.Request, &requestEnvelope)
	var responseEnvelope struct {
		CorrelationID string `json:"correlationId"`
	}
	responseEnvelopeErr := json.Unmarshal(serverTraffic.Response, &responseEnvelope)
	if clientErr != nil || serverErr != nil || clientObs.PID <= 0 || serverObs.PID <= 0 || clientObs.ExitCode != 0 || serverObs.ExitCode != 0 || clientDecodeErr != nil || serverDecodeErr != nil || requestEnvelopeErr != nil || responseEnvelopeErr != nil || clientTraffic.RunID != runID || serverTraffic.RunID != runID || clientTraffic.BuildSHA256 != base.Digest || serverTraffic.BuildSHA256 != candidate.Digest || clientTraffic.WorkingDirectory != working || serverTraffic.WorkingDirectory != working || clientTraffic.WireVersion != 7 || serverTraffic.WireVersion != 7 || requestEnvelope.Nonce != protocolNonce || serverTraffic.RequestSHA256 != requestHash || serverTraffic.RequestNonce != protocolNonce || responseEnvelope.CorrelationID != protocolNonce || len(clientTraffic.Request) == 0 || len(serverTraffic.Response) == 0 {
		protocolCheck.Status = StatusFail
		protocolCheck.Evidence = fmt.Sprintf("clientErr=%v serverErr=%v clientPid=%d serverPid=%d clientParse=%v serverParse=%v", clientErr, serverErr, clientObs.PID, serverObs.PID, clientDecodeErr, serverDecodeErr)
	} else {
		responseHash := digestBytes(serverTraffic.Response)
		protocolCheck.Status = StatusPass
		protocolCheck.Evidence = fmt.Sprintf("baseline client pid=%d candidate server pid=%d requestSha256=%s responseSha256=%s", clientObs.PID, serverObs.PID, requestHash, responseHash)
		clientArtifact := processArtifact("protocol-client", base, clientObs)
		clientArtifact.RequestSHA256 = requestHash
		serverArtifact := processArtifact("protocol-server", candidate, serverObs)
		serverArtifact.RequestSHA256 = requestHash
		serverArtifact.ResponseSHA256 = responseHash
		report.Artifacts = append(report.Artifacts, clientArtifact, serverArtifact)
	}
	report.Checks = append(report.Checks, protocolCheck)
	installed := filepath.Join(install, "waku")
	if err := copyRegularFile(candidate.Path, installed, 0700); err != nil {
		return UpgradeReport{}, err
	}
	rollbackObs, rollbackErr := runner.Run(ctx, candidate.Path, "--waku-validation-probe", "rollback", installed, base.Path)
	restoredDigest, digestErr := fileSHA256(installed)
	rollbackCheck := Check{Name: "application rollback"}
	if rollbackErr != nil || digestErr != nil || rollbackObs.PID <= 0 || rollbackObs.ExitCode != 0 || restoredDigest != base.Digest {
		rollbackCheck.Status = StatusFail
		rollbackCheck.Evidence = fmt.Sprintf("runner error=%v digest error=%v pid=%d exit=%d restored=%s want=%s", rollbackErr, digestErr, rollbackObs.PID, rollbackObs.ExitCode, restoredDigest, base.Digest)
	} else {
		rollbackCheck.Status = StatusPass
		rollbackCheck.Evidence = fmt.Sprintf("pid=%d exit=0 restored install sha256=%s", rollbackObs.PID, restoredDigest)
		artifact := processArtifact("rollback", candidate, rollbackObs)
		artifact.PreviousInstallSHA256 = base.Digest
		artifact.RestoredInstallSHA256 = restoredDigest
		report.Artifacts = append(report.Artifacts, artifact)
	}
	report.Checks = append(report.Checks, rollbackCheck)
	beforeReopen, err := snapshotDataFile(dataPath)
	if err != nil {
		return UpgradeReport{}, err
	}
	reopenObs, reopenErr := runner.Run(ctx, base.Path, "--waku-validation-probe", "reopen", dataPath)
	afterReopen, snapshotErr := snapshotDataFile(dataPath)
	reopenCheck := Check{Name: "rollback reopen verification"}
	if reopenErr != nil || snapshotErr != nil || reopenObs.PID <= 0 || reopenObs.ExitCode != 0 || afterReopen != beforeReopen {
		reopenCheck.Status = StatusFail
		reopenCheck.Evidence = fmt.Sprintf("runner error=%v snapshot error=%v pid=%d exit=%d", reopenErr, snapshotErr, reopenObs.PID, reopenObs.ExitCode)
	} else {
		reopenCheck.Status = StatusPass
		reopenCheck.Evidence = fmt.Sprintf("baseline pid=%d exit=0 schema=%d dataSha256=%s", reopenObs.PID, afterReopen.SchemaVersion, afterReopen.DataSHA256)
		artifact := processArtifact("reopen", base, reopenObs)
		artifact.BeforeSchemaSHA256 = beforeReopen.SchemaSHA256
		artifact.AfterSchemaSHA256 = afterReopen.SchemaSHA256
		artifact.BeforeDataSHA256 = beforeReopen.DataSHA256
		artifact.AfterDataSHA256 = afterReopen.DataSHA256
		report.Artifacts = append(report.Artifacts, artifact)
	}
	report.Checks = append(report.Checks, reopenCheck)
	return finishUpgradeReport(report, fixture, fixtureDigest)
}

func finishUpgradeReport(report UpgradeReport, fixture string, before [sha256.Size]byte) (UpgradeReport, error) {
	data, err := os.ReadFile(fixture)
	if err != nil {
		return UpgradeReport{}, err
	}
	check := Check{Name: "immutable source fixture"}
	if sha256.Sum256(data) != before {
		check.Status, check.Evidence = StatusFail, "source digest changed"
	} else {
		check.Status, check.Evidence = StatusPass, "source remained read-only and digest-stable"
	}
	report.Checks = append(report.Checks, check)
	for _, item := range report.Checks {
		if item.Status == StatusFail {
			report.Status = StatusFail
			return report, nil
		}
		if item.Status != StatusPass {
			report.Status = StatusBlocked
		}
	}
	return report, nil
}
func inspectExecutable(path string) (executableIdentity, error) {
	if path == "" {
		return executableIdentity{}, fmt.Errorf("path is empty")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return executableIdentity{}, err
	}
	info, err := os.Lstat(absolute)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&0111 == 0 {
		return executableIdentity{}, fmt.Errorf("not an executable regular file")
	}
	digest, err := fileSHA256(absolute)
	return executableIdentity{absolute, digest}, err
}

// inspectUpgradeExecutable admits only product binaries into the production
// upgrade harness. It snapshots the exact bytes that subsequent probes execute,
// then verifies a command-owned role contract and the expected implementation.
// Synthetic programs can exercise lower-level helpers in unit tests, but cannot
// establish upgrade or cross-language interoperability evidence.
func inspectUpgradeExecutable(ctx context.Context, source, snapshotDirectory, name, implementation string) (executableIdentity, error) {
	if err := ctx.Err(); err != nil {
		return executableIdentity{}, err
	}
	identity, err := inspectExecutable(source)
	if err != nil {
		return executableIdentity{}, err
	}
	suffix := filepath.Ext(identity.Path)
	snapshot, digest, cleanup, err := snapshotRegularFile(identity.Path, suffix, "waku-upgrade-executable-")
	if err != nil {
		return executableIdentity{}, err
	}
	defer cleanup()
	destination := filepath.Join(snapshotDirectory, name+suffix)
	if err := copyRegularFile(snapshot, destination, 0500); err != nil {
		return executableIdentity{}, err
	}
	privateIdentity, err := inspectExecutable(destination)
	if err != nil || privateIdentity.Digest != digest {
		return executableIdentity{}, errors.New("private executable snapshot digest mismatch")
	}
	probe, err := probeApplicationRole(privateIdentity.Path)
	if err != nil {
		return executableIdentity{}, fmt.Errorf("desktop release-role probe unavailable or invalid: %w", err)
	}
	if probe.Role != "desktop" {
		return executableIdentity{}, errors.New("release-role probe did not identify a desktop binary")
	}
	info, buildInfoErr := buildinfo.ReadFile(privateIdentity.Path)
	switch implementation {
	case "go":
		if buildInfoErr != nil || info.Main.Path != "github.com/roshankarande/waku/apps/desktop-go" {
			return executableIdentity{}, errors.New("candidate is not the Waku Go desktop product binary")
		}
		cgoDisabled := false
		for _, setting := range info.Settings {
			if setting.Key == "CGO_ENABLED" && setting.Value == "0" {
				cgoDisabled = true
			}
		}
		if !cgoDisabled {
			return executableIdentity{}, errors.New("candidate was not built with CGO_ENABLED=0")
		}
	case "rust":
		if buildInfoErr == nil {
			return executableIdentity{}, errors.New("baseline is a Go binary, not the required Rust desktop")
		}
	default:
		return executableIdentity{}, errors.New("unsupported upgrade implementation")
	}
	return privateIdentity, nil
}
func processArtifact(phase string, identity executableIdentity, observation processObservation) UpgradePhaseArtifact {
	return UpgradePhaseArtifact{Phase: phase, ExecutablePath: identity.Path, ExecutableSHA256: identity.Digest, PID: observation.PID, ExitCode: observation.ExitCode, StdoutSHA256: digestBytes(observation.Stdout), StderrSHA256: digestBytes(observation.Stderr)}
}
func digestBytes(data []byte) string { sum := sha256.Sum256(data); return fmt.Sprintf("%x", sum) }
func copyRegularFile(source, destination string, mode os.FileMode) error {
	data, err := os.ReadFile(source)
	if err != nil {
		return err
	}
	return os.WriteFile(destination, data, mode)
}
func snapshotDataFile(path string) (dataSnapshot, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		return dataSnapshot{}, fmt.Errorf("working data is missing or not a regular file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return dataSnapshot{}, err
	}
	var document struct {
		SchemaVersion int             `json:"schemaVersion"`
		FixtureType   string          `json:"fixtureType"`
		Data          json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(data, &document); err != nil || document.SchemaVersion <= 0 || document.FixtureType == "" || len(document.Data) == 0 {
		return dataSnapshot{}, fmt.Errorf("data file schema is invalid")
	}
	var logical any
	if err := json.Unmarshal(document.Data, &logical); err != nil {
		return dataSnapshot{}, err
	}
	canonicalData, _ := json.Marshal(logical)
	canonicalSchema, _ := json.Marshal(struct {
		SchemaVersion int    `json:"schemaVersion"`
		FixtureType   string `json:"fixtureType"`
	}{document.SchemaVersion, document.FixtureType})
	schemaDigest := sha256.Sum256(canonicalSchema)
	dataDigest := sha256.Sum256(canonicalData)
	return dataSnapshot{document.SchemaVersion, fmt.Sprintf("%x", schemaDigest), fmt.Sprintf("%x", dataDigest)}, nil
}
func randomHex(size int) (string, error) {
	value := make([]byte, size)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return hex.EncodeToString(value), nil
}

type UpgradeRollbackPlan struct {
	SchemaVersion int           `json:"schemaVersion"`
	Fixture       string        `json:"fixture"`
	WorkingCopy   string        `json:"workingCopy"`
	DataLockProof string        `json:"dataLockProof"`
	Steps         []UpgradeStep `json:"steps"`
}

func (p UpgradeRollbackPlan) Validate() error {
	if p.SchemaVersion != SchemaVersion {
		return fmt.Errorf("plan schemaVersion: got %d", p.SchemaVersion)
	}
	if p.Fixture == "" || p.WorkingCopy == "" || p.DataLockProof == "" {
		return fmt.Errorf("fixture, workingCopy, and dataLockProof are required")
	}
	if p.Fixture == p.WorkingCopy {
		return fmt.Errorf("workingCopy must differ from immutable fixture")
	}
	for i, step := range p.Steps {
		if step.Name == "" || step.Command == "" || len(step.Assertions) == 0 {
			return fmt.Errorf("steps[%d]: name, command, and assertions required", i)
		}
		lower := strings.ToLower(step.Command)
		if strings.Contains(lower, "production") || strings.Contains(lower, "$home") || strings.Contains(lower, "~/.waku") {
			return fmt.Errorf("steps[%d]: command may target live data", i)
		}
		if step.Destructive {
			return fmt.Errorf("steps[%d]: destructive fixture steps are not permitted", i)
		}
	}
	return nil
}
