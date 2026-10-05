package workspace

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func newTestSkillsService(t *testing.T, limits SkillsLimits) (*SkillsService, string, string) {
	t.Helper()
	base := t.TempDir()
	home := filepath.Join(base, "daemon-home")
	trash := filepath.Join(base, "recoverable-trash")
	if err := os.MkdirAll(home, 0o700); err != nil {
		t.Fatal(err)
	}
	homePath, err := NewHostPath(home)
	if err != nil {
		t.Fatal(err)
	}
	trashPath, err := NewHostPath(trash)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewSkillsService(homePath, nil, trashPath, limits)
	if err != nil {
		t.Fatal(err)
	}
	return service, home, trash
}

func writeTestSkill(t *testing.T, root, name, document string) string {
	t.Helper()
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, SkillFile), []byte(document), 0o600); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestSkillsLocationsUseDaemonHostRoots(t *testing.T) {
	service, home, _ := newTestSkillsService(t, SkillsLimits{})
	projectRoot := filepath.Join(t.TempDir(), "project")
	if err := os.MkdirAll(projectRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	projectPath, err := NewHostPath(projectRoot)
	if err != nil {
		t.Fatal(err)
	}
	locations := service.Locations([]SkillProject{{Name: "waku", Root: projectPath}})

	if got, want := locations[0].Root, filepath.Join(home, ".agents", "skills"); got != want {
		t.Fatalf("first user root = %q, want %q", got, want)
	}
	if locations[0].Scope != SkillScopeUser || locations[0].Source.Provider != "" {
		t.Fatalf("unexpected preferred root: %#v", locations[0])
	}
	wantProject := filepath.Join(projectRoot, ".codex", "skills")
	found := false
	for _, location := range locations {
		if location.Root == wantProject && location.Project != nil && *location.Project == "waku" {
			found = true
		}
	}
	if !found {
		t.Fatalf("missing project catalog root %q", wantProject)
	}
}

func TestSkillsScanMatchesProtocolShapeAndGroupsCopies(t *testing.T) {
	service, home, _ := newTestSkillsService(t, SkillsLimits{})
	shared := filepath.Join(home, ".agents", "skills")
	codex := filepath.Join(home, ".codex", "skills")
	document := "---\nname: review\ndescription: >-\n  Review the changed code\n  and flag risks.\nallowed-tools: Read, Bash\n---\nCheck carefully."
	sharedDir := writeTestSkill(t, shared, "review", document)
	writeTestSkill(t, codex, "review", document)
	if err := os.WriteFile(filepath.Join(sharedDir, "runbook.md"), []byte("support"), 0o600); err != nil {
		t.Fatal(err)
	}

	catalog, err := service.Scan(context.Background(), service.Locations(nil))
	if err != nil {
		t.Fatal(err)
	}
	if len(catalog.Skills) != 1 {
		t.Fatalf("skills = %d, want one grouped entry", len(catalog.Skills))
	}
	skill := catalog.Skills[0]
	if skill.Description != "Review the changed code and flag risks." || skill.Body != "Check carefully." {
		t.Fatalf("frontmatter/body mismatch: %#v", skill)
	}
	if skill.AllowedTools == nil || *skill.AllowedTools != "Read, Bash" || skill.SupportingFiles != 1 {
		t.Fatalf("metadata mismatch: %#v", skill)
	}
	if len(skill.Installs) != 2 || !skill.Enabled || skill.RowKey == 0 || skill.ModifiedAt == nil {
		t.Fatalf("grouped installs mismatch: %#v", skill)
	}

	encoded, err := json.Marshal(catalog)
	if err != nil {
		t.Fatal(err)
	}
	text := string(encoded)
	for _, fragment := range []string{`"source":"shared"`, `"source":{"provider":"codex"}`, `"skillFile":`, `"allowedTools":`, `"supportingFiles":`, `"modifiedAt":`, `"rowKey":`} {
		if !strings.Contains(text, fragment) {
			t.Errorf("catalog JSON %s does not contain %s", text, fragment)
		}
	}
}

func TestSkillsScanFollowsLinkedCatalogChildrenAndBoundsReads(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation needs privileges on Windows")
	}
	limits := DefaultSkillsLimits()
	limits.MaxSkillBytes = 64
	limits.MaxTreeEntries = 3
	service, home, _ := newTestSkillsService(t, limits)
	root := filepath.Join(home, ".agents", "skills")
	targetRoot := filepath.Join(t.TempDir(), "linked-targets")
	target := writeTestSkill(t, targetRoot, "linked", "---\nname: linked\n---\nBody")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(root, "linked")); err != nil {
		t.Fatal(err)
	}
	writeTestSkill(t, root, "oversized", strings.Repeat("x", 65))
	for i := 0; i < 10; i++ {
		if err := os.WriteFile(filepath.Join(target, "file-"+string(rune('a'+i))), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	catalog, err := service.Scan(context.Background(), service.Locations(nil))
	if err != nil {
		t.Fatal(err)
	}
	if len(catalog.Skills) != 1 || catalog.Skills[0].Name != "linked" {
		t.Fatalf("unexpected bounded symlink catalog: %#v", catalog)
	}
	if catalog.Skills[0].SupportingFiles > limits.MaxTreeEntries {
		t.Fatalf("tree accounting exceeded bound: %#v", catalog.Skills[0])
	}
}

func TestSetEnabledUsesAtomicRenamesAndRejectsNestedMutation(t *testing.T) {
	service, home, _ := newTestSkillsService(t, SkillsLimits{})
	root := filepath.Join(home, ".agents", "skills")
	dir := writeTestSkill(t, root, "deploy", "---\nname: deploy\n---\nShip")
	locations := service.Locations(nil)

	if err := service.SetEnabled(context.Background(), locations, []string{dir}, false); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, DisabledSkillFile)); err != nil {
		t.Fatalf("disabled marker missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, SkillFile)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("live marker still exists: %v", err)
	}
	if err := service.SetEnabled(context.Background(), locations, []string{dir}, true); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, DisabledSkillFile), []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := service.SetEnabled(context.Background(), locations, []string{dir}, false); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(filepath.Join(dir, DisabledSkillFile))
	if err != nil || string(contents) != "---\nname: deploy\n---\nShip" {
		t.Fatalf("live marker did not replace stale disabled marker: %q, %v", contents, err)
	}
	if err := service.SetEnabled(context.Background(), locations, []string{dir}, true); err != nil {
		t.Fatal(err)
	}
	if err := service.SetEnabled(context.Background(), locations, []string{filepath.Join(dir, "nested")}, false); !errors.Is(err, ErrSkillOutsideCatalog) {
		t.Fatalf("nested mutation error = %v", err)
	}
}

func TestSetEnabledRollsBackBatchFailure(t *testing.T) {
	service, home, _ := newTestSkillsService(t, SkillsLimits{})
	root := filepath.Join(home, ".agents", "skills")
	first := writeTestSkill(t, root, "first", "one")
	second := writeTestSkill(t, root, "second", "two")
	calls := 0
	service.beforeRename = func(_, _ string) error {
		calls++
		if calls == 2 {
			return errors.New("injected rename failure")
		}
		return nil
	}
	err := service.SetEnabled(context.Background(), service.Locations(nil), []string{first, second}, false)
	if err == nil {
		t.Fatal("expected injected failure")
	}
	for _, dir := range []string{first, second} {
		if _, err := os.Stat(filepath.Join(dir, SkillFile)); err != nil {
			t.Fatalf("batch rollback did not restore %s: %v", dir, err)
		}
	}
}

func TestTrashAndRecoverAreRenameOnlyAndCollisionSafe(t *testing.T) {
	service, home, trash := newTestSkillsService(t, SkillsLimits{})
	root := filepath.Join(home, ".agents", "skills")
	first := writeTestSkill(t, root, "first", "one")
	second := writeTestSkill(t, root, "second", "two")
	locations := service.Locations(nil)

	recovery, err := service.Trash(context.Background(), locations, []string{first, second})
	if err != nil {
		t.Fatal(err)
	}
	if len(recovery.Items) != 2 || recovery.Items[0].Identity == recovery.Items[1].Identity {
		t.Fatalf("invalid recovery identities: %#v", recovery)
	}
	for _, item := range recovery.Items {
		if _, err := os.Lstat(item.OriginalDir); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("original still exists: %v", err)
		}
		if _, err := os.Lstat(filepath.Join(trash, item.Identity)); err != nil {
			t.Fatalf("recoverable trash item missing: %v", err)
		}
	}
	if err := service.Recover(context.Background(), locations, recovery); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{first, second} {
		if _, err := os.Stat(filepath.Join(dir, SkillFile)); err != nil {
			t.Fatalf("recovered skill missing: %v", err)
		}
	}
}

func TestTrashRecoveryMetadataSurvivesServiceRestart(t *testing.T) {
	service, home, trash := newTestSkillsService(t, SkillsLimits{})
	dir := writeTestSkill(t, filepath.Join(home, ".agents", "skills"), "restart", "---\nname: restart\n---\nbody")
	locations := service.Locations(nil)
	if _, err := service.Trash(context.Background(), locations, []string{dir}); err != nil {
		t.Fatal(err)
	}
	manifest := filepath.Join(trash, skillRecoveryManifest)
	if info, err := os.Stat(manifest); err != nil || info.Size() == 0 {
		t.Fatalf("durable recovery manifest info=%v err=%v", info, err)
	}
	homePath, _ := NewHostPath(home)
	trashPath, _ := NewHostPath(trash)
	restarted, err := NewSkillsService(homePath, nil, trashPath, SkillsLimits{})
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.RecoverPaths(context.Background(), restarted.Locations(nil), []string{dir}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, SkillFile)); err != nil {
		t.Fatal(err)
	}
	state, err := restarted.loadRecoveryState()
	if err != nil {
		t.Fatal(err)
	}
	if len(state.ByOriginal) != 0 {
		t.Fatalf("recovery metadata retained restored entry: %+v", state)
	}
}

func TestTrashRecoveryMetadataIsBounded(t *testing.T) {
	service, home, _ := newTestSkillsService(t, SkillsLimits{MaxRecoveries: 1})
	root := filepath.Join(home, ".agents", "skills")
	first := writeTestSkill(t, root, "one", "one")
	second := writeTestSkill(t, root, "two", "two")
	locations := service.Locations(nil)
	if _, err := service.Trash(context.Background(), locations, []string{first}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Trash(context.Background(), locations, []string{second}); !errors.Is(err, ErrSkillLimitExceeded) {
		t.Fatalf("recovery bound=%v", err)
	}
	if _, err := os.Stat(second); err != nil {
		t.Fatalf("bounded trash moved source: %v", err)
	}
}

func TestTrashRefusesCorruptRecoveryMetadataBeforeMove(t *testing.T) {
	service, home, trash := newTestSkillsService(t, SkillsLimits{})
	dir := writeTestSkill(t, filepath.Join(home, ".agents", "skills"), "safe", "body")
	if err := os.MkdirAll(trash, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(trash, skillRecoveryManifest), []byte(`{"schemaVersion":1,"byOriginal":`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Trash(context.Background(), service.Locations(nil), []string{dir}); err == nil {
		t.Fatal("trash accepted corrupt recovery metadata")
	}
	if _, err := os.Stat(filepath.Join(dir, SkillFile)); err != nil {
		t.Fatalf("corrupt metadata moved source: %v", err)
	}
}

func TestTrashCompensatesPostRenameDurabilityFailure(t *testing.T) {
	service, home, trash := newTestSkillsService(t, SkillsLimits{})
	dir := writeTestSkill(t, filepath.Join(home, ".agents", "skills"), "durability", "body")
	service.afterSkillRename = func(string, string) error {
		service.afterSkillRename = nil
		return errors.New("synthetic directory fsync failure")
	}
	if _, err := service.Trash(context.Background(), service.Locations(nil), []string{dir}); err == nil {
		t.Fatal("trash reported success after injected post-rename failure")
	}
	if _, err := os.Stat(filepath.Join(dir, SkillFile)); err != nil {
		t.Fatalf("committed rename was not compensated: %v", err)
	}
	homePath, _ := NewHostPath(home)
	trashPath, _ := NewHostPath(trash)
	restarted, _ := NewSkillsService(homePath, nil, trashPath, SkillsLimits{})
	state, err := restarted.loadRecoveryState()
	if err != nil {
		t.Fatal(err)
	}
	if len(state.ByOriginal) != 0 {
		t.Fatalf("failed rename left recovery metadata: %+v", state)
	}
}

func TestTrashRestoresPriorManifestAfterPostReplaceFailure(t *testing.T) {
	service, home, trash := newTestSkillsService(t, SkillsLimits{})
	root := filepath.Join(home, ".agents", "skills")
	first := writeTestSkill(t, root, "first", "first")
	second := writeTestSkill(t, root, "second", "second")
	locations := service.Locations(nil)
	if _, err := service.Trash(context.Background(), locations, []string{first}); err != nil {
		t.Fatal(err)
	}
	service.afterManifestRename = func(string) error {
		service.afterManifestRename = nil
		return errors.New("synthetic manifest directory fsync failure")
	}
	if _, err := service.Trash(context.Background(), locations, []string{second}); err == nil {
		t.Fatal("second trash reported success after manifest durability failure")
	}
	if _, err := os.Stat(filepath.Join(second, SkillFile)); err != nil {
		t.Fatalf("second skill rename was not rolled back: %v", err)
	}
	homePath, _ := NewHostPath(home)
	trashPath, _ := NewHostPath(trash)
	restarted, _ := NewSkillsService(homePath, nil, trashPath, SkillsLimits{})
	state, err := restarted.loadRecoveryState()
	if err != nil {
		t.Fatal(err)
	}
	if len(state.ByOriginal) != 1 || state.ByOriginal[first] == "" || state.ByOriginal[second] != "" {
		t.Fatalf("prior manifest was not restored: %+v", state)
	}
	if err := restarted.RecoverPaths(context.Background(), restarted.Locations(nil), []string{first}); err != nil {
		t.Fatal(err)
	}
}

func TestTrashRollsBackBatchFailureWithoutDeleting(t *testing.T) {
	service, home, trash := newTestSkillsService(t, SkillsLimits{})
	root := filepath.Join(home, ".agents", "skills")
	first := writeTestSkill(t, root, "first", "one")
	second := writeTestSkill(t, root, "second", "two")
	calls := 0
	service.beforeRename = func(_, _ string) error {
		calls++
		if calls == 2 {
			return errors.New("injected rename failure")
		}
		return nil
	}
	if _, err := service.Trash(context.Background(), service.Locations(nil), []string{first, second}); err == nil {
		t.Fatal("expected injected failure")
	}
	for _, dir := range []string{first, second} {
		if _, err := os.Stat(filepath.Join(dir, SkillFile)); err != nil {
			t.Fatalf("trash rollback did not restore %s: %v", dir, err)
		}
	}
	entries, err := os.ReadDir(trash)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("rollback left %d items in trash", len(entries))
	}
}

func TestRecoverRefusesOverwriteAndKeepsTrashItem(t *testing.T) {
	service, home, trash := newTestSkillsService(t, SkillsLimits{})
	root := filepath.Join(home, ".agents", "skills")
	dir := writeTestSkill(t, root, "deploy", "original")
	locations := service.Locations(nil)
	recovery, err := service.Trash(context.Background(), locations, []string{dir})
	if err != nil {
		t.Fatal(err)
	}
	writeTestSkill(t, root, "deploy", "replacement")
	if err := service.Recover(context.Background(), locations, recovery); err == nil {
		t.Fatal("expected recovery conflict")
	}
	item := recovery.Items[0]
	if _, err := os.Stat(filepath.Join(trash, item.Identity, SkillFile)); err != nil {
		t.Fatalf("conflicted recovery was not retained: %v", err)
	}
	contents, err := os.ReadFile(filepath.Join(dir, SkillFile))
	if err != nil || string(contents) != "replacement" {
		t.Fatalf("existing destination changed: %q, %v", contents, err)
	}
}
