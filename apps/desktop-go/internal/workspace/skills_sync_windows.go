//go:build windows

package workspace

// Go cannot open a directory for FlushFileBuffers without a platform handle.
// The atomic file replace still preserves a valid old-or-new manifest.
func syncSkillDirectory(string) error { return nil }
