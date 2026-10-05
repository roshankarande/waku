//go:build windows

package storage

// Windows does not expose portable directory fsync through os.File. Atomic
// creation remains enforced, but durable directory metadata needs Win32 APIs.
func syncDirectory(string) error { return nil }
