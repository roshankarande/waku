//go:build !unix

package validation

import "errors"

func holdFileLock(string) (func() error, error) {
	return nil, errors.New("OS file-lock validation is unavailable on this platform")
}
