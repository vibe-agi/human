//go:build !windows

package humancmd

import "os"

func replaceFileAtomically(source, destination string) error {
	return os.Rename(source, destination)
}
