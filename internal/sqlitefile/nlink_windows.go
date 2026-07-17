//go:build windows

package sqlitefile

import "os"

// Windows FileInfo does not expose a link count through the portable Go API.
// The default LOCALAPPDATA tree remains the trusted boundary on this platform.
func hasMultipleLinks(os.FileInfo) bool { return false }
