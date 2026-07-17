//go:build windows

package callershim

import (
	"os/exec"
	"time"
)

// Windows CommandContext still kills its direct child. WaitDelay additionally
// bounds inherited pipe handles held by descendants so the HTTP tool call can
// never outlive its declared timeout indefinitely.
func configureCommandCancellation(command *exec.Cmd) {
	command.WaitDelay = time.Second
}
