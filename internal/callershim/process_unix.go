//go:build !windows

package callershim

import (
	"os"
	"os/exec"
	"syscall"
	"time"
)

// configureCommandCancellation puts the shell and every descendant it starts
// in a private process group. CommandContext's default cancellation kills only
// the shell; a daemonized grandchild can otherwise retain stdout/stderr pipes
// and keep Cmd.Wait blocked forever after the advertised timeout.
func configureCommandCancellation(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Cancel = func() error {
		if command.Process == nil {
			return os.ErrProcessDone
		}
		if err := syscall.Kill(-command.Process.Pid, syscall.SIGKILL); err != nil {
			if err == syscall.ESRCH {
				return os.ErrProcessDone
			}
			return err
		}
		return nil
	}
	// This is a second bound for unusual descendants which escape the process
	// group but retain an inherited pipe. It closes our side of the pipes and
	// makes Run return; the process-group kill remains the primary cleanup.
	command.WaitDelay = time.Second
}
