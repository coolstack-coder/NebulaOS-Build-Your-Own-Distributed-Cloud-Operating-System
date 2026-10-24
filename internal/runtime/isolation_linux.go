//go:build linux

package runtime

import (
	"os"
	"syscall"
)

func sysProcAttr(isolate bool) *syscall.SysProcAttr {
	attr := &syscall.SysProcAttr{
		Setpgid: true,
		// If the agent dies the kernel takes its tasks with it, the same way
		// a crashed machine would.
		Pdeathsig: syscall.SIGKILL,
	}
	if isolate && os.Geteuid() == 0 {
		attr.Cloneflags = syscall.CLONE_NEWUTS | syscall.CLONE_NEWPID |
			syscall.CLONE_NEWNS | syscall.CLONE_NEWIPC
	}
	return attr
}
