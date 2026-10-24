//go:build !linux

package runtime

import "syscall"

// Namespaces are Linux-only; elsewhere tasks just get their own process group.
func sysProcAttr(bool) *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setpgid: true}
}
