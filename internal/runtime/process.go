package runtime

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Process runs each task as a native child process in its own process group.
// On Linux with Isolate set it also unshares UTS, PID, mount and IPC
// namespaces, which is the same isolation primitive containers build on.
type Process struct {
	opts  Options
	mu    sync.Mutex
	procs map[string]*proc
}

type proc struct {
	cmd     *exec.Cmd
	started time.Time
	done    chan struct{}
	err     error
	usage   Status
	sampled time.Time
}

func NewProcess(opts Options) (*Process, error) {
	if err := os.MkdirAll(filepath.Join(opts.DataDir, "tasks"), 0o755); err != nil {
		return nil, err
	}
	return &Process{opts: opts, procs: map[string]*proc{}}, nil
}

func (p *Process) Name() string { return "process" }

func (p *Process) Start(_ context.Context, spec Spec) error {
	if len(spec.Command) == 0 {
		return errors.New("process runtime needs a command")
	}
	logf, err := os.Create(filepath.Join(p.opts.DataDir, "tasks", spec.TaskID+".log"))
	if err != nil {
		return err
	}
	cmd := exec.Command(spec.Command[0], spec.Command[1:]...)
	cmd.Stdout, cmd.Stderr = logf, logf
	cmd.Env = append(os.Environ(), "NEBULA_TASK_ID="+spec.TaskID, "NEBULA_SERVICE="+spec.Service)
	if spec.Port > 0 {
		cmd.Env = append(cmd.Env, "PORT="+strconv.Itoa(spec.Port))
	}
	for k, v := range spec.Env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	cmd.SysProcAttr = sysProcAttr(p.opts.Isolate)
	if err := cmd.Start(); err != nil {
		logf.Close()
		return err
	}
	pr := &proc{cmd: cmd, started: time.Now(), done: make(chan struct{})}
	go func() {
		pr.err = cmd.Wait()
		logf.Close()
		close(pr.done)
	}()
	p.mu.Lock()
	p.procs[spec.TaskID] = pr
	p.mu.Unlock()
	return nil
}

func (p *Process) Stop(_ context.Context, id string) error {
	p.mu.Lock()
	pr, ok := p.procs[id]
	delete(p.procs, id)
	p.mu.Unlock()
	if !ok {
		return nil
	}
	pgid := -pr.cmd.Process.Pid
	_ = syscall.Kill(pgid, syscall.SIGTERM)
	select {
	case <-pr.done:
	case <-time.After(3 * time.Second):
		_ = syscall.Kill(pgid, syscall.SIGKILL)
		<-pr.done
	}
	return nil
}

func (p *Process) Status(id string) (Status, bool) {
	p.mu.Lock()
	pr, ok := p.procs[id]
	p.mu.Unlock()
	if !ok {
		return Status{}, false
	}
	select {
	case <-pr.done:
		if pr.err == nil {
			return Status{State: Exited, StartedAt: pr.started}, true
		}
		return Status{State: Failed, Message: pr.err.Error(), StartedAt: pr.started}, true
	default:
	}
	p.mu.Lock()
	stale := time.Since(pr.sampled) > time.Second
	p.mu.Unlock()
	if stale {
		u := samplePS(pr.cmd.Process.Pid)
		p.mu.Lock()
		pr.usage, pr.sampled = u, time.Now()
		p.mu.Unlock()
	}
	p.mu.Lock()
	st := pr.usage
	p.mu.Unlock()
	st.State, st.StartedAt = Running, pr.started
	return st, true
}

// samplePS reads CPU and RSS from ps(1), which works the same on Linux and
// macOS and avoids a cgo dependency.
func samplePS(pid int) Status {
	out, err := exec.Command("ps", "-o", "%cpu=,rss=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return Status{}
	}
	f := strings.Fields(string(out))
	if len(f) < 2 {
		return Status{}
	}
	pct, _ := strconv.ParseFloat(f[0], 64)
	rss, _ := strconv.ParseInt(f[1], 10, 64)
	return Status{CPU: int64(pct * 10), Memory: rss / 1024}
}

func (p *Process) List(context.Context) ([]string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, 0, len(p.procs))
	for id := range p.procs {
		out = append(out, id)
	}
	return out, nil
}

func (p *Process) Update(Spec) {}

func (p *Process) Close() error {
	ids, _ := p.List(context.Background())
	for _, id := range ids {
		_ = p.Stop(context.Background(), id)
	}
	return nil
}
