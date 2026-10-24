package runtime

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

// OCI drives a Docker-compatible CLI: docker itself, or nerdctl talking to
// containerd. Containers are labelled with the owning node and task so an
// agent that restarts can find (and adopt or reap) what it left behind.
//
// Shelling out keeps the dependency tree small and works identically for
// both engines; the cost is ~10ms per call, which is irrelevant next to
// container start times.
type OCI struct {
	bin  string
	opts Options

	mu    sync.Mutex
	state map[string]Status // refreshed by the poller
	stop  chan struct{}
}

func NewOCI(bin string, opts Options) (*OCI, error) {
	if _, err := exec.LookPath(bin); err != nil {
		return nil, fmt.Errorf("%s not found in PATH: %w", bin, err)
	}
	o := &OCI{bin: bin, opts: opts, state: map[string]Status{}, stop: make(chan struct{})}
	go o.poll()
	return o, nil
}

func (o *OCI) Name() string {
	if o.bin == "nerdctl" {
		return "containerd"
	}
	return o.bin
}

func (o *OCI) container(id string) string { return "nebula-" + id }

func (o *OCI) run(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, o.bin, args...)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("%s %s: %v: %s", o.bin, args[0], err, strings.TrimSpace(errb.String()))
	}
	return strings.TrimSpace(out.String()), nil
}

func (o *OCI) Start(ctx context.Context, spec Spec) error {
	name := o.container(spec.TaskID)
	// A container with this name may linger from a previous attempt.
	_, _ = o.run(ctx, "rm", "-f", name)
	args := []string{"run", "-d", "--name", name,
		"--label", "nebula.node=" + o.opts.NodeID,
		"--label", "nebula.task=" + spec.TaskID,
		"--label", "nebula.service=" + spec.Service,
		"--hostname", spec.TaskID,
	}
	if spec.CPU > 0 {
		args = append(args, "--cpus", strconv.FormatFloat(float64(spec.CPU)/1000, 'f', 2, 64))
	}
	if spec.Memory > 0 {
		args = append(args, "--memory", strconv.FormatInt(spec.Memory, 10)+"m")
	}
	if spec.Port > 0 {
		args = append(args, "-p", strconv.Itoa(spec.Port))
	}
	for k, v := range spec.Env {
		args = append(args, "-e", k+"="+v)
	}
	args = append(args, "-e", "NEBULA_TASK_ID="+spec.TaskID, spec.Image)
	args = append(args, spec.Command...)

	o.mu.Lock()
	o.state[spec.TaskID] = Status{State: Starting, Message: "pulling " + spec.Image, StartedAt: time.Now()}
	o.mu.Unlock()
	// Image pulls can take a while; do them off the agent's sync loop.
	go func() {
		pctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		if _, err := o.run(pctx, args...); err != nil {
			o.mu.Lock()
			o.state[spec.TaskID] = Status{State: Failed, Message: err.Error()}
			o.mu.Unlock()
		}
	}()
	return nil
}

func (o *OCI) Stop(ctx context.Context, id string) error {
	o.mu.Lock()
	delete(o.state, id)
	o.mu.Unlock()
	_, err := o.run(ctx, "rm", "-f", o.container(id))
	return err
}

func (o *OCI) Status(id string) (Status, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	s, ok := o.state[id]
	return s, ok
}

func (o *OCI) List(ctx context.Context) ([]string, error) {
	out, err := o.run(ctx, "ps", "-a", "--filter", "label=nebula.node="+o.opts.NodeID, "--format", `{{.Label "nebula.task"}}`)
	if err != nil {
		return nil, err
	}
	var ids []string
	for _, l := range strings.Split(out, "\n") {
		if l = strings.TrimSpace(l); l != "" {
			ids = append(ids, l)
		}
	}
	return ids, nil
}

func (o *OCI) Update(Spec) {}

func (o *OCI) Close() error {
	close(o.stop)
	return nil
}

// poll refreshes container state and usage every two seconds in one
// `ps` + one `stats` call for all containers on this node.
func (o *OCI) poll() {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-o.stop:
			return
		case <-t.C:
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		o.refresh(ctx)
		cancel()
	}
}

func (o *OCI) refresh(ctx context.Context) {
	out, err := o.run(ctx, "ps", "-a", "--filter", "label=nebula.node="+o.opts.NodeID,
		"--format", `{{.Label "nebula.task"}}|{{.State}}|{{.Status}}|{{.Names}}`)
	if err != nil {
		return
	}
	type row struct{ state, status, name string }
	rows := map[string]row{}
	var running []string
	for _, l := range strings.Split(out, "\n") {
		f := strings.SplitN(strings.TrimSpace(l), "|", 4)
		if len(f) != 4 || f[0] == "" {
			continue
		}
		rows[f[0]] = row{f[1], f[2], f[3]}
		if f[1] == "running" {
			running = append(running, f[3])
		}
	}
	usage := map[string][2]int64{}
	if len(running) > 0 {
		args := append([]string{"stats", "--no-stream", "--format", "{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}"}, running...)
		if s, err := o.run(ctx, args...); err == nil {
			for _, l := range strings.Split(s, "\n") {
				f := strings.SplitN(strings.TrimSpace(l), "|", 3)
				if len(f) == 3 {
					usage[f[0]] = [2]int64{parsePercent(f[1]) * 10, parseMem(f[2])}
				}
			}
		}
	}

	o.mu.Lock()
	defer o.mu.Unlock()
	for id, st := range o.state {
		r, ok := rows[id]
		if !ok {
			continue // still pulling, or failed to start (state already says so)
		}
		switch r.state {
		case "running":
			u := usage[r.name]
			o.state[id] = Status{State: Running, CPU: u[0], Memory: u[1], StartedAt: st.StartedAt}
		case "created", "restarting":
			o.state[id] = Status{State: Starting, Message: r.status, StartedAt: st.StartedAt}
		case "exited", "dead":
			s := Failed
			if strings.Contains(r.status, "(0)") {
				s = Exited
			}
			o.state[id] = Status{State: s, Message: r.status, StartedAt: st.StartedAt}
		}
	}
}

func parsePercent(s string) int64 {
	f, _ := strconv.ParseFloat(strings.TrimSuffix(strings.TrimSpace(s), "%"), 64)
	return int64(f)
}

// parseMem turns "12.5MiB / 256MiB" into 12.
func parseMem(s string) int64 {
	used := strings.TrimSpace(strings.SplitN(s, "/", 2)[0])
	mult := 1.0
	for suf, m := range map[string]float64{"KiB": 1.0 / 1024, "MiB": 1, "GiB": 1024, "kB": 1.0 / 1000, "MB": 1, "GB": 1000, "B": 1.0 / (1 << 20)} {
		if strings.HasSuffix(used, suf) {
			used = strings.TrimSuffix(used, suf)
			mult = m
			break
		}
	}
	f, _ := strconv.ParseFloat(used, 64)
	return int64(f * mult)
}
