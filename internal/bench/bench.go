// Package bench measures NebulaOS end to end on a real multi-process
// cluster: scheduling latency, Raft write throughput, and how long the
// system takes to notice and heal from crashed nodes.
package bench

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	goruntime "runtime"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coolstack-coder/nebulaos/internal/demo"
	"github.com/coolstack-coder/nebulaos/internal/node"
	"github.com/coolstack-coder/nebulaos/internal/state"
)

type Config struct {
	Demo     demo.Config
	Runs     int
	Replicas int
	Logf     func(format string, args ...any)
}

type Scenario struct {
	Name        string
	Description string
	KillLeader  bool
	Followers   int
}

var Scenarios = []Scenario{
	{"follower-crash", "SIGKILL one follower that is running workloads", false, 1},
	{"leader-crash", "SIGKILL the Raft leader (control plane + its workloads)", true, 0},
	{"leader+follower-crash", "SIGKILL the leader and one follower: 2 of 5 nodes", true, 1},
}

type RunResult struct {
	ElectionMs  int64   `json:"election_ms"`  // kill → new leader (leader scenarios)
	DetectMs    int64   `json:"detect_ms"`    // kill → node declared down
	RecoveryMs  int64   `json:"recovery_ms"`  // kill → every replica running again
	HealMs      int64   `json:"heal_ms"`      // node declared down → replicas running (leader's own measurement)
	Moved       int     `json:"moved"`        // replicas rescheduled
	Available   float64 `json:"availability"` // mean running/desired over the 15s after the kill
	LeaderUpPct float64 `json:"leader_up"`    // share of that window with a reachable leader
}

type ScenarioResult struct {
	Scenario Scenario    `json:"scenario"`
	Runs     []RunResult `json:"runs"`
}

type Report struct {
	When           time.Time        `json:"when"`
	Host           string           `json:"host"`
	Nodes          int              `json:"nodes"`
	Replicas       int              `json:"replicas"`
	Config         demo.Config      `json:"-"`
	ColdStartMs    int64            `json:"cold_start_ms"`
	SchedP50Ms     float64          `json:"sched_p50_ms"`
	SchedP99Ms     float64          `json:"sched_p99_ms"`
	StartP50Ms     float64          `json:"start_p50_ms"`
	StartP99Ms     float64          `json:"start_p99_ms"`
	DeployAllMs    int64            `json:"deploy_all_ms"`
	WriteOpsPerSec float64          `json:"write_ops_per_sec"`
	WriteP50Ms     float64          `json:"write_p50_ms"`
	WriteP99Ms     float64          `json:"write_p99_ms"`
	Scenarios      []ScenarioResult `json:"scenarios"`
}

type runner struct {
	cfg  Config
	sup  *demo.Supervisor
	http *http.Client
}

func Run(cfg Config) (*Report, error) {
	if cfg.Logf == nil {
		cfg.Logf = func(string, ...any) {}
	}
	cfg.Demo.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	sup, err := demo.New(cfg.Demo)
	if err != nil {
		return nil, err
	}
	r := &runner{cfg: cfg, sup: sup, http: &http.Client{Timeout: 2 * time.Second}}
	rep := &Report{When: time.Now(), Nodes: cfg.Demo.Nodes, Replicas: cfg.Replicas, Config: cfg.Demo,
		Host: fmt.Sprintf("%s/%s, %d CPUs", goruntime.GOOS, goruntime.GOARCH, goruntime.NumCPU())}

	start := time.Now()
	if err := sup.Start(); err != nil {
		return nil, err
	}
	defer sup.Shutdown()
	if _, err := r.waitLeader(0, 20*time.Second); err != nil {
		return nil, err
	}
	rep.ColdStartMs = time.Since(start).Milliseconds()
	cfg.Logf("cold start: leader elected after %dms", rep.ColdStartMs)
	if err := r.waitNodesReady(cfg.Demo.Nodes, 15*time.Second); err != nil {
		return nil, err
	}

	if err := r.measureDeploy(rep); err != nil {
		return nil, err
	}
	cfg.Logf("scheduling: create→bound p50 %.1fms p99 %.1fms; create→running p50 %.0fms p99 %.0fms; %d replicas running in %dms",
		rep.SchedP50Ms, rep.SchedP99Ms, rep.StartP50Ms, rep.StartP99Ms, cfg.Replicas, rep.DeployAllMs)

	if err := r.measureWrites(rep, 5*time.Second, 16); err != nil {
		return nil, err
	}
	cfg.Logf("writes: %.0f ops/s, p50 %.1fms, p99 %.1fms", rep.WriteOpsPerSec, rep.WriteP50Ms, rep.WriteP99Ms)

	for _, sc := range Scenarios {
		res := ScenarioResult{Scenario: sc}
		for i := 0; i < cfg.Runs; i++ {
			rr, err := r.runScenario(sc)
			if err != nil {
				return nil, fmt.Errorf("%s run %d: %w", sc.Name, i+1, err)
			}
			cfg.Logf("%-22s run %d: election %4dms  detect %4dms  recovery %4dms  moved %2d  availability %.1f%%",
				sc.Name, i+1, rr.ElectionMs, rr.DetectMs, rr.RecoveryMs, rr.Moved, rr.Available*100)
			res.Runs = append(res.Runs, rr)
		}
		rep.Scenarios = append(rep.Scenarios, res)
	}
	return rep, nil
}

// ---------------------------------------------------------------- helpers

func (r *runner) overview() (*node.Overview, error) {
	api, err := r.sup.API()
	if err != nil {
		return nil, err
	}
	resp, err := r.http.Get("http://" + api + "/api/overview")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var o node.Overview
	return &o, json.NewDecoder(resp.Body).Decode(&o)
}

func (r *runner) send(method, path string, body any) error {
	api, err := r.sup.API()
	if err != nil {
		return err
	}
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest(method, "http://"+api+path, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%s %s: %s %s", method, path, resp.Status, msg)
	}
	return nil
}

// waitLeader waits for a leader in a term greater than minTerm, polling the
// surviving processes directly.
func (r *runner) waitLeader(minTerm uint64, timeout time.Duration) (uint64, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range r.sup.Nodes() {
			if !n.Alive {
				continue
			}
			var st struct {
				Role string `json:"role"`
				Term uint64 `json:"term"`
			}
			resp, err := r.http.Get("http://" + n.API + "/api/raft")
			if err != nil {
				continue
			}
			err = json.NewDecoder(resp.Body).Decode(&st)
			resp.Body.Close()
			if err == nil && st.Role == "leader" && st.Term > minTerm {
				return st.Term, nil
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return 0, errors.New("timed out waiting for a leader")
}

func (r *runner) waitNodesReady(n int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if o, err := r.overview(); err == nil {
			ready := 0
			for _, nd := range o.Nodes {
				if nd.Status == state.NodeReady {
					ready++
				}
			}
			if ready >= n {
				return nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("fewer than %d nodes ready after %s", n, timeout)
}

func healthy(o *node.Overview) (running, desired int) {
	for _, s := range o.Services {
		running += min(s.Running, s.Spec.Replicas)
		desired += s.Spec.Replicas
	}
	return
}

func (r *runner) waitHealthy(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if o, err := r.overview(); err == nil {
			if run, des := healthy(o); des > 0 && run == des {
				return nil
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	return errors.New("cluster did not become healthy")
}

func pct(xs []float64, p float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := slices.Clone(xs)
	sort.Float64s(s)
	return s[min(len(s)-1, int(float64(len(s))*p))]
}

// ---------------------------------------------------------------- phases

func (r *runner) measureDeploy(rep *Report) error {
	spec := state.ServiceSpec{Name: "bench", Image: "nginx:1.27-alpine", Replicas: r.cfg.Replicas,
		Resources: state.Resources{CPU: 200, Memory: 128}, SimDemand: int64(r.cfg.Replicas) * 80}
	t0 := time.Now()
	if err := r.send(http.MethodPost, "/api/services", spec); err != nil {
		return err
	}
	if err := r.waitHealthy(30 * time.Second); err != nil {
		return err
	}
	rep.DeployAllMs = time.Since(t0).Milliseconds()
	o, err := r.overview()
	if err != nil {
		return err
	}
	var sched, start []float64
	for _, t := range o.Tasks {
		if t.Service != "bench" || t.AssignedAt == 0 {
			continue
		}
		sched = append(sched, float64(t.AssignedAt-t.CreatedAt))
		if t.RunningAt > 0 {
			start = append(start, float64(t.RunningAt-t.CreatedAt))
		}
	}
	rep.SchedP50Ms, rep.SchedP99Ms = pct(sched, 0.5), pct(sched, 0.99)
	rep.StartP50Ms, rep.StartP99Ms = pct(start, 0.5), pct(start, 0.99)

	// A second, smaller service so failures always hit more than one service.
	side := state.ServiceSpec{Name: "side", Image: "redis:7-alpine", Replicas: 5,
		Resources: state.Resources{CPU: 150, Memory: 256}, SimDemand: 300}
	if err := r.send(http.MethodPost, "/api/services", side); err != nil {
		return err
	}
	return r.waitHealthy(20 * time.Second)
}

// measureWrites drives concurrent no-op spec updates through the API. Each
// one is a full round trip: HTTP → (forward to leader) → Raft append →
// replicate to a quorum → fsync → apply.
func (r *runner) measureWrites(rep *Report, d time.Duration, workers int) error {
	var (
		ops  atomic.Int64
		mu   sync.Mutex
		lats []float64
		wg   sync.WaitGroup
		fail atomic.Int64
	)
	stop := time.Now().Add(d)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; time.Now().Before(stop); i++ {
				v := int64(r.cfg.Replicas*80 + (w*1000+i)%7)
				t := time.Now()
				if err := r.send(http.MethodPatch, "/api/services/bench", state.ServicePatch{SimDemand: &v}); err != nil {
					fail.Add(1)
					continue
				}
				ops.Add(1)
				mu.Lock()
				lats = append(lats, float64(time.Since(t).Microseconds())/1000)
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	rep.WriteOpsPerSec = float64(ops.Load()) / d.Seconds()
	rep.WriteP50Ms, rep.WriteP99Ms = pct(lats, 0.5), pct(lats, 0.99)
	if fail.Load() > ops.Load()/10 {
		return fmt.Errorf("too many failed writes: %d", fail.Load())
	}
	return nil
}

func (r *runner) runScenario(sc Scenario) (RunResult, error) {
	var rr RunResult
	// Start every run from a fully healthy, fully joined cluster.
	for _, id := range r.sup.IDs() {
		_ = r.sup.StartNode(id)
	}
	if err := r.waitNodesReady(len(r.sup.IDs()), 20*time.Second); err != nil {
		return rr, err
	}
	if err := r.waitHealthy(20 * time.Second); err != nil {
		return rr, err
	}
	time.Sleep(1500 * time.Millisecond)

	o, err := r.overview()
	if err != nil {
		return rr, err
	}
	leader := o.Leader
	oldTerm := o.Raft.Term
	busy := map[string]int{}
	for _, t := range o.Tasks {
		if t.State == state.TaskRunning {
			busy[t.Node]++
		}
	}
	var victims []string
	if sc.KillLeader {
		victims = append(victims, leader)
	}
	var followers []string
	for _, id := range r.sup.IDs() {
		if id != leader && busy[id] > 0 {
			followers = append(followers, id)
		}
	}
	rand.Shuffle(len(followers), func(i, j int) { followers[i], followers[j] = followers[j], followers[i] })
	victims = append(victims, followers[:sc.Followers]...)
	for _, v := range victims {
		rr.Moved += busy[v]
	}
	lastSeq := uint64(0)
	if n := len(o.Events); n > 0 {
		lastSeq = o.Events[n-1].Seq
	}

	t0 := time.Now()
	for _, v := range victims {
		if err := r.sup.Kill(v); err != nil {
			return rr, err
		}
	}

	if sc.KillLeader {
		if _, err := r.waitLeader(oldTerm, 15*time.Second); err != nil {
			return rr, err
		}
		rr.ElectionMs = time.Since(t0).Milliseconds()
	}

	// Sample availability while waiting for recovery.
	window := 15 * time.Second
	var samples, leaderUp int
	var avail float64
	recovered := false
	for time.Since(t0) < window {
		o, err := r.overview()
		samples++
		if err == nil && o.Leader != "" && !slices.Contains(victims, o.Leader) {
			leaderUp++
			run, des := healthy(o)
			if des > 0 {
				avail += float64(run) / float64(des)
			}
			down := 0
			for _, nd := range o.Nodes {
				if slices.Contains(victims, nd.ID) && nd.Status == state.NodeDown {
					down++
				}
			}
			if rr.DetectMs == 0 && down == len(victims) {
				rr.DetectMs = time.Since(t0).Milliseconds()
			}
			if !recovered && down == len(victims) && run == des {
				rr.RecoveryMs = time.Since(t0).Milliseconds()
				recovered = true
				for _, e := range o.Events {
					if e.Seq > lastSeq && e.Kind == "recovered" {
						rr.HealMs = e.DurationMs
					}
				}
			}
		} else if err == nil && slices.Contains(victims, o.Leader) {
			// Stale view from a follower that still believes in the dead
			// leader: replicas on survivors are still serving.
			run, des := healthy(o)
			if des > 0 {
				avail += float64(run) / float64(des)
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !recovered {
		return rr, errors.New("did not recover within the window")
	}
	rr.Available = avail / float64(samples)
	rr.LeaderUpPct = float64(leaderUp) / float64(samples)
	return rr, nil
}

// ---------------------------------------------------------------- report

func median(xs []int64) int64 {
	if len(xs) == 0 {
		return 0
	}
	s := slices.Clone(xs)
	slices.Sort(s)
	return s[len(s)/2]
}

func maxOf(xs []int64) int64 {
	if len(xs) == 0 {
		return 0
	}
	return slices.Max(xs)
}

func (rep *Report) Markdown() string {
	var b strings.Builder
	c := rep.Config
	fmt.Fprintf(&b, "# NebulaOS benchmarks\n\n")
	fmt.Fprintf(&b, "Generated by `nebula bench` on %s (%s).\n\n", rep.When.Format("2006-01-02 15:04 MST"), rep.Host)
	fmt.Fprintf(&b, "Every number below comes from a real cluster of **%d separate OS processes** talking gRPC over loopback, "+
		"with Raft writes fsynced to disk (`--fsync=%v`). Failures are `SIGKILL`s — no graceful shutdown. "+
		"Workloads use the `%s` runtime so the measurements isolate the control plane from image pull and container start time.\n\n",
		rep.Nodes, c.Fsync, c.Runtime)
	fmt.Fprintf(&b, "Timers: heartbeat %s, node timeout %s, Raft election timeout %s–%s.\n\n", c.Heartbeat, c.NodeTimeout, c.Election, 2*c.Election)

	fmt.Fprintf(&b, "## Control plane\n\n| Metric | Value |\n|---|---|\n")
	fmt.Fprintf(&b, "| Cold start → leader elected | %d ms |\n", rep.ColdStartMs)
	fmt.Fprintf(&b, "| Scheduling latency (task created → bound to node), p50 / p99 | %.0f / %.0f ms |\n", rep.SchedP50Ms, rep.SchedP99Ms)
	fmt.Fprintf(&b, "| Task created → running, p50 / p99 | %.0f / %.0f ms |\n", rep.StartP50Ms, rep.StartP99Ms)
	fmt.Fprintf(&b, "| Deploy %d replicas → all running | %d ms |\n", rep.Replicas, rep.DeployAllMs)
	fmt.Fprintf(&b, "| Replicated writes through the API (16 clients) | %.0f ops/s |\n", rep.WriteOpsPerSec)
	fmt.Fprintf(&b, "| Write latency p50 / p99 | %.1f / %.1f ms |\n\n", rep.WriteP50Ms, rep.WriteP99Ms)

	fmt.Fprintf(&b, "## Failure recovery\n\n")
	fmt.Fprintf(&b, "Each scenario ran %d times from a fully healthy cluster. *Recovery* is wall-clock time from the `SIGKILL` until every replica "+
		"of every service is reported running again. *Availability* is the mean fraction of desired replicas running over the 15 s after the kill.\n\n", len(rep.Scenarios[0].Runs))
	fmt.Fprintf(&b, "| Scenario | New leader (median) | Failure detected (median) | Recovery median | Recovery worst | Replicas moved | Availability |\n|---|---|---|---|---|---|---|\n")
	for _, s := range rep.Scenarios {
		var el, det, rec []int64
		var av float64
		moved := 0
		for _, r := range s.Runs {
			el = append(el, r.ElectionMs)
			det = append(det, r.DetectMs)
			rec = append(rec, r.RecoveryMs)
			av += r.Available
			moved += r.Moved
		}
		elec := "n/a (leader survives)"
		if s.Scenario.KillLeader {
			elec = fmt.Sprintf("%d ms", median(el))
		}
		fmt.Fprintf(&b, "| **%s** — %s | %s | %d ms | **%d ms** | %d ms | %.1f avg | %.2f%% |\n",
			s.Scenario.Name, s.Scenario.Description, elec, median(det), median(rec), maxOf(rec),
			float64(moved)/float64(len(s.Runs)), 100*av/float64(len(s.Runs)))
	}
	fmt.Fprintf(&b, "\n### Where the time goes\n\n")
	fmt.Fprintf(&b, "Recovery ≈ *failure detection* (bounded by `--node-timeout`, plus an election if the leader died, because a new leader "+
		"gives every node a fresh timeout before judging it) + *rescheduling* (one Raft round trip) + *task start* (one heartbeat for the agent to "+
		"learn about the task, the runtime start, one heartbeat to report it running). Detection dominates by design: declaring a node dead too "+
		"eagerly causes needless churn on a slow network. Lower `--node-timeout` trades false positives for faster healing.\n\n")
	fmt.Fprintf(&b, "### Raw runs\n\n| Scenario | Run | Election | Detected | Recovered | Leader-measured heal | Moved | Availability |\n|---|---|---|---|---|---|---|---|\n")
	for _, s := range rep.Scenarios {
		for i, r := range s.Runs {
			fmt.Fprintf(&b, "| %s | %d | %d ms | %d ms | %d ms | %d ms | %d | %.2f%% |\n", s.Scenario.Name, i+1, r.ElectionMs, r.DetectMs, r.RecoveryMs, r.HealMs, r.Moved, 100*r.Available)
		}
	}
	fmt.Fprintf(&b, "\nReproduce with:\n\n```bash\nmake build && ./bin/nebula bench --runs %d --replicas %d --out docs/BENCHMARKS.md\n```\n", len(rep.Scenarios[0].Runs), rep.Replicas)
	return b.String()
}

func (rep *Report) WriteFiles(md string) error {
	if err := os.WriteFile(md, []byte(rep.Markdown()), 0o644); err != nil {
		return err
	}
	b, _ := json.MarshalIndent(rep, "", "  ")
	return os.WriteFile(strings.TrimSuffix(md, ".md")+".json", b, 0o644)
}
