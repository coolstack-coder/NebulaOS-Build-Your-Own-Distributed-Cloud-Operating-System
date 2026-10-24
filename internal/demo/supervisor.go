// Package demo runs a local NebulaOS cluster as real OS processes, one per
// node, and lets you break it. Killing a node here is a SIGKILL of that
// node's process: no graceful shutdown, no goodbye message, exactly what
// the rest of the cluster would see if the machine lost power.
package demo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Config struct {
	Nodes       int
	GRPCBase    int // node i listens on GRPCBase+i
	APIBase     int
	Runtime     string
	DataDir     string
	Binary      string
	CPU         int64
	Memory      int64
	NodeTimeout time.Duration
	Heartbeat   time.Duration
	Election    time.Duration
	Fsync       bool
	Logger      *slog.Logger
}

type proc struct {
	id      string
	grpc    string
	api     string
	cmd     *exec.Cmd
	alive   bool
	started time.Time
	exited  chan struct{}
}

type NodeInfo struct {
	ID        string `json:"id"`
	Alive     bool   `json:"alive"`
	PID       int    `json:"pid,omitempty"`
	API       string `json:"api"`
	Role      string `json:"role,omitempty"`
	Term      uint64 `json:"term,omitempty"`
	Commit    uint64 `json:"commit_index,omitempty"`
	Leader    string `json:"leader,omitempty"`
	UptimeMs  int64  `json:"uptime_ms,omitempty"`
	Reachable bool   `json:"reachable"`
}

type Supervisor struct {
	cfg   Config
	log   *slog.Logger
	mu    sync.Mutex
	procs map[string]*proc
	order []string
	info  map[string]NodeInfo
	http  *http.Client
	stop  chan struct{}
}

func New(cfg Config) (*Supervisor, error) {
	if cfg.Nodes < 1 {
		return nil, errors.New("need at least one node")
	}
	if cfg.Binary == "" {
		b, err := os.Executable()
		if err != nil {
			return nil, err
		}
		cfg.Binary = b
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	s := &Supervisor{
		cfg: cfg, log: cfg.Logger.With("component", "demo"),
		procs: map[string]*proc{}, info: map[string]NodeInfo{},
		http: &http.Client{Timeout: 800 * time.Millisecond}, stop: make(chan struct{}),
	}
	for i := 1; i <= cfg.Nodes; i++ {
		id := fmt.Sprintf("n%d", i)
		s.order = append(s.order, id)
		s.procs[id] = &proc{
			id:   id,
			grpc: fmt.Sprintf("127.0.0.1:%d", cfg.GRPCBase+i),
			api:  fmt.Sprintf("127.0.0.1:%d", cfg.APIBase+i),
		}
	}
	return s, nil
}

func (s *Supervisor) IDs() []string { return append([]string(nil), s.order...) }

// Start wipes the data directory and boots every node.
func (s *Supervisor) Start() error {
	if err := os.RemoveAll(s.cfg.DataDir); err != nil {
		return err
	}
	if err := os.MkdirAll(s.cfg.DataDir, 0o755); err != nil {
		return err
	}
	if s.cfg.Runtime == "docker" {
		s.reapContainers("")
	}
	for _, id := range s.order {
		if err := s.StartNode(id); err != nil {
			return err
		}
	}
	go s.pollLoop()
	return nil
}

func (s *Supervisor) peers() string {
	var parts []string
	for _, id := range s.order {
		parts = append(parts, id+"="+s.procs[id].grpc)
	}
	return strings.Join(parts, ",")
}

var zones = []string{"zone-a", "zone-b", "zone-c"}

// StartNode (re)starts one node process. Its data directory is kept, so a
// restarted node recovers its Raft log like a rebooted machine would.
func (s *Supervisor) StartNode(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.procs[id]
	if !ok {
		return fmt.Errorf("unknown node %s", id)
	}
	if p.alive {
		return nil
	}
	idx := 0
	for i, o := range s.order {
		if o == id {
			idx = i
		}
	}
	args := []string{"node",
		"--id", id, "--grpc", p.grpc, "--api", p.api, "--peers", s.peers(),
		"--data", filepath.Join(s.cfg.DataDir, id), "--runtime", s.cfg.Runtime,
		"--cpu", fmt.Sprint(s.cfg.CPU), "--mem", fmt.Sprint(s.cfg.Memory),
		"--labels", "zone=" + zones[idx%len(zones)],
		"--node-timeout", s.cfg.NodeTimeout.String(), "--heartbeat", s.cfg.Heartbeat.String(),
		"--election-timeout", s.cfg.Election.String(), fmt.Sprintf("--fsync=%v", s.cfg.Fsync),
	}
	logf, err := os.OpenFile(filepath.Join(s.cfg.DataDir, id+".log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	cmd := exec.Command(s.cfg.Binary, args...)
	cmd.Stdout, cmd.Stderr = logf, logf
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		logf.Close()
		return err
	}
	p.cmd, p.alive, p.started, p.exited = cmd, true, time.Now(), make(chan struct{})
	exited := p.exited
	go func() {
		_ = cmd.Wait()
		logf.Close()
		s.mu.Lock()
		if p.cmd == cmd {
			p.alive = false
		}
		s.mu.Unlock()
		close(exited)
	}()
	s.log.Info("started node", "id", id, "pid", cmd.Process.Pid)
	return nil
}

// Kill SIGKILLs a node. With the docker runtime it also removes the node's
// containers, since a dead machine takes its containers with it.
func (s *Supervisor) Kill(id string) error {
	s.mu.Lock()
	p, ok := s.procs[id]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("unknown node %s", id)
	}
	if !p.alive {
		s.mu.Unlock()
		return nil
	}
	pid, exited := p.cmd.Process.Pid, p.exited
	s.mu.Unlock()
	_ = syscall.Kill(-pid, syscall.SIGKILL)
	<-exited
	if s.cfg.Runtime == "docker" {
		s.reapContainers(id)
	}
	s.log.Warn("killed node", "id", id, "pid", pid)
	return nil
}

func (s *Supervisor) reapContainers(node string) {
	filter := "label=nebula.node"
	if node != "" {
		filter += "=" + node
	}
	out, err := exec.Command("docker", "ps", "-aq", "--filter", filter).Output()
	if err != nil {
		return
	}
	if ids := strings.Fields(string(out)); len(ids) > 0 {
		_ = exec.Command("docker", append([]string{"rm", "-f"}, ids...)...).Run()
	}
}

func (s *Supervisor) Shutdown() {
	close(s.stop)
	for _, id := range s.order {
		s.mu.Lock()
		p := s.procs[id]
		alive := p.alive
		s.mu.Unlock()
		if alive {
			_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGTERM)
		}
	}
	deadline := time.After(5 * time.Second)
	for _, id := range s.order {
		select {
		case <-s.procs[id].exited:
		case <-deadline:
			_ = s.Kill(id)
		}
	}
	if s.cfg.Runtime == "docker" {
		s.reapContainers("")
	}
}

// ---------------------------------------------------------------- polling

func (s *Supervisor) pollLoop() {
	t := time.NewTicker(250 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
		}
		var wg sync.WaitGroup
		for _, id := range s.order {
			wg.Add(1)
			go func(id string) {
				defer wg.Done()
				s.pollOne(id)
			}(id)
		}
		wg.Wait()
	}
}

func (s *Supervisor) pollOne(id string) {
	s.mu.Lock()
	p := s.procs[id]
	ni := NodeInfo{ID: id, Alive: p.alive, API: p.api}
	if p.alive {
		ni.PID = p.cmd.Process.Pid
	}
	s.mu.Unlock()
	if ni.Alive {
		var r struct {
			Role     string `json:"role"`
			Term     uint64 `json:"term"`
			Leader   string `json:"leader"`
			Commit   uint64 `json:"commit_index"`
			UptimeMs int64  `json:"uptime_ms"`
		}
		if err := s.getJSON("http://"+ni.API+"/api/raft", &r); err == nil {
			ni.Reachable, ni.Role, ni.Term, ni.Leader, ni.Commit, ni.UptimeMs = true, r.Role, r.Term, r.Leader, r.Commit, r.UptimeMs
		}
	}
	s.mu.Lock()
	s.info[id] = ni
	s.mu.Unlock()
}

func (s *Supervisor) getJSON(u string, out any) error {
	resp, err := s.http.Get(u)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("%s: %s", u, resp.Status)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// Nodes returns the latest per-process view.
func (s *Supervisor) Nodes() []NodeInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]NodeInfo, 0, len(s.order))
	for _, id := range s.order {
		ni := s.info[id]
		ni.ID, ni.Alive = id, s.procs[id].alive
		out = append(out, ni)
	}
	return out
}

// Leader returns the id of the highest-term node claiming leadership.
func (s *Supervisor) Leader() string {
	best, term := "", uint64(0)
	for _, n := range s.Nodes() {
		if n.Alive && n.Reachable && n.Role == "leader" && n.Term >= term {
			best, term = n.ID, n.Term
		}
	}
	return best
}

// API returns the HTTP address of a live node, preferring the leader.
func (s *Supervisor) API() (string, error) {
	if l := s.Leader(); l != "" {
		return s.procs[l].api, nil
	}
	for _, n := range s.Nodes() {
		if n.Alive && n.Reachable {
			return n.API, nil
		}
	}
	return "", errors.New("no reachable nodes")
}

func (s *Supervisor) APIOf(id string) string { return s.procs[id].api }

// ---------------------------------------------------------------- HTTP

// Handler serves the dashboard, the aggregated overview, chaos actions and
// proxies everything else under /api to a live node.
func (s *Supervisor) Handler(ui http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/overview", s.handleOverview)
	mux.HandleFunc("POST /api/chaos/kill/{id}", func(w http.ResponseWriter, r *http.Request) {
		s.chaosReply(w, s.Kill(r.PathValue("id")), "killed "+r.PathValue("id"))
	})
	mux.HandleFunc("POST /api/chaos/start/{id}", func(w http.ResponseWriter, r *http.Request) {
		s.chaosReply(w, s.StartNode(r.PathValue("id")), "started "+r.PathValue("id"))
	})
	mux.HandleFunc("POST /api/chaos/kill-leader", func(w http.ResponseWriter, _ *http.Request) {
		l := s.Leader()
		if l == "" {
			s.chaosReply(w, errors.New("no leader right now"), "")
			return
		}
		s.chaosReply(w, s.Kill(l), "killed leader "+l)
	})
	mux.HandleFunc("POST /api/chaos/kill-two", func(w http.ResponseWriter, _ *http.Request) {
		// The headline scenario: lose the leader plus one more node out of
		// five. A majority (3) survives, so the cluster must heal itself.
		var victims []string
		if l := s.Leader(); l != "" {
			victims = append(victims, l)
		}
		var alive []string
		for _, n := range s.Nodes() {
			if n.Alive && (len(victims) == 0 || n.ID != victims[0]) {
				alive = append(alive, n.ID)
			}
		}
		rand.Shuffle(len(alive), func(i, j int) { alive[i], alive[j] = alive[j], alive[i] })
		for len(victims) < 2 && len(alive) > 0 {
			victims, alive = append(victims, alive[0]), alive[1:]
		}
		for _, v := range victims {
			_ = s.Kill(v)
		}
		s.chaosReply(w, nil, "killed "+strings.Join(victims, " and "))
	})
	mux.HandleFunc("POST /api/chaos/restart-all", func(w http.ResponseWriter, _ *http.Request) {
		var errs []error
		for _, id := range s.order {
			errs = append(errs, s.StartNode(id))
		}
		s.chaosReply(w, errors.Join(errs...), "all nodes running")
	})
	mux.HandleFunc("/api/", s.proxy)
	mux.HandleFunc("/metrics", s.proxy)
	mux.Handle("/", ui)
	return mux
}

func (s *Supervisor) chaosReply(w http.ResponseWriter, err error, msg string) {
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]string{"ok": msg})
}

func (s *Supervisor) handleOverview(w http.ResponseWriter, r *http.Request) {
	nodes := s.Nodes()
	// Try the leader first, then anyone; a node that is mid-election still
	// serves its replicated state.
	cands := []string{}
	if l := s.Leader(); l != "" {
		cands = append(cands, s.procs[l].api)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	for _, n := range nodes {
		if n.Alive {
			cands = append(cands, n.API)
		}
	}
	var body map[string]any
	for _, api := range cands {
		ctx, cancel := context.WithTimeout(r.Context(), 900*time.Millisecond)
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+api+"/api/overview", nil)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == 200 && json.Unmarshal(b, &body) == nil {
				cancel()
				break
			}
		}
		cancel()
	}
	if body == nil {
		body = map[string]any{"nodes": []any{}, "services": []any{}, "tasks": []any{}, "events": []any{}}
	}
	body["extra"] = map[string]any{"demo": true, "processes": nodes, "runtime": s.cfg.Runtime}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(body)
}

func (s *Supervisor) proxy(w http.ResponseWriter, r *http.Request) {
	api, err := s.API()
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	target, _ := url.Parse("http://" + api)
	httputil.NewSingleHostReverseProxy(target).ServeHTTP(w, r)
}
