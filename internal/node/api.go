package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"sort"
	"time"

	"github.com/coolstack-coder/nebulaos/internal/agent"
	"github.com/coolstack-coder/nebulaos/internal/controller"
	"github.com/coolstack-coder/nebulaos/internal/metrics"
	"github.com/coolstack-coder/nebulaos/internal/raft"
	"github.com/coolstack-coder/nebulaos/internal/state"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Overview is everything the dashboard needs in one document.
type Overview struct {
	ServedBy string           `json:"served_by"`
	Leader   string           `json:"leader"`
	Raft     *raft.Status     `json:"raft,omitempty"`
	Agent    agent.Status     `json:"agent"`
	Nodes    []*state.Node    `json:"nodes"`
	Services []ServiceView    `json:"services"`
	Tasks    []*state.Task    `json:"tasks"`
	Events   []state.Event    `json:"events"`
	Live     *controller.Live `json:"live,omitempty"`
	Applied  uint64           `json:"applied_index"`
	Time     int64            `json:"time"`
	Extra    map[string]any   `json:"extra,omitempty"`
}

type ServiceView struct {
	state.Service
	Running int `json:"running"`
	Pending int `json:"pending"`
}

var nameRE = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,30}[a-z0-9])?$`)

func (n *Node) routes() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(metrics.Registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
	mux.HandleFunc("GET /api/raft", n.handleRaft)
	mux.HandleFunc("GET /api/overview", n.handleOverview)
	mux.HandleFunc("GET /api/services", n.handleListServices)
	mux.HandleFunc("POST /api/services", n.handleCreateService)
	mux.HandleFunc("PATCH /api/services/{name}", n.handlePatchService)
	mux.HandleFunc("DELETE /api/services/{name}", n.handleDeleteService)
	mux.HandleFunc("POST /api/nodes/{id}/{action}", n.handleNodeAction)
	if n.cfg.UI != nil {
		mux.Handle("/", n.cfg.UI)
	}
	return cors(mux)
}

func cors(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		h.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]string{"error": err.Error()})
}

func (n *Node) handleRaft(w http.ResponseWriter, _ *http.Request) {
	if n.raft == nil {
		writeJSON(w, http.StatusOK, map[string]any{"id": n.cfg.ID, "role": "worker", "agent": n.ag.Status()})
		return
	}
	s := n.raft.Status()
	writeJSON(w, http.StatusOK, map[string]any{
		"id": s.ID, "role": s.Role, "term": s.Term, "leader": s.Leader,
		"commit_index": s.CommitIndex, "last_index": s.LastIndex, "snapshot_index": s.SnapshotIndex,
		"stats": s.Stats, "agent": n.ag.Status(), "uptime_ms": time.Since(n.started).Milliseconds(),
	})
}

// handleOverview serves the leader's view. Followers proxy to the leader so
// the dashboard gets live metrics no matter which node it points at, and
// fall back to their own (possibly slightly stale) replica if they can't.
func (n *Node) handleOverview(w http.ResponseWriter, r *http.Request) {
	if n.raft == nil {
		writeErr(w, http.StatusNotFound, errors.New("worker nodes do not serve cluster state"))
		return
	}
	if !n.raft.IsLeader() && r.URL.Query().Get("local") == "" {
		if lid, _ := n.raft.Leader(); lid != "" {
			var api string
			n.fsm.Read(func(s *state.Snapshot) {
				if nd := s.Nodes[lid]; nd != nil {
					api = nd.APIAddress
				}
			})
			if api != "" {
				ctx, cancel := context.WithTimeout(r.Context(), 700*time.Millisecond)
				defer cancel()
				req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+api+"/api/overview?local=1", nil)
				if resp, err := http.DefaultClient.Do(req); err == nil {
					defer resp.Body.Close()
					if resp.StatusCode == http.StatusOK {
						w.Header().Set("Content-Type", "application/json")
						_, _ = io.Copy(w, resp.Body)
						return
					}
				}
			}
		}
	}
	writeJSON(w, http.StatusOK, n.Overview())
}

func (n *Node) Overview() *Overview {
	rs := n.raft.Status()
	view := n.fsm.View()
	o := &Overview{
		ServedBy: n.cfg.ID, Leader: rs.Leader, Raft: &rs, Agent: n.ag.Status(),
		Events: view.Events, Applied: view.Applied, Time: time.Now().UnixMilli(),
	}
	if len(o.Events) > 120 {
		o.Events = o.Events[len(o.Events)-120:]
	}
	if rs.Role == "leader" {
		o.Live = n.ctrl.Live()
	}
	for _, nd := range view.Nodes {
		o.Nodes = append(o.Nodes, nd)
	}
	sort.Slice(o.Nodes, func(i, j int) bool { return o.Nodes[i].ID < o.Nodes[j].ID })
	for _, t := range view.Tasks {
		o.Tasks = append(o.Tasks, t)
	}
	sort.Slice(o.Tasks, func(i, j int) bool { return o.Tasks[i].ID < o.Tasks[j].ID })
	for _, s := range view.Services {
		sv := ServiceView{Service: *s}
		for _, t := range view.TasksFor(s.Spec.Name) {
			switch t.State {
			case state.TaskRunning:
				sv.Running++
			case state.TaskPending, state.TaskAssigned:
				sv.Pending++
			}
		}
		o.Services = append(o.Services, sv)
	}
	sort.Slice(o.Services, func(i, j int) bool { return o.Services[i].Spec.Name < o.Services[j].Spec.Name })
	return o
}

func (n *Node) handleListServices(w http.ResponseWriter, _ *http.Request) {
	if n.fsm == nil {
		writeErr(w, http.StatusNotFound, errors.New("not a control-plane node"))
		return
	}
	writeJSON(w, http.StatusOK, n.Overview().Services)
}

func (n *Node) submitHTTP(w http.ResponseWriter, r *http.Request, cmd state.Command, okCode int, body any) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := n.Submit(ctx, cmd); err != nil {
		writeErr(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, okCode, body)
}

func (n *Node) handleCreateService(w http.ResponseWriter, r *http.Request) {
	var spec state.ServiceSpec
	if err := json.NewDecoder(r.Body).Decode(&spec); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if err := Validate(&spec); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	n.submitHTTP(w, r, state.Command{Op: state.OpCreateService, Spec: &spec}, http.StatusCreated, spec)
}

// Validate checks a spec and fills in defaults.
func Validate(s *state.ServiceSpec) error {
	if !nameRE.MatchString(s.Name) {
		return fmt.Errorf("invalid service name %q (lowercase letters, digits and dashes)", s.Name)
	}
	if s.Image == "" && len(s.Command) == 0 {
		return errors.New("image or command is required")
	}
	if s.Replicas < 0 || s.Replicas > 500 {
		return errors.New("replicas must be between 0 and 500")
	}
	if s.Resources.CPU == 0 {
		s.Resources.CPU = 250
	}
	if s.Resources.Memory == 0 {
		s.Resources.Memory = 128
	}
	if a := s.Autoscale; a != nil {
		if a.Min < 1 || a.Max < a.Min || a.TargetCPU < 1 || a.TargetCPU > 100 {
			return errors.New("autoscale needs 1 <= min <= max and target_cpu_percent in 1..100")
		}
		s.Replicas = min(max(s.Replicas, a.Min), a.Max)
	}
	return nil
}

func (n *Node) handlePatchService(w http.ResponseWriter, r *http.Request) {
	var p state.ServicePatch
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if p.Replicas != nil && (*p.Replicas < 0 || *p.Replicas > 500) {
		writeErr(w, http.StatusBadRequest, errors.New("replicas must be between 0 and 500"))
		return
	}
	reason := ""
	if p.Replicas != nil {
		reason = "manual"
	}
	n.submitHTTP(w, r, state.Command{Op: state.OpUpdateService, Name: r.PathValue("name"), Patch: &p, Reason: reason}, http.StatusOK, p)
}

func (n *Node) handleDeleteService(w http.ResponseWriter, r *http.Request) {
	n.submitHTTP(w, r, state.Command{Op: state.OpDeleteService, Name: r.PathValue("name")}, http.StatusOK, map[string]string{"deleted": r.PathValue("name")})
}

func (n *Node) handleNodeAction(w http.ResponseWriter, r *http.Request) {
	st := map[string]state.NodeStatus{"drain": state.NodeDraining, "uncordon": state.NodeReady}[r.PathValue("action")]
	if st == "" {
		writeErr(w, http.StatusNotFound, errors.New("action must be drain or uncordon"))
		return
	}
	n.submitHTTP(w, r, state.Command{Op: state.OpSetNodeStatus, NodeID: r.PathValue("id"), Status: st, Reason: "operator"}, http.StatusOK,
		map[string]string{"node": r.PathValue("id"), "status": string(st)})
}
