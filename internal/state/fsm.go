package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"sort"
	"sync"
)

const maxEvents = 400

// Op names a state machine operation.
type Op string

const (
	OpRegisterNode  Op = "register_node"
	OpSetNodeStatus Op = "set_node_status"
	OpCreateService Op = "create_service"
	OpUpdateService Op = "update_service"
	OpDeleteService Op = "delete_service"
	OpCreateTasks   Op = "create_tasks"
	OpAssignTasks   Op = "assign_tasks"
	OpUpdateTasks   Op = "update_tasks"
	OpRemoveTasks   Op = "remove_tasks"
	OpRecordEvent   Op = "record_event"
)

// Command is the unit replicated through Raft. Everything the state machine
// needs, including timestamps and generated ids, travels inside the command
// so that every replica computes the same result.
type Command struct {
	Op   Op    `json:"op"`
	Time int64 `json:"time"`

	Node   *Node      `json:"node,omitempty"`
	NodeID string     `json:"node_id,omitempty"`
	Status NodeStatus `json:"status,omitempty"`
	Reason string     `json:"reason,omitempty"`

	Spec  *ServiceSpec  `json:"spec,omitempty"`
	Name  string        `json:"name,omitempty"`
	Patch *ServicePatch `json:"patch,omitempty"`

	Tasks       []Task       `json:"tasks,omitempty"`
	Assignments []Assignment `json:"assignments,omitempty"`
	Updates     []TaskUpdate `json:"updates,omitempty"`
	TaskIDs     []string     `json:"task_ids,omitempty"`

	Event *Event `json:"event,omitempty"`
}

type ServicePatch struct {
	Replicas  *int       `json:"replicas,omitempty"`
	Image     *string    `json:"image,omitempty"`
	SimDemand *int64     `json:"sim_demand_millis,omitempty"`
	Autoscale *Autoscale `json:"autoscale,omitempty"`
	// ClearAutoscale turns autoscaling off.
	ClearAutoscale bool `json:"clear_autoscale,omitempty"`
}

type Assignment struct {
	TaskID string `json:"task_id"`
	NodeID string `json:"node_id"`
	Score  int    `json:"score"`
}

type TaskUpdate struct {
	TaskID   string    `json:"task_id"`
	NodeID   string    `json:"node_id"`
	State    TaskState `json:"state"`
	Message  string    `json:"message,omitempty"`
	Restarts int32     `json:"restarts"`
}

func (c Command) Encode() []byte {
	b, err := json.Marshal(c)
	if err != nil {
		panic(err)
	}
	return b
}

// Snapshot is the full replicated state. It is also what the API serves.
type Snapshot struct {
	Nodes    map[string]*Node    `json:"nodes"`
	Services map[string]*Service `json:"services"`
	Tasks    map[string]*Task    `json:"tasks"`
	Events   []Event             `json:"events"`
	EventSeq uint64              `json:"event_seq"`
	Applied  uint64              `json:"applied_index"`
}

func emptySnapshot() *Snapshot {
	return &Snapshot{Nodes: map[string]*Node{}, Services: map[string]*Service{}, Tasks: map[string]*Task{}}
}

// FSM implements raft.FSM. Readers use View, which returns a deep copy, so
// they never observe a half-applied command.
type FSM struct {
	mu sync.RWMutex
	s  *Snapshot
	// onApply, if set, is called after every command with the applied index.
	onApply func(index uint64, c *Command)
}

func NewFSM() *FSM { return &FSM{s: emptySnapshot()} }

func (f *FSM) OnApply(fn func(uint64, *Command)) { f.onApply = fn }

var ErrUnknownOp = errors.New("unknown op")

func (f *FSM) Apply(index uint64, data []byte) any {
	var c Command
	if err := json.Unmarshal(data, &c); err != nil {
		return fmt.Errorf("decode command: %w", err)
	}
	f.mu.Lock()
	err := f.apply(&c)
	f.s.Applied = index
	f.mu.Unlock()
	if f.onApply != nil {
		f.onApply(index, &c)
	}
	return err
}

func (f *FSM) Snapshot() ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return json.Marshal(f.s)
}

func (f *FSM) Restore(data []byte) error {
	s := emptySnapshot()
	if err := json.Unmarshal(data, s); err != nil {
		return err
	}
	f.mu.Lock()
	f.s = s
	f.mu.Unlock()
	return nil
}

// View returns a deep copy of the current state.
func (f *FSM) View() *Snapshot {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := &Snapshot{
		Nodes:    make(map[string]*Node, len(f.s.Nodes)),
		Services: make(map[string]*Service, len(f.s.Services)),
		Tasks:    make(map[string]*Task, len(f.s.Tasks)),
		Events:   append([]Event(nil), f.s.Events...),
		EventSeq: f.s.EventSeq,
		Applied:  f.s.Applied,
	}
	for k, v := range f.s.Nodes {
		n := *v
		n.Labels = maps.Clone(v.Labels)
		out.Nodes[k] = &n
	}
	for k, v := range f.s.Services {
		s := *v
		s.Spec.Env = maps.Clone(v.Spec.Env)
		s.Spec.NodeSelector = maps.Clone(v.Spec.NodeSelector)
		s.Spec.Command = append([]string(nil), v.Spec.Command...)
		if v.Spec.Autoscale != nil {
			a := *v.Spec.Autoscale
			s.Spec.Autoscale = &a
		}
		out.Services[k] = &s
	}
	for k, v := range f.s.Tasks {
		t := *v
		out.Tasks[k] = &t
	}
	return out
}

func (f *FSM) event(c *Command, kind, msg, node, svc string) {
	f.s.EventSeq++
	f.s.Events = append(f.s.Events, Event{
		Seq: f.s.EventSeq, Time: c.Time, Kind: kind, Message: msg, Node: node, Service: svc,
	})
	if len(f.s.Events) > maxEvents {
		f.s.Events = f.s.Events[len(f.s.Events)-maxEvents:]
	}
}

func (f *FSM) apply(c *Command) error {
	s := f.s
	switch c.Op {
	case OpRegisterNode:
		if c.Node == nil || c.Node.ID == "" {
			return errors.New("register_node: missing node")
		}
		prev, existed := s.Nodes[c.Node.ID]
		n := *c.Node
		n.Status = NodeReady
		n.ChangedAt = c.Time
		n.DownReason = ""
		if existed {
			n.JoinedAt = prev.JoinedAt
		} else {
			n.JoinedAt = c.Time
		}
		s.Nodes[n.ID] = &n
		switch {
		case !existed:
			f.event(c, "node_joined", fmt.Sprintf("%s joined (%dm CPU, %d MB, runtime %s)", n.ID, n.Capacity.CPU, n.Capacity.Memory, n.Runtime), n.ID, "")
		case prev.Status == NodeDown:
			f.event(c, "node_recovered", fmt.Sprintf("%s is back online", n.ID), n.ID, "")
		}
		return nil

	case OpSetNodeStatus:
		n, ok := s.Nodes[c.NodeID]
		if !ok {
			return fmt.Errorf("unknown node %q", c.NodeID)
		}
		if n.Status == c.Status {
			return nil
		}
		n.Status = c.Status
		n.ChangedAt = c.Time
		n.DownReason = c.Reason
		if c.Status == NodeDown {
			lost := 0
			for _, t := range s.Tasks {
				if t.Node == n.ID && t.State.Active() {
					t.State = TaskLost
					t.Message = "node " + n.ID + " down"
					lost++
				}
			}
			f.event(c, "node_down", fmt.Sprintf("%s marked down (%s); %d task(s) lost", n.ID, c.Reason, lost), n.ID, "")
		} else if c.Status == NodeDraining {
			// Draining evicts everything; the reconciler replaces the tasks
			// on other nodes and the scheduler skips this one.
			evicted := 0
			for _, t := range s.Tasks {
				if t.Node == n.ID && t.State.Active() {
					t.State = TaskLost
					t.Message = "evicted: " + n.ID + " draining"
					evicted++
				}
			}
			f.event(c, "node_drained", fmt.Sprintf("%s cordoned and drained; %d task(s) evicted", n.ID, evicted), n.ID, "")
		} else {
			f.event(c, "node_status", fmt.Sprintf("%s is now %s", n.ID, c.Status), n.ID, "")
		}
		return nil

	case OpCreateService:
		if c.Spec == nil || c.Spec.Name == "" {
			return errors.New("create_service: missing spec")
		}
		if _, ok := s.Services[c.Spec.Name]; ok {
			return fmt.Errorf("service %q already exists", c.Spec.Name)
		}
		spec := *c.Spec
		s.Services[spec.Name] = &Service{Spec: spec, CreatedAt: c.Time, UpdatedAt: c.Time, Generation: 1}
		f.event(c, "service_created", fmt.Sprintf("deployed %s (%s) x%d", spec.Name, spec.Image, spec.Replicas), "", spec.Name)
		return nil

	case OpUpdateService:
		svc, ok := s.Services[c.Name]
		if !ok {
			return fmt.Errorf("unknown service %q", c.Name)
		}
		p := c.Patch
		if p == nil {
			return nil
		}
		if p.Replicas != nil && *p.Replicas != svc.Spec.Replicas {
			from := svc.Spec.Replicas
			svc.Spec.Replicas = *p.Replicas
			kind := "service_scaled"
			if c.Reason != "" && c.Reason != "manual" {
				kind = "autoscaled"
			}
			msg := fmt.Sprintf("%s scaled %d → %d", c.Name, from, *p.Replicas)
			if c.Reason != "" {
				msg += " (" + c.Reason + ")"
			}
			f.event(c, kind, msg, "", c.Name)
		}
		if p.Image != nil && *p.Image != svc.Spec.Image {
			svc.Spec.Image = *p.Image
			f.event(c, "service_updated", fmt.Sprintf("%s image → %s", c.Name, *p.Image), "", c.Name)
		}
		if p.SimDemand != nil {
			svc.Spec.SimDemand = *p.SimDemand
		}
		if p.Autoscale != nil {
			a := *p.Autoscale
			svc.Spec.Autoscale = &a
		}
		if p.ClearAutoscale {
			svc.Spec.Autoscale = nil
		}
		svc.UpdatedAt = c.Time
		svc.Generation++
		return nil

	case OpDeleteService:
		if _, ok := s.Services[c.Name]; !ok {
			return fmt.Errorf("unknown service %q", c.Name)
		}
		delete(s.Services, c.Name)
		for id, t := range s.Tasks {
			if t.Service == c.Name {
				delete(s.Tasks, id)
			}
		}
		f.event(c, "service_deleted", "deleted "+c.Name, "", c.Name)
		return nil

	case OpCreateTasks:
		for _, t := range c.Tasks {
			if _, ok := s.Services[t.Service]; !ok {
				continue // service deleted while the command was in flight
			}
			if _, ok := s.Tasks[t.ID]; ok {
				continue
			}
			nt := t
			nt.State = TaskPending
			nt.CreatedAt = c.Time
			s.Tasks[nt.ID] = &nt
		}
		return nil

	case OpAssignTasks:
		for _, a := range c.Assignments {
			t, ok := s.Tasks[a.TaskID]
			n, nok := s.Nodes[a.NodeID]
			if !ok || !nok || t.State != TaskPending || n.Status != NodeReady {
				continue
			}
			t.State = TaskAssigned
			t.Node = a.NodeID
			t.AssignedAt = c.Time
			msg := fmt.Sprintf("%s → %s (score %d)", t.ID, a.NodeID, a.Score)
			if t.ReplacedNode != "" {
				msg = fmt.Sprintf("%s rescheduled %s → %s (score %d)", t.ID, t.ReplacedNode, a.NodeID, a.Score)
			}
			f.event(c, "task_scheduled", msg, a.NodeID, t.Service)
		}
		return nil

	case OpUpdateTasks:
		for _, u := range c.Updates {
			t, ok := s.Tasks[u.TaskID]
			// Reports from a node that no longer owns the task (for example
			// one that came back after being declared dead) are ignored.
			if !ok || t.Node != u.NodeID || !t.State.Active() {
				continue
			}
			if t.State != TaskRunning && u.State == TaskRunning {
				t.RunningAt = c.Time
			}
			if u.State == TaskFailed && t.State != TaskFailed {
				f.event(c, "task_failed", fmt.Sprintf("%s failed on %s: %s", t.ID, t.Node, u.Message), t.Node, t.Service)
			}
			t.State = u.State
			t.Message = u.Message
			t.Restarts = u.Restarts
		}
		return nil

	case OpRemoveTasks:
		for _, id := range c.TaskIDs {
			delete(s.Tasks, id)
		}
		return nil

	case OpRecordEvent:
		if c.Event != nil {
			f.event(c, c.Event.Kind, c.Event.Message, c.Event.Node, c.Event.Service)
			f.s.Events[len(f.s.Events)-1].DurationMs = c.Event.DurationMs
		}
		return nil
	}
	return fmt.Errorf("%w: %s", ErrUnknownOp, c.Op)
}

// ---------------------------------------------------------------- queries

// Allocated sums the requests of active tasks on each node.
func (s *Snapshot) Allocated() map[string]Resources {
	out := map[string]Resources{}
	for _, t := range s.Tasks {
		if t.Node != "" && t.State.Active() {
			out[t.Node] = out[t.Node].Add(t.Resources)
		}
	}
	return out
}

// TasksFor returns a service's tasks sorted by id.
func (s *Snapshot) TasksFor(service string) []*Task {
	var out []*Task
	for _, t := range s.Tasks {
		if t.Service == service {
			out = append(out, t)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// TasksOn returns the active tasks bound to a node.
func (s *Snapshot) TasksOn(node string) []*Task {
	var out []*Task
	for _, t := range s.Tasks {
		if t.Node == node && t.State.Active() {
			out = append(out, t)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Read runs fn against the live state under a read lock. fn must not keep
// references or mutate anything; use View for that.
func (f *FSM) Read(fn func(s *Snapshot)) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	fn(f.s)
}
