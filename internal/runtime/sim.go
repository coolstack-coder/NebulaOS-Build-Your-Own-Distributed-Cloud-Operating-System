package runtime

import (
	"context"
	"math"
	"math/rand/v2"
	"sync"
	"time"
)

// Sim is an in-process runtime that models resource usage instead of running
// anything. Each replica serves an equal share of the service's SimDemand,
// capped at 1.5x its CPU request (a soft limit, like a cgroup with burst).
// It exists so the full control plane, including autoscaling, can be
// demonstrated and benchmarked on any laptop.
type Sim struct {
	mu    sync.Mutex
	tasks map[string]*simTask
	start time.Duration
}

type simTask struct {
	spec    Spec
	created time.Time
	phase   float64
}

func NewSim() *Sim { return &Sim{tasks: map[string]*simTask{}, start: 250 * time.Millisecond} }

func (s *Sim) Name() string { return "sim" }

func (s *Sim) Start(_ context.Context, spec Spec) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks[spec.TaskID] = &simTask{spec: spec, created: time.Now(), phase: rand.Float64() * 2 * math.Pi}
	return nil
}

func (s *Sim) Stop(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.tasks, id)
	return nil
}

func (s *Sim) Update(spec Spec) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t, ok := s.tasks[spec.TaskID]; ok {
		t.spec.SimDemand = spec.SimDemand
		t.spec.Replicas = spec.Replicas
	}
}

func (s *Sim) Status(id string) (Status, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok {
		return Status{}, false
	}
	age := time.Since(t.created)
	if age < s.start {
		return Status{State: Starting, Message: "pulling image", StartedAt: t.created}, true
	}
	req := float64(max(t.spec.CPU, 1))
	share := 0.05 * req // idle baseline
	if t.spec.SimDemand > 0 && t.spec.Replicas > 0 {
		share = float64(t.spec.SimDemand) / float64(t.spec.Replicas)
	}
	// A slow wobble so charts look alive without moving the average.
	share *= 1 + 0.06*math.Sin(age.Seconds()/3+t.phase)
	cpu := math.Min(share, 1.5*req)
	mem := float64(t.spec.Memory) * (0.35 + 0.35*math.Min(cpu/req, 1))
	return Status{State: Running, CPU: int64(cpu), Memory: int64(mem), StartedAt: t.created}, true
}

func (s *Sim) List(context.Context) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.tasks))
	for id := range s.tasks {
		out = append(out, id)
	}
	return out, nil
}

func (s *Sim) Close() error { return nil }
