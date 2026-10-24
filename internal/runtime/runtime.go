// Package runtime abstracts how a node actually runs a task. The agent only
// ever talks to the Runtime interface, so the same control plane can drive
// Docker, containerd (through nerdctl), bare processes, or a simulator.
package runtime

import (
	"context"
	"fmt"
	"time"
)

type State string

const (
	Starting State = "starting"
	Running  State = "running"
	Exited   State = "exited" // stopped on its own with code 0
	Failed   State = "failed"
)

type Spec struct {
	TaskID  string
	Service string
	Image   string
	Command []string
	Env     map[string]string
	CPU     int64 // millicores
	Memory  int64 // MB
	Port    int
	// Simulator inputs, see ServiceSpec.SimDemand.
	SimDemand int64
	Replicas  int
}

type Status struct {
	State     State
	Message   string
	CPU       int64 // millicores in use
	Memory    int64 // MB in use
	StartedAt time.Time
}

type Runtime interface {
	Name() string
	Start(ctx context.Context, spec Spec) error
	Stop(ctx context.Context, taskID string) error
	Status(taskID string) (Status, bool)
	// List returns the ids of every task this runtime currently holds for
	// this node, including ones started by a previous agent process.
	List(ctx context.Context) ([]string, error)
	// Update pushes spec changes that do not need a restart.
	Update(spec Spec)
	Close() error
}

type Options struct {
	NodeID  string
	DataDir string
	// Isolate puts process-runtime tasks in fresh Linux namespaces.
	Isolate bool
}

func New(name string, opts Options) (Runtime, error) {
	switch name {
	case "sim", "":
		return NewSim(), nil
	case "process":
		return NewProcess(opts)
	case "docker":
		return NewOCI("docker", opts)
	case "containerd", "nerdctl":
		// nerdctl is containerd's Docker-compatible CLI, so the same driver
		// works against containerd directly.
		return NewOCI("nerdctl", opts)
	}
	return nil, fmt.Errorf("unknown runtime %q (want sim, process, docker or containerd)", name)
}
