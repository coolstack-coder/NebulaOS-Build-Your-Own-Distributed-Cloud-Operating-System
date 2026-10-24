// Package state holds NebulaOS's replicated cluster state and the
// deterministic state machine that Raft applies commands to.
package state

type Resources struct {
	CPU    int64 `json:"cpu_millis"`
	Memory int64 `json:"memory_mb"`
}

func (r Resources) Add(o Resources) Resources { return Resources{r.CPU + o.CPU, r.Memory + o.Memory} }
func (r Resources) Sub(o Resources) Resources { return Resources{r.CPU - o.CPU, r.Memory - o.Memory} }
func (r Resources) Fits(in Resources) bool    { return r.CPU <= in.CPU && r.Memory <= in.Memory }

type NodeStatus string

const (
	NodeReady NodeStatus = "ready"
	NodeDown  NodeStatus = "down"
	// NodeDraining nodes keep running what they have but get nothing new.
	NodeDraining NodeStatus = "draining"
)

type Node struct {
	ID         string            `json:"id"`
	Address    string            `json:"address"`
	APIAddress string            `json:"api_address,omitempty"`
	Capacity   Resources         `json:"capacity"`
	Labels     map[string]string `json:"labels,omitempty"`
	Status     NodeStatus        `json:"status"`
	Voter      bool              `json:"voter"`
	Runtime    string            `json:"runtime"`
	JoinedAt   int64             `json:"joined_at"`
	ChangedAt  int64             `json:"changed_at"`
	DownReason string            `json:"down_reason,omitempty"`
}

type Autoscale struct {
	Min int `json:"min"`
	Max int `json:"max"`
	// TargetCPU is the desired average utilisation in percent of each
	// replica's CPU request, like a Kubernetes HPA target.
	TargetCPU int `json:"target_cpu_percent"`
}

type ServiceSpec struct {
	Name         string            `json:"name"`
	Image        string            `json:"image"`
	Command      []string          `json:"command,omitempty"`
	Env          map[string]string `json:"env,omitempty"`
	Replicas     int               `json:"replicas"`
	Resources    Resources         `json:"resources"`
	Port         int               `json:"port,omitempty"`
	NodeSelector map[string]string `json:"node_selector,omitempty"`
	Autoscale    *Autoscale        `json:"autoscale,omitempty"`
	// SimDemand is the total CPU (millicores) the service's traffic needs.
	// Only the simulated runtime uses it; it lets demos drive load.
	SimDemand int64 `json:"sim_demand_millis,omitempty"`
}

type Service struct {
	Spec       ServiceSpec `json:"spec"`
	CreatedAt  int64       `json:"created_at"`
	UpdatedAt  int64       `json:"updated_at"`
	Generation int64       `json:"generation"`
}

type TaskState string

const (
	TaskPending  TaskState = "pending"  // waiting for the scheduler
	TaskAssigned TaskState = "assigned" // bound to a node, not yet confirmed running
	TaskRunning  TaskState = "running"
	TaskFailed   TaskState = "failed"
	TaskLost     TaskState = "lost" // its node died
)

// Active tasks count towards a service's replicas.
func (s TaskState) Active() bool {
	return s == TaskPending || s == TaskAssigned || s == TaskRunning
}

type Task struct {
	ID         string    `json:"id"`
	Service    string    `json:"service"`
	Node       string    `json:"node,omitempty"`
	State      TaskState `json:"state"`
	Message    string    `json:"message,omitempty"`
	Resources  Resources `json:"resources"`
	CreatedAt  int64     `json:"created_at"`
	AssignedAt int64     `json:"assigned_at,omitempty"`
	RunningAt  int64     `json:"running_at,omitempty"`
	Restarts   int32     `json:"restarts"`
	// ReplacedNode records where a rescheduled task's predecessor ran, so the
	// UI can draw "moved from n3".
	ReplacedNode string `json:"replaced_node,omitempty"`
}

type Event struct {
	Seq     uint64 `json:"seq"`
	Time    int64  `json:"time"`
	Kind    string `json:"kind"`
	Message string `json:"message"`
	Node    string `json:"node,omitempty"`
	Service string `json:"service,omitempty"`
	// DurationMs is set on measured events such as recovery.
	DurationMs int64 `json:"duration_ms,omitempty"`
}
