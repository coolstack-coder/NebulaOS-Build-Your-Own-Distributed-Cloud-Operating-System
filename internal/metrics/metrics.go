// Package metrics defines the Prometheus series every NebulaOS node exports
// on /metrics. Gauges describing the whole cluster are only meaningful on the
// leader; followers report them as they last saw them in replicated state.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

var (
	Registry = prometheus.NewRegistry()

	RaftTerm = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "nebula_raft_term", Help: "Current Raft term on this node.",
	})
	RaftIsLeader = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "nebula_raft_is_leader", Help: "1 if this node is the Raft leader.",
	})
	RaftCommitIndex = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "nebula_raft_commit_index", Help: "Highest committed log index known to this node.",
	})
	RaftLeaderChanges = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "nebula_raft_leader_changes_total", Help: "Times this node gained leadership.",
	})
	RaftApplyLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "nebula_raft_apply_seconds",
		Help:    "Time from proposing a command to it being applied on the leader.",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 14),
	})

	SchedulingLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "nebula_scheduling_latency_seconds",
		Help:    "Time from a task being created to being bound to a node.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
	})
	TaskStartup = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "nebula_task_startup_seconds",
		Help:    "Time from a task being created to its node reporting it running.",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 12),
	})
	RecoveryTime = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "nebula_failure_recovery_seconds",
		Help:    "Time from a node being declared down until every service is back at its desired replica count.",
		Buckets: prometheus.ExponentialBuckets(0.1, 1.6, 14),
	})
	NodeFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "nebula_node_failures_total", Help: "Nodes declared down by this leader.",
	})
	Autoscales = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nebula_autoscale_events_total", Help: "Autoscaler decisions.",
	}, []string{"service", "direction"})

	Nodes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "nebula_nodes", Help: "Nodes by status.",
	}, []string{"status"})
	Tasks = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "nebula_tasks", Help: "Tasks by state.",
	}, []string{"state"})
	NodeCPU = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "nebula_node_cpu_used_millicores", Help: "CPU used on each node, as reported by its agent.",
	}, []string{"node"})
	NodeMemory = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "nebula_node_memory_used_mb", Help: "Memory used on each node, as reported by its agent.",
	}, []string{"node"})
	ServiceReplicas = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "nebula_service_replicas", Help: "Replica counts per service.",
	}, []string{"service", "kind"})
	ServiceCPUUtil = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "nebula_service_cpu_utilization_ratio", Help: "Average CPU used / requested across running replicas.",
	}, []string{"service"})
)

func init() {
	Registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		RaftTerm, RaftIsLeader, RaftCommitIndex, RaftLeaderChanges, RaftApplyLatency,
		SchedulingLatency, TaskStartup, RecoveryTime, NodeFailures, Autoscales,
		Nodes, Tasks, NodeCPU, NodeMemory, ServiceReplicas, ServiceCPUUtil,
	)
}
