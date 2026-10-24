package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/coolstack-coder/nebulaos/internal/node"
	"github.com/coolstack-coder/nebulaos/internal/state"
)

const ctlUsage = `Usage: nebula ctl [--addr URL] <command>

Commands:
  status                         cluster summary
  get nodes|services|tasks|events
  deploy NAME [flags]            create a service (nebula ctl deploy -h)
  apply -f FILE.json             create a service from a JSON spec
  scale NAME REPLICAS
  autoscale NAME MIN:MAX:TARGET%  |  autoscale NAME off
  load NAME MILLICORES           set simulated traffic (sim runtime)
  delete NAME
  drain NODE | uncordon NODE

The address defaults to $NEBULA_ADDR or http://127.0.0.1:8001; any node works.
`

type ctl struct {
	addr string
	http *http.Client
}

func runCtl(args []string) error {
	fs := flag.NewFlagSet("ctl", flag.ExitOnError)
	def := os.Getenv("NEBULA_ADDR")
	if def == "" {
		def = "http://127.0.0.1:8001"
	}
	addr := fs.String("addr", def, "API address of any node")
	fs.Usage = func() { fmt.Fprint(os.Stderr, ctlUsage) }
	fs.Parse(args)
	rest := fs.Args()
	if len(rest) == 0 || rest[0] == "help" {
		fmt.Print(ctlUsage)
		return nil
	}
	c := &ctl{addr: strings.TrimRight(*addr, "/"), http: &http.Client{Timeout: 10 * time.Second}}
	cmd, rest := rest[0], rest[1:]
	switch cmd {
	case "status":
		return c.status()
	case "get":
		if len(rest) != 1 {
			return errors.New("usage: get nodes|services|tasks|events")
		}
		return c.get(rest[0])
	case "deploy":
		return c.deploy(rest)
	case "apply":
		return c.apply(rest)
	case "scale":
		if len(rest) != 2 {
			return errors.New("usage: scale NAME REPLICAS")
		}
		n, err := strconv.Atoi(rest[1])
		if err != nil {
			return err
		}
		return c.patch(rest[0], state.ServicePatch{Replicas: &n})
	case "load":
		if len(rest) != 2 {
			return errors.New("usage: load NAME MILLICORES")
		}
		v, err := strconv.ParseInt(rest[1], 10, 64)
		if err != nil {
			return err
		}
		return c.patch(rest[0], state.ServicePatch{SimDemand: &v})
	case "autoscale":
		if len(rest) != 2 {
			return errors.New("usage: autoscale NAME MIN:MAX:TARGET | off")
		}
		if rest[1] == "off" {
			return c.patch(rest[0], state.ServicePatch{ClearAutoscale: true})
		}
		a, err := parseAutoscale(rest[1])
		if err != nil {
			return err
		}
		return c.patch(rest[0], state.ServicePatch{Autoscale: a})
	case "delete":
		if len(rest) != 1 {
			return errors.New("usage: delete NAME")
		}
		return c.do(http.MethodDelete, "/api/services/"+rest[0], nil, nil)
	case "drain", "uncordon":
		if len(rest) != 1 {
			return fmt.Errorf("usage: %s NODE", cmd)
		}
		return c.do(http.MethodPost, "/api/nodes/"+rest[0]+"/"+cmd, nil, nil)
	}
	return fmt.Errorf("unknown ctl command %q", cmd)
}

func parseAutoscale(s string) (*state.Autoscale, error) {
	p := strings.Split(strings.TrimSuffix(s, "%"), ":")
	if len(p) != 3 {
		return nil, fmt.Errorf("autoscale must look like MIN:MAX:TARGET, got %q", s)
	}
	var v [3]int
	for i := range p {
		n, err := strconv.Atoi(strings.TrimSuffix(p[i], "%"))
		if err != nil {
			return nil, err
		}
		v[i] = n
	}
	return &state.Autoscale{Min: v[0], Max: v[1], TargetCPU: v[2]}, nil
}

func (c *ctl) do(method, path string, body, out any) error {
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, c.addr+path, rd)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		var e struct{ Error string }
		if json.Unmarshal(b, &e) == nil && e.Error != "" {
			return errors.New(e.Error)
		}
		return fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	if out != nil {
		return json.Unmarshal(b, out)
	}
	if method != http.MethodGet {
		fmt.Println("ok")
	}
	return nil
}

func (c *ctl) patch(name string, p state.ServicePatch) error {
	return c.do(http.MethodPatch, "/api/services/"+name, p, nil)
}

func (c *ctl) overview() (*node.Overview, error) {
	var o node.Overview
	return &o, c.do(http.MethodGet, "/api/overview", nil, &o)
}

func ago(ms int64) string {
	if ms <= 0 {
		return "-"
	}
	return time.Since(time.UnixMilli(ms)).Truncate(time.Second).String()
}

func (c *ctl) status() error {
	o, err := c.overview()
	if err != nil {
		return err
	}
	ready, running, desired := 0, 0, 0
	for _, n := range o.Nodes {
		if n.Status == state.NodeReady {
			ready++
		}
	}
	for _, s := range o.Services {
		running += s.Running
		desired += s.Spec.Replicas
	}
	if o.Raft == nil {
		return errors.New("no reachable control-plane node")
	}
	fmt.Printf("leader:    %s (term %d, commit index %d)\n", o.Leader, o.Raft.Term, o.Raft.CommitIndex)
	fmt.Printf("nodes:     %d/%d ready\n", ready, len(o.Nodes))
	fmt.Printf("services:  %d, replicas %d/%d running\n", len(o.Services), running, desired)
	if o.Live != nil {
		fmt.Printf("scheduler: p50 %.1fms, p99 %.1fms\n", o.Live.SchedP50Ms, o.Live.SchedP99Ms)
		if n := len(o.Live.Recoveries); n > 0 {
			r := o.Live.Recoveries[n-1]
			fmt.Printf("last self-heal: %v recovered in %dms\n", r.Nodes, r.DurationMs)
		}
	}
	return nil
}

func (c *ctl) get(what string) error {
	o, err := c.overview()
	if err != nil {
		return err
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	defer w.Flush()
	switch what {
	case "nodes", "node", "no":
		fmt.Fprintln(w, "NODE\tSTATUS\tROLE\tCPU USED/CAP\tMEM USED/CAP\tTASKS\tRUNTIME\tADDRESS")
		tasks := map[string]int{}
		for _, t := range o.Tasks {
			if t.State.Active() {
				tasks[t.Node]++
			}
		}
		for _, n := range o.Nodes {
			role := "worker"
			if n.Voter {
				role = "voter"
			}
			if n.ID == o.Leader {
				role = "leader"
			}
			var used state.Resources
			if o.Live != nil {
				used = o.Live.Nodes[n.ID].Usage
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%dm/%dm\t%dMB/%dMB\t%d\t%s\t%s\n", n.ID, n.Status, role,
				used.CPU, n.Capacity.CPU, used.Memory, n.Capacity.Memory, tasks[n.ID], n.Runtime, n.Address)
		}
	case "services", "service", "svc":
		fmt.Fprintln(w, "SERVICE\tIMAGE\tREPLICAS\tCPU UTIL\tAUTOSCALE\tAGE")
		for _, s := range o.Services {
			auto := "-"
			if a := s.Spec.Autoscale; a != nil {
				auto = fmt.Sprintf("%d-%d @ %d%%", a.Min, a.Max, a.TargetCPU)
			}
			util := "-"
			if o.Live != nil {
				util = fmt.Sprintf("%.0f%%", o.Live.ServiceUtil[s.Spec.Name]*100)
			}
			img := s.Spec.Image
			if img == "" {
				img = strings.Join(s.Spec.Command, " ")
			}
			fmt.Fprintf(w, "%s\t%s\t%d/%d\t%s\t%s\t%s\n", s.Spec.Name, img, s.Running, s.Spec.Replicas, util, auto, ago(s.CreatedAt))
		}
	case "tasks", "task":
		fmt.Fprintln(w, "TASK\tSERVICE\tNODE\tSTATE\tRESTARTS\tAGE\tMESSAGE")
		for _, t := range o.Tasks {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d\t%s\t%s\n", t.ID, t.Service, t.Node, t.State, t.Restarts, ago(t.CreatedAt), t.Message)
		}
	case "events", "event":
		fmt.Fprintln(w, "TIME\tKIND\tMESSAGE")
		for _, e := range o.Events {
			fmt.Fprintf(w, "%s\t%s\t%s\n", time.UnixMilli(e.Time).Format("15:04:05.000"), e.Kind, e.Message)
		}
	default:
		return fmt.Errorf("unknown resource %q", what)
	}
	return nil
}

func (c *ctl) deploy(args []string) error {
	fs := flag.NewFlagSet("deploy", flag.ExitOnError)
	image := fs.String("image", "", "container image")
	cmd := fs.String("cmd", "", "command (space separated)")
	replicas := fs.Int("replicas", 1, "replica count")
	cpu := fs.Int64("cpu", 250, "CPU request in millicores")
	mem := fs.Int64("mem", 128, "memory request in MB")
	port := fs.Int("port", 0, "container port")
	auto := fs.String("autoscale", "", "MIN:MAX:TARGET% to enable autoscaling")
	demand := fs.Int64("load", 0, "simulated CPU demand in millicores (sim runtime)")
	sel := fs.String("selector", "", "node selector k=v,...")
	if len(args) == 0 || strings.HasPrefix(args[0], "-") {
		return errors.New("usage: deploy NAME [--image IMG] [--replicas N] ...")
	}
	name := args[0]
	fs.Parse(args[1:])
	spec := state.ServiceSpec{
		Name: name, Image: *image, Replicas: *replicas, Port: *port, SimDemand: *demand,
		Resources: state.Resources{CPU: *cpu, Memory: *mem},
	}
	if *cmd != "" {
		spec.Command = strings.Fields(*cmd)
	}
	if *auto != "" {
		a, err := parseAutoscale(*auto)
		if err != nil {
			return err
		}
		spec.Autoscale = a
	}
	if *sel != "" {
		m, err := parseKV(*sel)
		if err != nil {
			return err
		}
		spec.NodeSelector = m
	}
	return c.do(http.MethodPost, "/api/services", spec, nil)
}

func (c *ctl) apply(args []string) error {
	if len(args) != 2 || args[0] != "-f" {
		return errors.New("usage: apply -f FILE.json")
	}
	b, err := os.ReadFile(args[1])
	if err != nil {
		return err
	}
	var spec state.ServiceSpec
	if err := json.Unmarshal(b, &spec); err != nil {
		return err
	}
	return c.do(http.MethodPost, "/api/services", spec, nil)
}
