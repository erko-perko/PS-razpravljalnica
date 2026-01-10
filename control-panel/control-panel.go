package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	pb "razpravljalnica/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

// ServerProcess tracks a running server process (optional launcher)
type ServerProcess struct {
	NodeID  string
	Address string
	Port    int
	Cmd     *exec.Cmd
}

type processInfo struct {
	PID       int       `json:"pid"`
	Address   string    `json:"address"`
	StartedAt time.Time `json:"started_at"`
}

type processRegistry struct {
	Version   int                    `json:"version"`
	Processes map[string]processInfo `json:"processes"`
}

func defaultRegistryPath() string {
	// Keep it local and obvious for demo purposes.
	return "control-panel.processes.json"
}

func loadRegistry(path string) (processRegistry, error) {
	reg := processRegistry{Version: 1, Processes: map[string]processInfo{}}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return reg, nil
		}
		return reg, err
	}
	if len(data) == 0 {
		return reg, nil
	}
	if err := json.Unmarshal(data, &reg); err != nil {
		return processRegistry{Version: 1, Processes: map[string]processInfo{}}, err
	}
	if reg.Processes == nil {
		reg.Processes = map[string]processInfo{}
	}
	if reg.Version == 0 {
		reg.Version = 1
	}
	return reg, nil
}

func saveRegistry(path string, reg processRegistry) error {
	if reg.Version == 0 {
		reg.Version = 1
	}
	if reg.Processes == nil {
		reg.Processes = map[string]processInfo{}
	}
	data, err := json.MarshalIndent(reg, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		// if path has no dir component (relative file), Dir() returns "." and MkdirAll is fine
		return err
	}
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	// Windows can't replace existing file with Rename reliably; remove first.
	_ = os.Remove(path)
	return os.Rename(tmp, path)
}

func registerProcess(registryPath, nodeID, address string, pid int) {
	reg, err := loadRegistry(registryPath)
	if err != nil {
		log.Printf("[CP] warning: failed reading registry %s: %v", registryPath, err)
		return
	}
	reg.Processes[nodeID] = processInfo{PID: pid, Address: address, StartedAt: time.Now()}
	if err := saveRegistry(registryPath, reg); err != nil {
		log.Printf("[CP] warning: failed writing registry %s: %v", registryPath, err)
	}
}

func unregisterProcess(registryPath, nodeID string) {
	reg, err := loadRegistry(registryPath)
	if err != nil {
		return
	}
	delete(reg.Processes, nodeID)
	_ = saveRegistry(registryPath, reg)
}

func killRegisteredProcess(registryPath, nodeID string) (killed bool, msg string) {
	reg, err := loadRegistry(registryPath)
	if err != nil {
		return false, fmt.Sprintf("failed to read registry: %v", err)
	}
	info, ok := reg.Processes[nodeID]
	if !ok {
		return false, "no local process registered for this node id"
	}
	proc, err := os.FindProcess(info.PID)
	if err != nil {
		unregisterProcess(registryPath, nodeID)
		return false, fmt.Sprintf("failed to find process PID=%d (removed from registry)", info.PID)
	}
	if err := proc.Kill(); err != nil {
		// On Windows, Kill may fail if already exited; treat that as non-fatal and cleanup registry.
		unregisterProcess(registryPath, nodeID)
		return false, fmt.Sprintf("failed to kill PID=%d (removed from registry anyway): %v", info.PID, err)
	}
	unregisterProcess(registryPath, nodeID)
	return true, fmt.Sprintf("killed PID=%d", info.PID)
}

func killAllRegisteredProcesses(registryPath string) {
	reg, err := loadRegistry(registryPath)
	if err != nil {
		log.Printf("[CP] warning: failed to read registry for cleanup: %v", err)
		return
	}
	if len(reg.Processes) == 0 {
		_ = os.Remove(registryPath)
		return
	}

	for nodeID, info := range reg.Processes {
		proc, err := os.FindProcess(info.PID)
		if err != nil {
			continue
		}
		if err := proc.Kill(); err != nil {
			// Best-effort: process may have already exited.
			continue
		}
		log.Printf("[CP] cleaned up node %s (PID=%d)", nodeID, info.PID)
	}

	// Remove registry file at the end so next run starts clean.
	_ = os.Remove(registryPath)
}

func parseListenAddress(input string) (host string, port int, normalized string, err error) {
	if input == "" {
		return "", 0, "", fmt.Errorf("empty address")
	}

	// Allow passing just a port for convenience.
	if !strings.Contains(input, ":") {
		p, perr := strconv.Atoi(input)
		if perr != nil {
			return "", 0, "", fmt.Errorf("invalid port/address %q", input)
		}
		return "localhost", p, fmt.Sprintf("localhost:%d", p), nil
	}

	h, pStr, splitErr := net.SplitHostPort(input)
	if splitErr != nil {
		return "", 0, "", fmt.Errorf("invalid address %q (expected host:port)", input)
	}
	p, perr := strconv.Atoi(pStr)
	if perr != nil {
		return "", 0, "", fmt.Errorf("invalid port in %q", input)
	}
	if h == "" {
		h = "localhost"
	}
	return h, p, fmt.Sprintf("%s:%d", h, p), nil
}

func printChain(label string, head *pb.NodeInfo, tail *pb.NodeInfo, chain []*pb.NodeInfo) {
	fmt.Printf("%s\n", label)
	fmt.Printf("  head: %s (%s)\n", head.GetNodeId(), head.GetAddress())
	fmt.Printf("  tail: %s (%s)\n", tail.GetNodeId(), tail.GetAddress())
	fmt.Printf("  chain (%d):\n", len(chain))
	for i, n := range chain {
		fmt.Printf("    %d) %s (%s)\n", i+1, n.GetNodeId(), n.GetAddress())
	}
}

func addServerToLocalControlPlane(cp *controlPlaneServer, nodeID, nodeAddr, controlAddr, serverBin, secret, registryPath string, launch bool, startWait time.Duration) int {
	host, port, normalizedAddr, err := parseListenAddress(nodeAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return 2
	}
	// The current server implementation binds to localhost:<port>.
	if host != "localhost" && host != "127.0.0.1" {
		fmt.Fprintf(os.Stderr, "for this demo, addr host must be localhost/127.0.0.1 (got %q)\n", host)
		return 2
	}

	var startedCmd *exec.Cmd
	if launch {
		cmd := exec.Command(
			serverBin,
			"-n", nodeID,
			"-a", fmt.Sprintf("%d", port),
			"-c", controlAddr,
			"-secret", secret,
		)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			fmt.Fprintf(os.Stderr, "failed to start server process: %v\n", err)
			return 1
		}
		startedCmd = cmd
		registerProcess(registryPath, nodeID, normalizedAddr, cmd.Process.Pid)
		fmt.Printf("Started server process PID=%d for %s on %s\n", cmd.Process.Pid, nodeID, normalizedAddr)
		time.Sleep(startWait)
	}

	resp, err := cp.Join(context.Background(), &pb.JoinRequest{NodeId: nodeID, Address: normalizedAddr})
	if err != nil {
		if startedCmd != nil && startedCmd.Process != nil {
			_ = startedCmd.Process.Kill()
			unregisterProcess(registryPath, nodeID)
		}
		fmt.Fprintf(os.Stderr, "Join failed: %v\n", err)
		return 1
	}
	printChain("Added server to chain.", resp.GetHead(), resp.GetTail(), resp.GetChain())
	return 0
}

func dropServerFromLocalControlPlane(cp *controlPlaneServer, nodeID, registryPath string, killLocal bool) int {
	if killLocal {
		_, msg := killRegisteredProcess(registryPath, nodeID)
		fmt.Printf("Local process: %s\n", msg)
	}
	resp, err := cp.Leave(context.Background(), &pb.LeaveRequest{NodeId: nodeID})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Leave failed: %v\n", err)
		return 1
	}
	printChain("Dropped server from chain.", resp.GetHead(), resp.GetTail(), resp.GetChain())
	time.Sleep(500 * time.Millisecond)
	return 0
}

func printReplHelp() {
	fmt.Println("Commands:")
	fmt.Println("  help                         Show this help")
	fmt.Println("  state                        Print current chain state")
	fmt.Println("  add <nodeId> <port|host:port> [--no-launch]")
	fmt.Println("  drop <nodeId>                Drop from chain + kill local process")
	fmt.Println("  drop <nodeId> --no-kill      Drop from chain without killing")
	fmt.Println("  exit | quit                  Stop control panel")
}

func runRepl(cp *controlPlaneServer, controlAddr, serverBin, secret, registryPath string, startWait time.Duration, stop func()) {
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("Control panel REPL ready. Type 'help' for commands.")
	for {
		fmt.Print("cp> ")
		if !scanner.Scan() {
			// stdin closed (or error) -> exit gracefully
			stop()
			return
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		cmd := strings.ToLower(fields[0])
		switch cmd {
		case "help", "?":
			printReplHelp()
		case "state":
			resp, err := cp.GetClusterState(context.Background(), &emptypb.Empty{})
			if err != nil {
				fmt.Fprintf(os.Stderr, "GetClusterState failed: %v\n", err)
				continue
			}
			printChain("Cluster state:", resp.GetHead(), resp.GetTail(), resp.GetChain())
		case "add":
			if len(fields) < 3 {
				fmt.Fprintln(os.Stderr, "usage: add <nodeId> <port|host:port> [--no-launch]")
				continue
			}
			nodeID := fields[1]
			nodeAddr := fields[2]
			launch := true
			for _, f := range fields[3:] {
				if f == "--no-launch" {
					launch = false
				}
			}
			_ = addServerToLocalControlPlane(cp, nodeID, nodeAddr, controlAddr, serverBin, secret, registryPath, launch, startWait)
		case "drop", "remove":
			if len(fields) < 2 {
				fmt.Fprintln(os.Stderr, "usage: drop <nodeId> [--no-kill]")
				continue
			}
			nodeID := fields[1]
			killLocal := true
			for _, f := range fields[2:] {
				if f == "--no-kill" {
					killLocal = false
				}
			}
			_ = dropServerFromLocalControlPlane(cp, nodeID, registryPath, killLocal)
		case "exit", "quit":
			stop()
			return
		default:
			fmt.Fprintf(os.Stderr, "unknown command: %s (type 'help')\n", fields[0])
		}
	}
}

type nodeStatus struct {
	info          *pb.NodeInfo
	lastHeartbeat time.Time
	lastApplied   int64
	alive         bool
	subCount      int64
}

type controlPlaneServer struct {
	pb.UnimplementedControlPlaneServer

	lock sync.Mutex

	// ordered chain head->tail
	chain []*pb.NodeInfo
	nodes map[string]*nodeStatus

	heartbeatTimeout time.Duration
}

func newControlPlane(timeout time.Duration) *controlPlaneServer {
	return &controlPlaneServer{
		chain:            make([]*pb.NodeInfo, 0),
		nodes:            make(map[string]*nodeStatus),
		heartbeatTimeout: timeout,
	}
}

func cloneNodeInfo(n *pb.NodeInfo) *pb.NodeInfo {
	if n == nil {
		return nil
	}
	return &pb.NodeInfo{NodeId: n.GetNodeId(), Address: n.GetAddress()}
}

func (c *controlPlaneServer) GetClusterState(ctx context.Context, _ *emptypb.Empty) (*pb.GetClusterStateResponse, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	head, tail := c.getHeadTailLocked()
	chain := make([]*pb.NodeInfo, 0, len(c.chain))
	for _, n := range c.chain {
		chain = append(chain, cloneNodeInfo(n))
	}
	return &pb.GetClusterStateResponse{
		Head:  head,
		Tail:  tail,
		Chain: chain,
	}, nil
}

func (c *controlPlaneServer) ConfigureChain(ctx context.Context, req *pb.ConfigureChainRequest) (*emptypb.Empty, error) {
	c.lock.Lock()
	c.chain = append([]*pb.NodeInfo(nil), req.GetNodes()...)
	// also ensure nodes map contains these nodes
	for _, n := range c.chain {
		if _, ok := c.nodes[n.GetNodeId()]; !ok {
			c.nodes[n.GetNodeId()] = &nodeStatus{
				info:          &pb.NodeInfo{NodeId: n.GetNodeId(), Address: n.GetAddress()},
				lastHeartbeat: time.Time{},
				lastApplied:   0,
				alive:         false,
			}
		}
	}
	c.lock.Unlock()

	go c.pushChainToAll()
	return &emptypb.Empty{}, nil
}

func (c *controlPlaneServer) GetLeastLoadedNode(ctx context.Context, _ *emptypb.Empty) (*pb.NodeInfo, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	var best *pb.NodeInfo
	var bestCount int64 = -1

	// izbiraj samo po chain vrstnem redu in samo alive node-e
	for _, n := range c.chain {
		st, ok := c.nodes[n.GetNodeId()]
		if !ok || !st.alive {
			continue
		}
		if best == nil || st.subCount < bestCount {
			best = st.info
			bestCount = st.subCount
		}
	}

	if best == nil {
		return &pb.NodeInfo{NodeId: "none", Address: ""}, nil
	}
	// vrni kopijo, da ne deliš pointerja
	return &pb.NodeInfo{NodeId: best.GetNodeId(), Address: best.GetAddress()}, nil
}

func (c *controlPlaneServer) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	var changed bool

	c.lock.Lock()
	st, ok := c.nodes[req.GetNodeId()]
	if !ok {
		// auto-join on first heartbeat if not joined
		info := &pb.NodeInfo{NodeId: req.GetNodeId(), Address: req.GetAddress()}
		st = &nodeStatus{
			info:          info,
			lastHeartbeat: time.Now(),
			lastApplied:   req.GetLastAppliedSeq(),
			alive:         true,
			subCount:      0,
		}
		st.subCount = req.GetSubCount()
		c.nodes[req.GetNodeId()] = st

		// join at tail, but avoid duplicates
		changed = c.appendToChainIfMissing(info)
	} else {
		// normal heartbeat update
		st.info.Address = req.GetAddress()
		st.lastHeartbeat = time.Now()
		st.lastApplied = req.GetLastAppliedSeq()
		st.subCount = req.GetSubCount()
		if !st.alive {
			st.alive = true

			// node was previously removed from chain by failure monitor -> re-add at tail if missing
			if c.appendToChainIfMissing(st.info) {
				changed = true
			} else {
				// even if already present, topology might have changed recently;
				// not strictly required, but safer to re-push when a node revives
				changed = true
			}
		} else {
			st.alive = true
		}
	}
	c.lock.Unlock()

	if changed {
		go c.pushChainToAll()
	}
	return &pb.HeartbeatResponse{Ok: true}, nil
}

func (c *controlPlaneServer) Join(ctx context.Context, req *pb.JoinRequest) (*pb.JoinResponse, error) {
	var changed bool

	c.lock.Lock()
	st, ok := c.nodes[req.GetNodeId()]
	if !ok {
		info := &pb.NodeInfo{NodeId: req.GetNodeId(), Address: req.GetAddress()}
		c.nodes[req.GetNodeId()] = &nodeStatus{
			info:          info,
			lastHeartbeat: time.Now(),
			lastApplied:   0,
			alive:         true,
		}
		changed = c.appendToChainIfMissing(info)
		st = c.nodes[req.GetNodeId()]
	} else {
		// already known (maybe via heartbeat auto-join) -> just update
		st.info.Address = req.GetAddress()
		st.lastHeartbeat = time.Now()
		if !st.alive {
			st.alive = true
		}
		// ensure it's in chain (avoid duplicates)
		changed = c.appendToChainIfMissing(st.info)
	}

	head, tail := c.getHeadTailLocked()
	chain := make([]*pb.NodeInfo, 0, len(c.chain))
	for _, n := range c.chain {
		chain = append(chain, cloneNodeInfo(n))
	}
	resp := &pb.JoinResponse{Head: head, Tail: tail, Chain: chain}
	c.lock.Unlock()

	if changed {
		go c.pushChainToAll()
	}
	return resp, nil
}

func (c *controlPlaneServer) Leave(ctx context.Context, req *pb.LeaveRequest) (*pb.LeaveResponse, error) {
	c.lock.Lock()
	_, ok := c.nodes[req.GetNodeId()]
	if ok {
		delete(c.nodes, req.GetNodeId())
		c.chain = filterChain(c.chain, req.GetNodeId())
	}
	head, tail := c.getHeadTailLocked()
	chain := make([]*pb.NodeInfo, 0, len(c.chain))
	for _, n := range c.chain {
		chain = append(chain, cloneNodeInfo(n))
	}
	c.lock.Unlock()

	go c.pushChainToAll()
	return &pb.LeaveResponse{Head: head, Tail: tail, Chain: chain}, nil
}

func (c *controlPlaneServer) getHeadTailLocked() (*pb.NodeInfo, *pb.NodeInfo) {
	if len(c.chain) == 0 {
		return &pb.NodeInfo{NodeId: "none", Address: ""}, &pb.NodeInfo{NodeId: "none", Address: ""}
	}
	return cloneNodeInfo(c.chain[0]), cloneNodeInfo(c.chain[len(c.chain)-1])
}

func filterChain(chain []*pb.NodeInfo, nodeID string) []*pb.NodeInfo {
	out := make([]*pb.NodeInfo, 0, len(chain))
	for _, n := range chain {
		if n.GetNodeId() != nodeID {
			out = append(out, n)
		}
	}
	return out
}

func (c *controlPlaneServer) inChain(nodeID string) bool {
	for _, n := range c.chain {
		if n.GetNodeId() == nodeID {
			return true
		}
	}
	return false
}

func (c *controlPlaneServer) appendToChainIfMissing(info *pb.NodeInfo) bool {
	if c.inChain(info.GetNodeId()) {
		return false
	}
	c.chain = append(c.chain, info)
	return true
}

func (c *controlPlaneServer) monitorFailuresLoop(ctx context.Context) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		c.lock.Lock()
		now := time.Now()

		changed := false
		for id, st := range c.nodes {
			if st.alive && now.Sub(st.lastHeartbeat) > c.heartbeatTimeout {
				log.Printf("[CP] node %s considered dead (last heartbeat %v ago)", id, now.Sub(st.lastHeartbeat))
				st.alive = false
				changed = true
			}
		}

		if changed {
			// rebuild chain from alive nodes in existing order
			newChain := make([]*pb.NodeInfo, 0, len(c.chain))
			for _, n := range c.chain {
				st, ok := c.nodes[n.GetNodeId()]
				if ok && st.alive {
					newChain = append(newChain, st.info)
				}
			}
			c.chain = newChain
		}
		c.lock.Unlock()

		if changed {
			go c.pushChainToAll()
		}
	}
}

func (c *controlPlaneServer) pushChainToAll() {
	c.lock.Lock()
	// Build snapshots under lock so we don't share mutable *pb.NodeInfo across goroutines.
	nodes := make([]*pb.NodeInfo, 0, len(c.chain))
	for _, n := range c.chain {
		nodes = append(nodes, cloneNodeInfo(n))
	}
	aliveTargets := make([]*pb.NodeInfo, 0, len(c.chain))
	for _, n := range c.chain {
		if st, ok := c.nodes[n.GetNodeId()]; ok && st.alive {
			aliveTargets = append(aliveTargets, cloneNodeInfo(st.info))
		}
	}
	c.lock.Unlock()

	for _, n := range aliveTargets {
		c.pushChainToNode(n.GetAddress(), nodes)
	}
}

func (c *controlPlaneServer) pushChainToNode(addr string, nodes []*pb.NodeInfo) {
	if addr == "" {
		return
	}
	// Bounded retries: helps with transient dial/startup races without needing periodic pushes.
	for attempt := 1; attempt <= 3; attempt++ {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			time.Sleep(time.Duration(attempt) * 200 * time.Millisecond)
			continue
		}
		client := pb.NewMessageBoardClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err = client.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})
		cancel()
		_ = conn.Close()
		if err == nil {
			return
		}
		log.Printf("Napaka v konfiguraciji strežnika %s (attempt %d/3): %v", addr, attempt, err)
		time.Sleep(time.Duration(attempt) * 200 * time.Millisecond)
	}
}

////////////////////////////////////////////////////////////////////////////////
// Optional launcher: start data-plane nodes as OS processes
////////////////////////////////////////////////////////////////////////////////

func startServers(numServers int, headPort int, controlAddr string, secret string, serverBinary string, registryPath string) []*ServerProcess {
	servers := make([]*ServerProcess, 0, numServers)
	for i := 0; i < numServers; i++ {
		nodeID := fmt.Sprintf("node-%d", i+1)
		port := headPort + i
		address := fmt.Sprintf("localhost:%d", port)

		log.Printf("Starting %s on %s ...", nodeID, address)

		cmd := exec.Command(
			serverBinary,
			"-n", nodeID,
			"-a", fmt.Sprintf("%d", port),
			"-c", controlAddr,
			"-secret", secret,
		)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		// Keep it portable: do not use Windows-only flags on non-Windows.
		// If you really need process-group semantics on Windows, use a separate build tag file.
		if runtime.GOOS == "windows" {
			// best-effort: still avoid referencing windows-only constants here
			// (no-op)
		}

		if err := cmd.Start(); err != nil {
			log.Printf("Failed to start %s: %v", nodeID, err)
			continue
		}
		if cmd.Process != nil {
			registerProcess(registryPath, nodeID, address, cmd.Process.Pid)
		}

		servers = append(servers, &ServerProcess{
			NodeID:  nodeID,
			Address: address,
			Port:    port,
			Cmd:     cmd,
		})
		time.Sleep(500 * time.Millisecond) // small delay
		log.Printf("Started %s (PID=%d)", nodeID, cmd.Process.Pid)
	}
	return servers
}

func main() {
	numServers := flag.Int("n", 3, "number of servers in the chain")
	headPort := flag.Int("p", 50051, "head server port")
	controlPort := flag.Int("cp", 60000, "control plane port")
	timeoutMs := flag.Int("to", 2000, "heartbeat timeout (ms)")
	secret := flag.String("secret", "dev-secret-change-me", "token signing secret (must match servers)")

	// launcher options
	launch := flag.Bool("launch", false, "also launch data-plane servers as OS processes")
	serverBin := flag.String("serverbin", "../server/server.exe", "path to server binary (used only with -launch)")
	registryPath := flag.String("registry", defaultRegistryPath(), "path to local process registry (for drop-server kill)")
	startWait := flag.Duration("start-wait", 600*time.Millisecond, "delay after starting a server before Join (REPL add)")

	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage:")
		fmt.Fprintln(os.Stderr, "  control-panel [flags]")
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "Starts the control plane and opens an interactive REPL on stdin.")
		fmt.Fprintln(os.Stderr, "REPL commands: help, state, add <nodeId> <port|host:port>, drop <nodeId>, exit")
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "Flags:")
		flag.PrintDefaults()
	}

	flag.Parse()

	controlAddr := fmt.Sprintf("localhost:%d", *controlPort)
	log.Printf("Control Plane listening on %s (timeout=%dms)", controlAddr, *timeoutMs)

	lis, err := net.Listen("tcp", controlAddr)
	if err != nil {
		log.Fatal(err)
	}

	cp := newControlPlane(time.Duration(*timeoutMs) * time.Millisecond)
	go cp.monitorFailuresLoop(context.Background())

	grpcServer := grpc.NewServer()
	pb.RegisterControlPlaneServer(grpcServer, cp)

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatal(err)
		}
	}()

	stopChan := make(chan struct{})
	stopOnce := sync.Once{}
	stop := func() {
		stopOnce.Do(func() { close(stopChan) })
	}

	var servers []*ServerProcess
	if *launch {
		servers = startServers(*numServers, *headPort, controlAddr, *secret, *serverBin, *registryPath)
		if len(servers) == 0 {
			log.Fatal("No servers started")
		}
		log.Println("Servers started. Press Ctrl+C to stop...")
	} else {
		log.Println("Control plane running. (Use -launch to start data-plane nodes automatically.)")
	}

	// REPL runs in the same terminal (stdin). It can stop the server via stop().
	go runRepl(cp, controlAddr, *serverBin, *secret, *registryPath, *startWait, stop)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	select {
	case <-sigChan:
	case <-stopChan:
	}

	log.Println("Shutting down control plane...")
	// Clean up any nodes started via REPL or -launch.
	killAllRegisteredProcesses(*registryPath)
	grpcServer.GracefulStop()

	if *launch {
		log.Println("Shutting down servers...")
		for _, s := range servers {
			if s.Cmd != nil && s.Cmd.Process != nil {
				_ = s.Cmd.Process.Kill()
			}
		}
	}
}
