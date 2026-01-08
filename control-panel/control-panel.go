package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
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

func (c *controlPlaneServer) GetClusterState(ctx context.Context, _ *emptypb.Empty) (*pb.GetClusterStateResponse, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	head, tail := c.getHeadTailLocked()
	return &pb.GetClusterStateResponse{
		Head:  head,
		Tail:  tail,
		Chain: append([]*pb.NodeInfo(nil), c.chain...),
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
	resp := &pb.JoinResponse{Head: head, Tail: tail, Chain: append([]*pb.NodeInfo(nil), c.chain...)}
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
	c.lock.Unlock()

	go c.pushChainToAll()
	return &pb.LeaveResponse{Head: head, Tail: tail, Chain: append([]*pb.NodeInfo(nil), c.chain...)}, nil
}

func (c *controlPlaneServer) getHeadTailLocked() (*pb.NodeInfo, *pb.NodeInfo) {
	if len(c.chain) == 0 {
		return &pb.NodeInfo{NodeId: "none", Address: ""}, &pb.NodeInfo{NodeId: "none", Address: ""}
	}
	return c.chain[0], c.chain[len(c.chain)-1]
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

func (c *controlPlaneServer) monitorFailuresLoop() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
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
	nodes := append([]*pb.NodeInfo(nil), c.chain...)
	aliveTargets := make([]*pb.NodeInfo, 0, len(nodes))
	for _, n := range nodes {
		if st, ok := c.nodes[n.GetNodeId()]; ok && st.alive {
			aliveTargets = append(aliveTargets, st.info)
		}
	}
	c.lock.Unlock()

	req := &pb.ConfigureChainRequest{Nodes: nodes}
	for _, n := range aliveTargets {
		conn, err := grpc.NewClient(n.GetAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			continue
		}
		client := pb.NewMessageBoardClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err = client.ConfigureChain(ctx, req)
		if err != nil {
			log.Printf("Napaka v konfiguraciji strežnika %s: %v", n.GetAddress(), err)
		}
		cancel()
		_ = conn.Close()
	}
}

////////////////////////////////////////////////////////////////////////////////
// Optional launcher: start data-plane nodes as OS processes
////////////////////////////////////////////////////////////////////////////////

func startServers(numServers int, headPort int, controlAddr string, secret string, serverBinary string) []*ServerProcess {
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

		servers = append(servers, &ServerProcess{
			NodeID:  nodeID,
			Address: address,
			Port:    port,
			Cmd:     cmd,
		})
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
	serverBin := flag.String("serverbin", "./server", "path to server binary (used only with -launch)")

	flag.Parse()

	controlAddr := fmt.Sprintf("localhost:%d", *controlPort)
	log.Printf("Control Plane listening on %s (timeout=%dms)", controlAddr, *timeoutMs)

	lis, err := net.Listen("tcp", controlAddr)
	if err != nil {
		log.Fatal(err)
	}

	cp := newControlPlane(time.Duration(*timeoutMs) * time.Millisecond)
	go cp.monitorFailuresLoop()

	grpcServer := grpc.NewServer()
	pb.RegisterControlPlaneServer(grpcServer, cp)

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatal(err)
		}
	}()

	var servers []*ServerProcess
	if *launch {
		servers = startServers(*numServers, *headPort, controlAddr, *secret, *serverBin)
		if len(servers) == 0 {
			log.Fatal("No servers started")
		}
		log.Println("Servers started. Press Ctrl+C to stop...")
	} else {
		log.Println("Control plane running. (Use -launch to start data-plane nodes automatically.)")
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down control plane...")

	if *launch {
		log.Println("Shutting down servers...")
		for _, s := range servers {
			if s.Cmd != nil && s.Cmd.Process != nil {
				_ = s.Cmd.Process.Kill()
			}
		}
	}
}
