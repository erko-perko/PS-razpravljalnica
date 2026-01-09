package main

import (
	"context"
	"testing"
	"time"

	pb "razpravljalnica/pb"

	"google.golang.org/protobuf/types/known/emptypb"
)

func eventually(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("condition not met within %v", timeout)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// NOTE: These are unit tests for the control plane's state management logic.
// The control plane does NOT start server processes - it only tracks metadata
// about servers that register themselves via Join/Heartbeat.
// Real servers must be started externally (manually, via -launch flag, or by orchestrator).

// Test newControlPlane creates a control plane with correct initial state
func TestNewControlPlane(t *testing.T) {
	timeout := 2 * time.Second
	cp := newControlPlane(timeout)

	if cp == nil {
		t.Fatal("newControlPlane returned nil")
	}
	if cp.heartbeatTimeout != timeout {
		t.Errorf("expected timeout %v, got %v", timeout, cp.heartbeatTimeout)
	}
	if len(cp.chain) != 0 {
		t.Errorf("expected empty chain, got %d nodes", len(cp.chain))
	}
	if len(cp.nodes) != 0 {
		t.Errorf("expected empty nodes map, got %d nodes", len(cp.nodes))
	}
}

// Test GetClusterState with empty cluster
func TestGetClusterStateEmpty(t *testing.T) {
	cp := newControlPlane(2 * time.Second)
	ctx := context.Background()

	resp, err := cp.GetClusterState(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("GetClusterState failed: %v", err)
	}

	if resp.Head.GetNodeId() != "none" {
		t.Errorf("expected head to be 'none', got %s", resp.Head.GetNodeId())
	}
	if resp.Tail.GetNodeId() != "none" {
		t.Errorf("expected tail to be 'none', got %s", resp.Tail.GetNodeId())
	}
	if len(resp.Chain) != 0 {
		t.Errorf("expected empty chain, got %d nodes", len(resp.Chain))
	}
}

// Test GetClusterState with configured nodes (metadata only - servers not actually running)
func TestGetClusterStateWithNodes(t *testing.T) {
	cp := newControlPlane(2 * time.Second)
	ctx := context.Background()

	nodes := []*pb.NodeInfo{
		{NodeId: "node-1", Address: "localhost:50051"},
		{NodeId: "node-2", Address: "localhost:50052"},
		{NodeId: "node-3", Address: "localhost:50053"},
	}

	_, err := cp.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})
	if err != nil {
		t.Fatalf("ConfigureChain failed: %v", err)
	}

	// Give it a moment for goroutine to complete
	time.Sleep(50 * time.Millisecond)

	resp, err := cp.GetClusterState(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("GetClusterState failed: %v", err)
	}

	if resp.Head.GetNodeId() != "node-1" {
		t.Errorf("expected head to be 'node-1', got %s", resp.Head.GetNodeId())
	}
	if resp.Tail.GetNodeId() != "node-3" {
		t.Errorf("expected tail to be 'node-3', got %s", resp.Tail.GetNodeId())
	}
	if len(resp.Chain) != 3 {
		t.Errorf("expected chain length 3, got %d", len(resp.Chain))
	}
}

// Test ConfigureChain - only updates chain metadata, does NOT start servers
func TestConfigureChain(t *testing.T) {
	cp := newControlPlane(2 * time.Second)
	ctx := context.Background()

	nodes := []*pb.NodeInfo{
		{NodeId: "node-1", Address: "localhost:50051"},
		{NodeId: "node-2", Address: "localhost:50052"},
	}

	_, err := cp.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})
	if err != nil {
		t.Fatalf("ConfigureChain failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if len(cp.chain) != 2 {
		t.Errorf("expected chain length 2, got %d", len(cp.chain))
	}
	if len(cp.nodes) != 2 {
		t.Errorf("expected nodes map size 2, got %d", len(cp.nodes))
	}
	// Note: nodes are marked as alive=false since no heartbeats received
	for id, st := range cp.nodes {
		if st.alive {
			t.Errorf("node %s should be marked dead (no heartbeat yet)", id)
		}
	}
}

// Test ConfigureChain can be called multiple times
func TestConfigureChainMultipleTimes(t *testing.T) {
	cp := newControlPlane(2 * time.Second)
	ctx := context.Background()

	// First configuration
	nodes1 := []*pb.NodeInfo{
		{NodeId: "node-1", Address: "localhost:50051"},
	}
	_, err := cp.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes1})
	if err != nil {
		t.Fatalf("First ConfigureChain failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// Second configuration (replaces)
	nodes2 := []*pb.NodeInfo{
		{NodeId: "node-2", Address: "localhost:50052"},
		{NodeId: "node-3", Address: "localhost:50053"},
	}
	_, err = cp.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes2})
	if err != nil {
		t.Fatalf("Second ConfigureChain failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if len(cp.chain) != 2 {
		t.Errorf("expected chain length 2, got %d", len(cp.chain))
	}
	if cp.chain[0].GetNodeId() != "node-2" {
		t.Errorf("expected first node to be 'node-2', got %s", cp.chain[0].GetNodeId())
	}
}

// Test Join functionality - simulates a running server registering itself with control plane
func TestJoin(t *testing.T) {
	cp := newControlPlane(2 * time.Second)
	ctx := context.Background()

	// Simulate a server that's already running calling Join to register
	req := &pb.JoinRequest{
		NodeId:  "node-1",
		Address: "localhost:50051",
	}

	resp, err := cp.Join(ctx, req)
	if err != nil {
		t.Fatalf("Join failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if len(cp.chain) != 1 {
		t.Errorf("expected chain length 1, got %d", len(cp.chain))
	}
	if len(cp.nodes) != 1 {
		t.Errorf("expected nodes map size 1, got %d", len(cp.nodes))
	}

	if resp.Chain[0].GetNodeId() != "node-1" {
		t.Errorf("expected node-1 in chain, got %s", resp.Chain[0].GetNodeId())
	}
}

// Test Join multiple nodes
func TestJoinMultipleNodes(t *testing.T) {
	cp := newControlPlane(2 * time.Second)
	ctx := context.Background()

	// Join first node
	_, err := cp.Join(ctx, &pb.JoinRequest{NodeId: "node-1", Address: "localhost:50051"})
	if err != nil {
		t.Fatalf("First Join failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// Join second node
	_, err = cp.Join(ctx, &pb.JoinRequest{NodeId: "node-2", Address: "localhost:50052"})
	if err != nil {
		t.Fatalf("Second Join failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if len(cp.chain) != 2 {
		t.Errorf("expected chain length 2, got %d", len(cp.chain))
	}
	if cp.chain[0].GetNodeId() != "node-1" {
		t.Errorf("expected first node to be 'node-1', got %s", cp.chain[0].GetNodeId())
	}
	if cp.chain[1].GetNodeId() != "node-2" {
		t.Errorf("expected second node to be 'node-2', got %s", cp.chain[1].GetNodeId())
	}
}

// Test Join prevents duplicate nodes
func TestJoinDuplicateNode(t *testing.T) {
	cp := newControlPlane(2 * time.Second)
	ctx := context.Background()

	// Join first time
	_, err := cp.Join(ctx, &pb.JoinRequest{NodeId: "node-1", Address: "localhost:50051"})
	if err != nil {
		t.Fatalf("First Join failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// Join same node again
	_, err = cp.Join(ctx, &pb.JoinRequest{NodeId: "node-1", Address: "localhost:50051"})
	if err != nil {
		t.Fatalf("Second Join failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if len(cp.chain) != 1 {
		t.Errorf("expected chain length 1 (no duplicates), got %d", len(cp.chain))
	}
}

// Test Leave functionality
func TestLeave(t *testing.T) {
	cp := newControlPlane(2 * time.Second)
	ctx := context.Background()

	// Join a node first
	_, err := cp.Join(ctx, &pb.JoinRequest{NodeId: "node-1", Address: "localhost:50051"})
	if err != nil {
		t.Fatalf("Join failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// Leave
	resp, err := cp.Leave(ctx, &pb.LeaveRequest{NodeId: "node-1"})
	if err != nil {
		t.Fatalf("Leave failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if len(cp.chain) != 0 {
		t.Errorf("expected chain length 0, got %d", len(cp.chain))
	}
	if len(cp.nodes) != 0 {
		t.Errorf("expected nodes map size 0, got %d", len(cp.nodes))
	}
	if len(resp.Chain) != 0 {
		t.Errorf("expected empty chain in response, got %d", len(resp.Chain))
	}
}

// Test Leave non-existent node (should not error)
func TestLeaveNonExistentNode(t *testing.T) {
	cp := newControlPlane(2 * time.Second)
	ctx := context.Background()

	_, err := cp.Leave(ctx, &pb.LeaveRequest{NodeId: "node-999"})
	if err != nil {
		t.Fatalf("Leave failed: %v", err)
	}

	if len(cp.chain) != 0 {
		t.Errorf("expected chain length 0, got %d", len(cp.chain))
	}
}

// Test Heartbeat creates node status - simulates a running server sending heartbeat
func TestHeartbeat(t *testing.T) {
	cp := newControlPlane(2 * time.Second)
	ctx := context.Background()

	// Simulate a running server sending its first heartbeat (auto-join)
	req := &pb.HeartbeatRequest{
		NodeId:         "node-1",
		Address:        "localhost:50051",
		LastAppliedSeq: 10,
		SubCount:       5,
	}

	resp, err := cp.Heartbeat(ctx, req)
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}

	if !resp.Ok {
		t.Error("expected Ok=true")
	}

	time.Sleep(50 * time.Millisecond)

	status, ok := cp.nodes["node-1"]
	if !ok {
		t.Fatal("node-1 not found in nodes map")
	}
	if !status.alive {
		t.Error("expected node to be alive")
	}
	if status.lastApplied != 10 {
		t.Errorf("expected lastApplied=10, got %d", status.lastApplied)
	}
	if status.subCount != 5 {
		t.Errorf("expected subCount=5, got %d", status.subCount)
	}
}

// Test Heartbeat updates existing node
func TestHeartbeatUpdate(t *testing.T) {
	cp := newControlPlane(2 * time.Second)
	ctx := context.Background()

	// First heartbeat
	req1 := &pb.HeartbeatRequest{
		NodeId:         "node-1",
		Address:        "localhost:50051",
		LastAppliedSeq: 10,
		SubCount:       5,
	}
	_, err := cp.Heartbeat(ctx, req1)
	if err != nil {
		t.Fatalf("First Heartbeat failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// Second heartbeat with updated values
	req2 := &pb.HeartbeatRequest{
		NodeId:         "node-1",
		Address:        "localhost:50051",
		LastAppliedSeq: 20,
		SubCount:       7,
	}
	_, err = cp.Heartbeat(ctx, req2)
	if err != nil {
		t.Fatalf("Second Heartbeat failed: %v", err)
	}

	status := cp.nodes["node-1"]
	if status.lastApplied != 20 {
		t.Errorf("expected lastApplied=20, got %d", status.lastApplied)
	}
	if status.subCount != 7 {
		t.Errorf("expected subCount=7, got %d", status.subCount)
	}
}

// Test GetLeastLoadedNode with empty cluster
func TestGetLeastLoadedNodeEmpty(t *testing.T) {
	cp := newControlPlane(2 * time.Second)
	ctx := context.Background()

	resp, err := cp.GetLeastLoadedNode(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("GetLeastLoadedNode failed: %v", err)
	}

	if resp.GetNodeId() != "none" {
		t.Errorf("expected 'none', got %s", resp.GetNodeId())
	}
}

// Test GetLeastLoadedNode with single node
func TestGetLeastLoadedNodeSingle(t *testing.T) {
	cp := newControlPlane(2 * time.Second)
	ctx := context.Background()

	// Simulate a running server sending heartbeat to register
	req := &pb.HeartbeatRequest{
		NodeId:         "node-1",
		Address:        "localhost:50051",
		LastAppliedSeq: 10,
		SubCount:       5,
	}
	_, err := cp.Heartbeat(ctx, req)
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	resp, err := cp.GetLeastLoadedNode(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("GetLeastLoadedNode failed: %v", err)
	}

	if resp.GetNodeId() != "node-1" {
		t.Errorf("expected 'node-1', got %s", resp.GetNodeId())
	}
}

// Test GetLeastLoadedNode selects node with lowest subCount
func TestGetLeastLoadedNodeMultiple(t *testing.T) {
	cp := newControlPlane(2 * time.Second)
	ctx := context.Background()

	// Add nodes with different sub counts
	nodes := []struct {
		id       string
		subCount int64
	}{
		{"node-1", 10},
		{"node-2", 3},
		{"node-3", 7},
	}

	for _, n := range nodes {
		req := &pb.HeartbeatRequest{
			NodeId:         n.id,
			Address:        "localhost:5005" + n.id[len(n.id)-1:],
			LastAppliedSeq: 0,
			SubCount:       n.subCount,
		}
		_, err := cp.Heartbeat(ctx, req)
		if err != nil {
			t.Fatalf("Heartbeat failed for %s: %v", n.id, err)
		}
	}

	time.Sleep(50 * time.Millisecond)

	resp, err := cp.GetLeastLoadedNode(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("GetLeastLoadedNode failed: %v", err)
	}

	if resp.GetNodeId() != "node-2" {
		t.Errorf("expected 'node-2' (lowest subCount), got %s", resp.GetNodeId())
	}
}

// Test GetLeastLoadedNode excludes dead nodes
func TestGetLeastLoadedNodeExcludesDead(t *testing.T) {
	cp := newControlPlane(100 * time.Millisecond)
	ctx := context.Background()

	// Add node-1 with low subCount
	req1 := &pb.HeartbeatRequest{
		NodeId:         "node-1",
		Address:        "localhost:50051",
		LastAppliedSeq: 0,
		SubCount:       1,
	}
	_, err := cp.Heartbeat(ctx, req1)
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}

	// Add node-2 with higher subCount
	req2 := &pb.HeartbeatRequest{
		NodeId:         "node-2",
		Address:        "localhost:50052",
		LastAppliedSeq: 0,
		SubCount:       10,
	}
	_, err = cp.Heartbeat(ctx, req2)
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// Start failure monitor
	monitorCtx, monitorCancel := context.WithCancel(context.Background())
	defer monitorCancel()
	go cp.monitorFailuresLoop(monitorCtx)

	// Keep node-2 alive while waiting for node-1 to be marked dead
	keepAliveCtx, keepAliveCancel := context.WithCancel(context.Background())
	defer keepAliveCancel()
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-keepAliveCtx.Done():
				return
			case <-ticker.C:
				_, _ = cp.Heartbeat(ctx, req2)
			}
		}
	}()

	eventually(t, 2*time.Second, func() bool {
		cp.lock.Lock()
		defer cp.lock.Unlock()
		st := cp.nodes["node-1"]
		return st != nil && !st.alive
	})

	resp, err := cp.GetLeastLoadedNode(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("GetLeastLoadedNode failed: %v", err)
	}

	// Should return node-2 even though it has higher subCount, because node-1 is dead
	if resp.GetNodeId() != "node-2" {
		t.Errorf("expected 'node-2' (node-1 is dead), got %s", resp.GetNodeId())
	}
}

// Test failure monitor detects dead nodes
func TestMonitorFailuresDetectsDead(t *testing.T) {
	cp := newControlPlane(200 * time.Millisecond)
	ctx := context.Background()

	// Add node
	req := &pb.HeartbeatRequest{
		NodeId:         "node-1",
		Address:        "localhost:50051",
		LastAppliedSeq: 0,
		SubCount:       5,
	}
	_, err := cp.Heartbeat(ctx, req)
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// Start failure monitor
	monitorCtx, monitorCancel := context.WithCancel(context.Background())
	defer monitorCancel()
	go cp.monitorFailuresLoop(monitorCtx)

	eventually(t, 2*time.Second, func() bool {
		cp.lock.Lock()
		defer cp.lock.Unlock()
		st := cp.nodes["node-1"]
		return st != nil && !st.alive && len(cp.chain) == 0
	})
}

// Test node revival after failure
func TestNodeRevival(t *testing.T) {
	cp := newControlPlane(200 * time.Millisecond)
	ctx := context.Background()

	// Add node
	req := &pb.HeartbeatRequest{
		NodeId:         "node-1",
		Address:        "localhost:50051",
		LastAppliedSeq: 0,
		SubCount:       5,
	}
	_, err := cp.Heartbeat(ctx, req)
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// Start failure monitor
	monitorCtx, monitorCancel := context.WithCancel(context.Background())
	defer monitorCancel()
	go cp.monitorFailuresLoop(monitorCtx)

	// Wait for timeout (node becomes dead)
	eventually(t, 2*time.Second, func() bool {
		cp.lock.Lock()
		defer cp.lock.Unlock()
		st := cp.nodes["node-1"]
		return st != nil && !st.alive
	})

	// Send heartbeat again (node revives)
	_, err = cp.Heartbeat(ctx, req)
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	eventually(t, 2*time.Second, func() bool {
		cp.lock.Lock()
		defer cp.lock.Unlock()
		st := cp.nodes["node-1"]
		return st != nil && st.alive && len(cp.chain) == 1
	})
}

// Test filterChain helper function
func TestFilterChain(t *testing.T) {
	chain := []*pb.NodeInfo{
		{NodeId: "node-1", Address: "localhost:50051"},
		{NodeId: "node-2", Address: "localhost:50052"},
		{NodeId: "node-3", Address: "localhost:50053"},
	}

	filtered := filterChain(chain, "node-2")

	if len(filtered) != 2 {
		t.Errorf("expected length 2, got %d", len(filtered))
	}
	if filtered[0].GetNodeId() != "node-1" {
		t.Errorf("expected node-1, got %s", filtered[0].GetNodeId())
	}
	if filtered[1].GetNodeId() != "node-3" {
		t.Errorf("expected node-3, got %s", filtered[1].GetNodeId())
	}
}

// Test filterChain with non-existent node
func TestFilterChainNonExistent(t *testing.T) {
	chain := []*pb.NodeInfo{
		{NodeId: "node-1", Address: "localhost:50051"},
	}

	filtered := filterChain(chain, "node-999")

	if len(filtered) != 1 {
		t.Errorf("expected length 1, got %d", len(filtered))
	}
}

// Test filterChain with empty chain
func TestFilterChainEmpty(t *testing.T) {
	chain := []*pb.NodeInfo{}

	filtered := filterChain(chain, "node-1")

	if len(filtered) != 0 {
		t.Errorf("expected length 0, got %d", len(filtered))
	}
}

// Test inChain helper function
func TestInChain(t *testing.T) {
	cp := newControlPlane(2 * time.Second)

	cp.chain = []*pb.NodeInfo{
		{NodeId: "node-1", Address: "localhost:50051"},
		{NodeId: "node-2", Address: "localhost:50052"},
	}

	if !cp.inChain("node-1") {
		t.Error("expected node-1 to be in chain")
	}
	if !cp.inChain("node-2") {
		t.Error("expected node-2 to be in chain")
	}
	if cp.inChain("node-3") {
		t.Error("expected node-3 not to be in chain")
	}
}

// Test appendToChainIfMissing
func TestAppendToChainIfMissing(t *testing.T) {
	cp := newControlPlane(2 * time.Second)

	info1 := &pb.NodeInfo{NodeId: "node-1", Address: "localhost:50051"}
	info2 := &pb.NodeInfo{NodeId: "node-2", Address: "localhost:50052"}

	// Append new node
	changed := cp.appendToChainIfMissing(info1)
	if !changed {
		t.Error("expected changed=true when adding new node")
	}
	if len(cp.chain) != 1 {
		t.Errorf("expected chain length 1, got %d", len(cp.chain))
	}

	// Append duplicate node
	changed = cp.appendToChainIfMissing(info1)
	if changed {
		t.Error("expected changed=false when adding duplicate node")
	}
	if len(cp.chain) != 1 {
		t.Errorf("expected chain length 1 (no duplicate), got %d", len(cp.chain))
	}

	// Append another new node
	changed = cp.appendToChainIfMissing(info2)
	if !changed {
		t.Error("expected changed=true when adding new node")
	}
	if len(cp.chain) != 2 {
		t.Errorf("expected chain length 2, got %d", len(cp.chain))
	}
}

// Test concurrent heartbeats
func TestConcurrentHeartbeats(t *testing.T) {
	cp := newControlPlane(2 * time.Second)
	ctx := context.Background()

	done := make(chan bool)
	numGoroutines := 10

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			req := &pb.HeartbeatRequest{
				NodeId:         "node-1",
				Address:        "localhost:50051",
				LastAppliedSeq: int64(id),
				SubCount:       int64(id),
			}
			_, err := cp.Heartbeat(ctx, req)
			if err != nil {
				t.Errorf("Heartbeat failed: %v", err)
			}
			done <- true
		}(i)
	}

	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	if len(cp.nodes) != 1 {
		t.Errorf("expected 1 node, got %d", len(cp.nodes))
	}
	if len(cp.chain) > 1 {
		t.Errorf("expected at most 1 node in chain, got %d", len(cp.chain))
	}
}

// Test concurrent joins
func TestConcurrentJoins(t *testing.T) {
	cp := newControlPlane(2 * time.Second)
	ctx := context.Background()

	done := make(chan bool)
	numNodes := 5

	for i := 1; i <= numNodes; i++ {
		go func(id int) {
			req := &pb.JoinRequest{
				NodeId:  "node-" + string(rune('0'+id)),
				Address: "localhost:5005" + string(rune('0'+id)),
			}
			_, err := cp.Join(ctx, req)
			if err != nil {
				t.Errorf("Join failed: %v", err)
			}
			done <- true
		}(i)
	}

	for i := 0; i < numNodes; i++ {
		<-done
	}

	time.Sleep(100 * time.Millisecond)

	if len(cp.nodes) != numNodes {
		t.Errorf("expected %d nodes, got %d", numNodes, len(cp.nodes))
	}
}

// Test getHeadTailLocked with single node
func TestGetHeadTailLockedSingle(t *testing.T) {
	cp := newControlPlane(2 * time.Second)

	cp.chain = []*pb.NodeInfo{
		{NodeId: "node-1", Address: "localhost:50051"},
	}

	head, tail := cp.getHeadTailLocked()

	if head.GetNodeId() != "node-1" {
		t.Errorf("expected head 'node-1', got %s", head.GetNodeId())
	}
	if tail.GetNodeId() != "node-1" {
		t.Errorf("expected tail 'node-1', got %s", tail.GetNodeId())
	}
}

// Test getHeadTailLocked with multiple nodes
func TestGetHeadTailLockedMultiple(t *testing.T) {
	cp := newControlPlane(2 * time.Second)

	cp.chain = []*pb.NodeInfo{
		{NodeId: "node-1", Address: "localhost:50051"},
		{NodeId: "node-2", Address: "localhost:50052"},
		{NodeId: "node-3", Address: "localhost:50053"},
	}

	head, tail := cp.getHeadTailLocked()

	if head.GetNodeId() != "node-1" {
		t.Errorf("expected head 'node-1', got %s", head.GetNodeId())
	}
	if tail.GetNodeId() != "node-3" {
		t.Errorf("expected tail 'node-3', got %s", tail.GetNodeId())
	}
}

// Test auto-join via heartbeat
func TestAutoJoinViaHeartbeat(t *testing.T) {
	cp := newControlPlane(2 * time.Second)
	ctx := context.Background()

	// Node sends heartbeat without joining first
	req := &pb.HeartbeatRequest{
		NodeId:         "node-1",
		Address:        "localhost:50051",
		LastAppliedSeq: 0,
		SubCount:       0,
	}

	_, err := cp.Heartbeat(ctx, req)
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// Node should be auto-joined
	if len(cp.nodes) != 1 {
		t.Errorf("expected 1 node (auto-joined), got %d", len(cp.nodes))
	}
	if len(cp.chain) != 1 {
		t.Errorf("expected 1 node in chain (auto-joined), got %d", len(cp.chain))
	}
	if !cp.nodes["node-1"].alive {
		t.Error("expected node to be alive")
	}
}

// Test Leave updates head and tail
func TestLeaveUpdatesHeadTail(t *testing.T) {
	cp := newControlPlane(2 * time.Second)
	ctx := context.Background()

	// Join multiple nodes
	nodes := []string{"node-1", "node-2", "node-3"}
	for i, nodeID := range nodes {
		req := &pb.JoinRequest{
			NodeId:  nodeID,
			Address: "localhost:5005" + string(rune('1'+i)),
		}
		_, err := cp.Join(ctx, req)
		if err != nil {
			t.Fatalf("Join failed for %s: %v", nodeID, err)
		}
	}

	time.Sleep(50 * time.Millisecond)

	// Remove head node
	resp, err := cp.Leave(ctx, &pb.LeaveRequest{NodeId: "node-1"})
	if err != nil {
		t.Fatalf("Leave failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if resp.Head.GetNodeId() != "node-2" {
		t.Errorf("expected new head 'node-2', got %s", resp.Head.GetNodeId())
	}
	if resp.Tail.GetNodeId() != "node-3" {
		t.Errorf("expected tail 'node-3', got %s", resp.Tail.GetNodeId())
	}
}

// Test ConfigureChain doesn't make non-running servers available for load balancing
func TestConfigureChainDoesNotMakeServersAvailable(t *testing.T) {
	cp := newControlPlane(2 * time.Second)
	ctx := context.Background()

	// ConfigureChain adds nodes to chain but marks them as not alive (no heartbeat)
	nodes := []*pb.NodeInfo{
		{NodeId: "node-1", Address: "localhost:50051"},
		{NodeId: "node-2", Address: "localhost:50052"},
	}

	_, err := cp.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})
	if err != nil {
		t.Fatalf("ConfigureChain failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// Nodes are in chain but not alive (no heartbeat received)
	for _, node := range cp.nodes {
		if node.alive {
			t.Error("nodes configured via ConfigureChain should not be alive without heartbeat")
		}
	}

	// GetLeastLoadedNode should return "none" because no servers are alive
	resp, err := cp.GetLeastLoadedNode(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("GetLeastLoadedNode failed: %v", err)
	}
	if resp.GetNodeId() != "none" {
		t.Errorf("expected 'none' (no alive servers), got %s", resp.GetNodeId())
	}
}

// Test ConfigureChain with empty nodes list
func TestConfigureChainEmpty(t *testing.T) {
	cp := newControlPlane(2 * time.Second)
	ctx := context.Background()

	// First add some nodes
	_, err := cp.Join(ctx, &pb.JoinRequest{NodeId: "node-1", Address: "localhost:50051"})
	if err != nil {
		t.Fatalf("Join failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// Now configure with empty list
	_, err = cp.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: []*pb.NodeInfo{}})
	if err != nil {
		t.Fatalf("ConfigureChain failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if len(cp.chain) != 0 {
		t.Errorf("expected empty chain, got %d nodes", len(cp.chain))
	}
}

// Test servers become available only after sending heartbeat
func TestServersAvailableOnlyAfterHeartbeat(t *testing.T) {
	cp := newControlPlane(2 * time.Second)
	ctx := context.Background()

	// ConfigureChain - servers in chain but not alive
	nodes := []*pb.NodeInfo{
		{NodeId: "node-1", Address: "localhost:50051"},
	}
	_, err := cp.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})
	if err != nil {
		t.Fatalf("ConfigureChain failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// Server not available for load balancing yet
	resp, err := cp.GetLeastLoadedNode(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("GetLeastLoadedNode failed: %v", err)
	}
	if resp.GetNodeId() != "none" {
		t.Errorf("expected 'none' before heartbeat, got %s", resp.GetNodeId())
	}

	// Now simulate running server sending heartbeat
	heartbeat := &pb.HeartbeatRequest{
		NodeId:         "node-1",
		Address:        "localhost:50051",
		LastAppliedSeq: 0,
		SubCount:       5,
	}
	_, err = cp.Heartbeat(ctx, heartbeat)
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// Now server should be available
	resp, err = cp.GetLeastLoadedNode(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("GetLeastLoadedNode failed: %v", err)
	}
	if resp.GetNodeId() != "node-1" {
		t.Errorf("expected 'node-1' after heartbeat, got %s", resp.GetNodeId())
	}
}

// Test GetLeastLoadedNode with all dead nodes
func TestGetLeastLoadedNodeAllDead(t *testing.T) {
	cp := newControlPlane(100 * time.Millisecond)
	ctx := context.Background()

	// Add nodes
	req := &pb.HeartbeatRequest{
		NodeId:         "node-1",
		Address:        "localhost:50051",
		LastAppliedSeq: 0,
		SubCount:       1,
	}
	_, err := cp.Heartbeat(ctx, req)
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// Start failure monitor
	monitorCtx, monitorCancel := context.WithCancel(context.Background())
	defer monitorCancel()
	go cp.monitorFailuresLoop(monitorCtx)

	eventually(t, 2*time.Second, func() bool {
		cp.lock.Lock()
		defer cp.lock.Unlock()
		st := cp.nodes["node-1"]
		return st != nil && !st.alive
	})

	resp, err := cp.GetLeastLoadedNode(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("GetLeastLoadedNode failed: %v", err)
	}

	if resp.GetNodeId() != "none" {
		t.Errorf("expected 'none' when all nodes are dead, got %s", resp.GetNodeId())
	}
}
