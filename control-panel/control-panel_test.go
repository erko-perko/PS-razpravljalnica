package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	pb "razpravljalnica/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Test: startServers creates the correct number of servers
func TestStartServers(t *testing.T) {
	servers := startServers(3, 60000)
	defer func() {
		for _, server := range servers {
			if server.Cmd != nil && server.Cmd.Process != nil {
				server.Cmd.Process.Kill()
				server.Cmd.Wait()
			}
		}
		time.Sleep(500 * time.Millisecond)
	}()

	if len(servers) != 3 {
		t.Errorf("Expected 3 servers, got %d", len(servers))
	}

	// Verify server properties
	for i, server := range servers {
		expectedPort := 60000 + i

		if server.Port != expectedPort {
			t.Errorf("Server %d: expected port %d, got %d", i, expectedPort, server.Port)
		}

		if server.Cmd == nil {
			t.Errorf("Server %d: command should not be nil", i)
		}

		if server.Cmd.Process == nil {
			t.Errorf("Server %d: process should be started", i)
		}
	}
}

// Test: configureChain sends configuration to all nodes
func TestConfigureChain(t *testing.T) {
	servers := startServers(2, 60010)
	defer func() {
		for _, server := range servers {
			if server.Cmd != nil && server.Cmd.Process != nil {
				server.Cmd.Process.Kill()
				server.Cmd.Wait()
			}
		}
		time.Sleep(500 * time.Millisecond)
	}()

	if len(servers) != 2 {
		t.Fatal("Failed to start servers")
	}

	// Wait for servers to be ready
	time.Sleep(3 * time.Second)

	nodes := []*pb.NodeInfo{
		{NodeId: "node-1", Address: "localhost:60010"},
		{NodeId: "node-2", Address: "localhost:60011"},
	}

	ctx := context.Background()
	configureChain(ctx, nodes)

	// Verify configuration by connecting to each node
	for _, node := range nodes {
		conn, err := grpc.NewClient(node.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Errorf("Failed to connect to %s: %v", node.NodeId, err)
			continue
		}

		client := pb.NewControlPlaneClient(conn)
		response, err := client.GetClusterState(ctx, &emptypb.Empty{})
		conn.Close()

		if err != nil {
			t.Errorf("Failed to get cluster state from %s: %v", node.NodeId, err)
		}

		// First node should be head, last node should be tail
		if node.NodeId == "node-1" && response.Head == nil {
			t.Error("First node should report a head")
		}
		if node.NodeId == "node-2" && response.Tail == nil {
			t.Error("Last node should report a tail")
		}
	}
}

// Test: chain with different number of servers
func TestVariableChainSize(t *testing.T) {
	testCases := []struct {
		name       string
		numServers int
		startPort  int
	}{
		{"Single Node", 1, 60020},
		{"Two Nodes", 2, 60025},
		{"Five Nodes", 5, 60030},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			servers := startServers(tc.numServers, tc.startPort)
			defer func() {
				for _, server := range servers {
					if server.Cmd != nil && server.Cmd.Process != nil {
						server.Cmd.Process.Kill()
						server.Cmd.Wait()
					}
				}
				time.Sleep(500 * time.Millisecond)
			}()

			if len(servers) != tc.numServers {
				t.Errorf("Expected %d servers, got %d", tc.numServers, len(servers))
			}

			// Verify each server is running
			for _, server := range servers {
				if server.Cmd == nil || server.Cmd.Process == nil {
					t.Error("Server should be running")
				}
			}
		})
	}
}

// Test: servers start on correct ports
func TestServerPorts(t *testing.T) {
	servers := startServers(3, 60040)
	defer func() {
		for _, server := range servers {
			if server.Cmd != nil && server.Cmd.Process != nil {
				server.Cmd.Process.Kill()
				server.Cmd.Wait()
			}
		}
		time.Sleep(500 * time.Millisecond)
	}()

	expectedPorts := []int{60040, 60041, 60042}
	for i, server := range servers {
		if server.Port != expectedPorts[i] {
			t.Errorf("Server %d: expected port %d, got %d", i, expectedPorts[i], server.Port)
		}
	}

	// Wait and verify servers are listening on their ports
	time.Sleep(2 * time.Second)

	for _, server := range servers {
		conn, err := grpc.NewClient(server.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Errorf("Failed to connect to server at %s: %v", server.Address, err)
			continue
		}
		conn.Close()
	}
}

// Integration test: Full chain setup and basic operation
func TestFullChainSetup(t *testing.T) {
	servers := startServers(3, 60050)
	defer func() {
		for _, server := range servers {
			if server.Cmd != nil && server.Cmd.Process != nil {
				server.Cmd.Process.Kill()
				server.Cmd.Wait()
			}
		}
		time.Sleep(500 * time.Millisecond)
	}()

	if len(servers) != 3 {
		t.Fatal("Failed to start servers")
	}

	// Wait for servers to be ready
	time.Sleep(3 * time.Second)

	nodes := make([]*pb.NodeInfo, len(servers))
	for i, server := range servers {
		nodes[i] = &pb.NodeInfo{
			NodeId:  server.NodeID,
			Address: server.Address,
		}
	}

	ctx := context.Background()
	configureChain(ctx, nodes)

	// Wait for configuration to propagate
	time.Sleep(500 * time.Millisecond)

	// Connect to head and perform a write operation
	headConn, err := grpc.NewClient(nodes[0].Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to connect to head: %v", err)
	}
	defer headConn.Close()

	headClient := pb.NewMessageBoardClient(headConn)
	user, err := headClient.CreateUser(ctx, &pb.CreateUserRequest{Name: "IntegrationTest"})
	if err != nil {
		t.Fatalf("Failed to create user on head: %v", err)
	}

	if user.Id <= 0 {
		t.Error("User should have positive ID")
	}

	if !user.Committed {
		t.Error("User should be committed after propagating through chain")
	}

	// Wait for propagation
	time.Sleep(500 * time.Millisecond)

	// Connect to tail and verify read operation
	tailConn, err := grpc.NewClient(nodes[len(nodes)-1].Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to connect to tail: %v", err)
	}
	defer tailConn.Close()

	tailClient := pb.NewMessageBoardClient(tailConn)
	topics, err := tailClient.ListTopics(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("Failed to list topics on tail: %v", err)
	}

	if topics == nil {
		t.Error("Expected non-nil topics response from tail")
	}
}

// Test: server addresses are formatted correctly
func TestServerAddressFormat(t *testing.T) {
	servers := startServers(2, 60060)
	defer func() {
		for _, server := range servers {
			if server.Cmd != nil && server.Cmd.Process != nil {
				server.Cmd.Process.Kill()
				server.Cmd.Wait()
			}
		}
		time.Sleep(500 * time.Millisecond)
	}()

	for i, server := range servers {
		expectedAddress := fmt.Sprintf("localhost:%d", 60060+i)
		if server.Address != expectedAddress {
			t.Errorf("Server %d address: expected format 'localhost:PORT', got '%s'", i, server.Address)
		}

		// Verify NodeID format
		if len(server.NodeID) == 0 {
			t.Errorf("Server %d: NodeID should not be empty", i)
		}
	}
}

// Test: control panel handles server startup failures gracefully
func TestServerStartupFailure(t *testing.T) {
	// Try to start servers on a potentially occupied port range
	// This test verifies the control panel doesn't crash on failure
	servers := startServers(2, 60070)
	defer func() {
		for _, server := range servers {
			if server.Cmd != nil && server.Cmd.Process != nil {
				server.Cmd.Process.Kill()
				server.Cmd.Wait()
			}
		}
		time.Sleep(500 * time.Millisecond)
	}()

	// Even if some servers fail, we should get a slice (possibly empty)
	if servers == nil {
		t.Error("startServers should return a slice even on failure")
	}
}

// Test: configureChain handles unavailable nodes gracefully
func TestConfigureChainWithUnavailableNode(t *testing.T) {
	// Create nodes where some don't exist
	nodes := []*pb.NodeInfo{
		{NodeId: "node-1", Address: "localhost:60080"},
		{NodeId: "node-2", Address: "localhost:60081"}, // This node doesn't exist
	}

	ctx := context.Background()

	// Should not panic or crash
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("configureChain should not panic on unavailable nodes: %v", r)
		}
	}()

	configureChain(ctx, nodes)
}

// Test: multiple chains can be created with different port ranges
func TestMultipleChains(t *testing.T) {
	// Start first chain
	chain1 := startServers(2, 60090)
	defer func() {
		for _, server := range chain1 {
			if server.Cmd != nil && server.Cmd.Process != nil {
				server.Cmd.Process.Kill()
				server.Cmd.Wait()
			}
		}
		time.Sleep(500 * time.Millisecond)
	}()

	if len(chain1) != 2 {
		t.Error("Failed to start first chain")
	}

	// Start second chain on different ports
	chain2 := startServers(2, 60095)
	defer func() {
		for _, server := range chain2 {
			if server.Cmd != nil && server.Cmd.Process != nil {
				server.Cmd.Process.Kill()
				server.Cmd.Wait()
			}
		}
		time.Sleep(500 * time.Millisecond)
	}()

	if len(chain2) != 2 {
		t.Error("Failed to start second chain")
	}

	// Verify chains are independent
	if chain1[0].Port == chain2[0].Port {
		t.Error("Chains should use different ports")
	}
}
