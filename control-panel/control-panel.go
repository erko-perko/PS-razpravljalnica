package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	pb "razpravljalnica/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ServerProcess tracks a running server process
type ServerProcess struct {
	NodeID  string
	Address string
	Port    int
	Cmd     *exec.Cmd
}

func main() {
	// parse command line arguments
	numServers := flag.Int("n", 3, "number of servers in the chain")
	headPort := flag.Int("p", 50051, "port number for the head server")
	flag.Parse()

	log.Printf("Control Panel - Starting chain with %d servers, head port: %d", *numServers, *headPort)

	ctx := context.Background()

	// start all servers
	servers := startServers(*numServers, *headPort)
	if len(servers) == 0 {
		log.Fatal("Failed to start any servers")
	}

	// wait for servers to be ready
	log.Println("Waiting for servers to start...")
	time.Sleep(3 * time.Second)

	// build node configuration
	nodes := make([]*pb.NodeInfo, 0, len(servers))
	for _, server := range servers {
		nodes = append(nodes, &pb.NodeInfo{
			NodeId:  server.NodeID,
			Address: server.Address,
		})
	}

	// configure the chain on all nodes
	configureChain(ctx, nodes)

	log.Println("Chain configuration complete!")
	log.Println("Press Ctrl+C to stop all servers...")

	// keep control panel running until interrupt
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down servers...")
	for _, server := range servers {
		if server.Cmd != nil && server.Cmd.Process != nil {
			server.Cmd.Process.Kill()
		}
	}
}

// startServers starts the specified number of server processes
func startServers(numServers int, headPort int) []*ServerProcess {
	servers := make([]*ServerProcess, 0, numServers)

	for i := 0; i < numServers; i++ {
		nodeID := fmt.Sprintf("node-%d", i+1)
		port := headPort + i
		address := fmt.Sprintf("localhost:%d", port)

		log.Printf("Starting %s on port %d...", nodeID, port)

		// create command to start server
		cmd := exec.Command(
			"../server/server.exe",
			"-n", nodeID,
			"-a", fmt.Sprintf("%d", port),
		)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		cmd.SysProcAttr = &syscall.SysProcAttr{
			CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP,
		}

		if err := cmd.Start(); err != nil {
			log.Fatal(err)
		}

		server := &ServerProcess{
			NodeID:  nodeID,
			Address: address,
			Port:    port,
			Cmd:     cmd,
		}
		servers = append(servers, server)

		log.Printf("Started %s (PID: %d)", nodeID, cmd.Process.Pid)
	}

	return servers
}

// configureChain sends chain configuration to all nodes
func configureChain(ctx context.Context, nodes []*pb.NodeInfo) {
	log.Println("Configuring chain replication...")

	for _, node := range nodes {
		log.Printf("Configuring %s at %s...", node.NodeId, node.Address)

		conn, err := grpc.NewClient(node.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("Failed to connect to %s: %v", node.NodeId, err)
			continue
		}

		client := pb.NewControlPlaneClient(conn)
		_, err = client.ConfigureChain(ctx, &pb.ConfigureChainRequest{
			Nodes: nodes,
		})
		conn.Close()

		if err != nil {
			log.Printf("Failed to configure %s: %v", node.NodeId, err)
		} else {
			log.Printf("Successfully configured %s", node.NodeId)
		}
	}
}
