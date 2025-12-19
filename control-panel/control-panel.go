package main

import (
	"context"
	"log"
	"time"

	pb "razpravljalnica/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	log.Println("Control Panel - Configuring chain replication...")

	// počaka, da se vsi strežniki zaženejo
	time.Sleep(2 * time.Second)

	ctx := context.Background()

	// določi konfiguracijo verige
	nodes := []*pb.NodeInfo{
		{NodeId: "node-1", Address: ":50051"},
		{NodeId: "node-2", Address: ":50052"},
		{NodeId: "node-3", Address: ":50053"},
	}

	// konfiguriraj vsako vozlišče
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

	log.Println("Chain configuration complete!")
}
