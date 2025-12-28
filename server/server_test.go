package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"testing"
	"time"

	pb "razpravljalnica/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Pomožna funkcija za zagon testnega strežnika
func startTestServer(nodeID, address string) *messageBoardServer {
	// ustvarimo primer strežnika in zaženemo gRPC listener
	grpcStreznik := grpc.NewServer() //ustvari nov gRPC strežnik

	streznik := newMessageBoardServer(nodeID, address) // ustvarimo strežnik

	pb.RegisterMessageBoardServer(grpcStreznik, streznik) // registriramo oba servisa, ta je glavni za večino funkcij odjemalca
	pb.RegisterControlPlaneServer(grpcStreznik, streznik) //nadzorna ravnina

	// odpremo TCP port
	listen, err := net.Listen("tcp", address)
	if err != nil {
		panic(err)
	}

	log.Printf("MessageBoard server %s listening on %s", nodeID, address)

	go func() {
		if err := grpcStreznik.Serve(listen); err != nil {
			panic(err)
		}
	}()

	// kratek zamik, da se listener zagotovo zažene
	time.Sleep(20 * time.Millisecond)

	return streznik
}

// Pomožna funkcija za ustvarjanje gRPC odjemalca
func createClient(address string) (pb.MessageBoardClient, pb.ControlPlaneClient, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, nil, err
	}
	mbClient := pb.NewMessageBoardClient(conn)
	cpClient := pb.NewControlPlaneClient(conn)
	return mbClient, cpClient, conn, nil
}

// Test: ustvarjanje in inicializacija strežnika
func TestServerCreation(t *testing.T) {
	server := startTestServer("test-node", ":50060")

	if server.nodeID != "test-node" {
		t.Errorf("Expected nodeID 'test-node', got '%s'", server.nodeID)
	}

	if server.address != ":50060" {
		t.Errorf("Expected address ':50060', got '%s'", server.address)
	}

	if !server.isHead() {
		t.Error("Expected new server to be head (no predecessor)")
	}

	if !server.isTail() {
		t.Error("Expected new server to be tail (no successor)")
	}
}

// Test: konfiguracija verige
func TestChainConfiguration(t *testing.T) {
	head := startTestServer("node-1", ":50061")
	middle := startTestServer("node-2", ":50062")
	tail := startTestServer("node-3", ":50063")

	nodes := []*pb.NodeInfo{
		{NodeId: "node-1", Address: ":50061"},
		{NodeId: "node-2", Address: ":50062"},
		{NodeId: "node-3", Address: ":50063"},
	}

	ctx := context.Background()

	// Konfiguriraj vsa vozlišča
	_, err := head.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})
	if err != nil {
		t.Fatalf("Failed to configure head: %v", err)
	}

	_, err = middle.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})
	if err != nil {
		t.Fatalf("Failed to configure middle: %v", err)
	}

	_, err = tail.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})
	if err != nil {
		t.Fatalf("Failed to configure tail: %v", err)
	}

	// Preveri konfiguracijo head vozlišča
	if !head.isHead() {
		t.Error("node-1 should be head")
	}
	if head.isTail() {
		t.Error("node-1 should not be tail")
	}

	// Preveri konfiguracijo srednjega vozlišča
	if middle.isHead() {
		t.Error("node-2 should not be head")
	}
	if middle.isTail() {
		t.Error("node-2 should not be tail")
	}

	// Preveri konfiguracijo tail vozlišča
	if tail.isHead() {
		t.Error("node-3 should not be head")
	}
	if !tail.isTail() {
		t.Error("node-3 should be tail")
	}
}

// Test: write operacije so dovoljene samo na headu
func TestWriteOnlyAtHead(t *testing.T) {
	head := startTestServer("node-1", ":50064")
	middle := startTestServer("node-2", ":50065")

	nodes := []*pb.NodeInfo{
		{NodeId: "node-1", Address: ":50064"},
		{NodeId: "node-2", Address: ":50065"},
	}

	ctx := context.Background()
	head.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})
	middle.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})

	head.successorClient = nil // onemogoči razširjanje za test

	// Poskus ustvarjanja uporabnika na headu (naj bi uspel)
	user, err := head.CreateUser(ctx, &pb.CreateUserRequest{Name: "Miha"})
	if err != nil {
		t.Errorf("CreateUser at head should succeed: %v", err)
	}
	if user != nil && user.Name != "Miha" {
		t.Errorf("Expected user name 'Miha', got '%s'", user.Name)
	}

	// Poskus ustvarjanja uporabnika na middleu (naj ne bi uspel)
	_, err = middle.CreateUser(ctx, &pb.CreateUserRequest{Name: "Janez"})
	if err == nil {
		t.Error("CreateUser at non-head should fail")
	}
}

// Test: read operacije so dovoljene samo na tailu
func TestReadOnlyAtTail(t *testing.T) {
	head := startTestServer("node-1", ":50066")
	tail := startTestServer("node-2", ":50067")

	nodes := []*pb.NodeInfo{
		{NodeId: "node-1", Address: ":50066"},
		{NodeId: "node-2", Address: ":50067"},
	}

	ctx := context.Background()
	head.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})
	tail.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})

	// Poskusi izpisati teme na headu (ne uspe)
	_, err := head.ListTopics(ctx, &emptypb.Empty{})
	if err == nil {
		t.Error("ListTopics at non-tail should fail")
	}

	// Poskusi izpisati teme na tailu (uspe)
	response, err := tail.ListTopics(ctx, &emptypb.Empty{})
	if err != nil {
		t.Errorf("ListTopics at tail should succeed: %v", err)
	}
	if response == nil {
		t.Error("Expected non-nil response from ListTopics")
	}
}

// Test: skladnost ID-jev po verigi
func TestIDConsistency(t *testing.T) {
	head := startTestServer("node-1", "localhost:50068")
	middle := startTestServer("node-2", "localhost:50069")
	tail := startTestServer("node-3", "localhost:50070")

	nodes := []*pb.NodeInfo{
		{NodeId: "node-1", Address: "localhost:50068"},
		{NodeId: "node-2", Address: "localhost:50069"},
		{NodeId: "node-3", Address: "localhost:50070"},
	}

	ctx := context.Background()
	head.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})
	middle.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})
	tail.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})

	time.Sleep(100 * time.Millisecond)

	// Create user at head
	user, err := head.CreateUser(ctx, &pb.CreateUserRequest{Name: "Charlie"})
	if err != nil {
		t.Fatalf("Failed to create user: %v", err)
	}

	if !user.Committed {
		t.Error("User should be committed after acknowledgment from tail")
	}

	fmt.Println("Created user with ID:", user.Id)

	// kratek zamik za razširjanje
	time.Sleep(100 * time.Millisecond)

	// Preveri, da je ID uporabnika enak na vseh vozliščih
	headUserID := user.Id

	// Preveri middle vozlišče
	if middleUser, ok := middle.users[headUserID]; !ok {
		t.Error("User not propagated to middle node")
	} else if middleUser.Name != "Charlie" {
		t.Errorf("Expected user name 'Charlie' in middle, got '%s'", middleUser.Name)
	}

	// Preveri tail vozlišče
	if tailUser, ok := tail.users[headUserID]; !ok {
		t.Error("User not propagated to tail node")
	} else if tailUser.Name != "Charlie" {
		t.Errorf("Expected user name 'Charlie' in tail, got '%s'", tailUser.Name)
	}
}

// Test: sequence numbers are assigned and increment correctly
func TestSequenceNumbers(t *testing.T) {
	head := startTestServer("node-1", "localhost:50071")
	tail := startTestServer("node-2", "localhost:50072")

	nodes := []*pb.NodeInfo{
		{NodeId: "node-1", Address: "localhost:50071"},
		{NodeId: "node-2", Address: "localhost:50072"},
	}

	ctx := context.Background()
	head.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})
	tail.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})

	// Create user and topic
	user, _ := head.CreateUser(ctx, &pb.CreateUserRequest{Name: "SeqTest"})
	topic, _ := head.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "SeqTopic"})

	if user.SequenceNumber <= 0 {
		t.Error("User should have positive sequence number")
	}
	if topic.SequenceNumber <= 0 {
		t.Error("Topic should have positive sequence number")
	}
	if topic.SequenceNumber <= user.SequenceNumber {
		t.Error("Topic sequence number should be greater than user sequence number")
	}

	time.Sleep(100 * time.Millisecond)

	// Post multiple messages and verify sequence numbers increase
	lastSeq := topic.SequenceNumber
	for i := 0; i < 3; i++ {
		msg, err := head.PostMessage(ctx, &pb.PostMessageRequest{
			TopicId: topic.Id,
			UserId:  user.Id,
			Text:    "Seq test",
		})
		if err != nil {
			t.Fatalf("Failed to post message: %v", err)
		}
		if msg.SequenceNumber != lastSeq+1 {
			t.Errorf("Sequence numbers should increase by 1: got %d after %d", msg.SequenceNumber, lastSeq)
		}
		lastSeq = msg.SequenceNumber
	}
}

// Test: committed flag is set after acknowledgment from tail
func TestCommittedFlag(t *testing.T) {
	head := startTestServer("node-1", "localhost:50073")
	tail := startTestServer("node-2", "localhost:50074")

	nodes := []*pb.NodeInfo{
		{NodeId: "node-1", Address: "localhost:50073"},
		{NodeId: "node-2", Address: "localhost:50074"},
	}

	ctx := context.Background()
	head.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})
	tail.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})

	user, _ := head.CreateUser(ctx, &pb.CreateUserRequest{Name: "CommitTest"})

	// After acknowledgment, committed flag should be true
	if !user.Committed {
		t.Error("User should be committed after acknowledgment from tail")
	}

	topic, _ := head.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "CommitTopic"})
	if !topic.Committed {
		t.Error("Topic should be committed after acknowledgment from tail")
	}

	time.Sleep(100 * time.Millisecond)

	msg, _ := head.PostMessage(ctx, &pb.PostMessageRequest{
		TopicId: topic.Id,
		UserId:  user.Id,
		Text:    "Commit test",
	})
	if !msg.Committed {
		t.Error("Message should be committed after acknowledgment from tail")
	}
}

// Test: sequence number validation rejects out-of-order operations
func TestSequenceValidation(t *testing.T) {
	head := startTestServer("node-1", "localhost:50075")
	middle := startTestServer("node-2", "localhost:50076")

	nodes := []*pb.NodeInfo{
		{NodeId: "node-1", Address: "localhost:50075"},
		{NodeId: "node-2", Address: "localhost:50076"},
	}

	ctx := context.Background()
	head.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})
	middle.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})

	user, _ := head.CreateUser(ctx, &pb.CreateUserRequest{Name: "ValidTest"})
	topic, _ := head.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "ValidTopic"})

	time.Sleep(100 * time.Millisecond)

	// Manually try to propagate a message with a sequence number that skips values
	// This simulates an out-of-order operation
	_, err := middle.PropagatePost(ctx, &pb.PostMessageRequest{
		TopicId:        topic.Id,
		UserId:         user.Id,
		Text:           "Out of order",
		MessageId:      1,
		SequenceNumber: middle.lastSeqNumber + 10, // Skip many sequence numbers
	})

	if err == nil {
		t.Error("Should reject operation with out-of-order sequence number")
	}
}

// Test: head and tail detection using explicit addresses
func TestHeadTailDetectionWithAddresses(t *testing.T) {
	head := startTestServer("node-1", "localhost:50077")
	middle := startTestServer("node-2", "localhost:50078")
	tail := startTestServer("node-3", "localhost:50079")

	nodes := []*pb.NodeInfo{
		{NodeId: "node-1", Address: "localhost:50077"},
		{NodeId: "node-2", Address: "localhost:50078"},
		{NodeId: "node-3", Address: "localhost:50079"},
	}

	ctx := context.Background()
	head.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})
	middle.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})
	tail.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})

	// Verify head detection
	if !head.isHead() {
		t.Error("node-1 should be detected as head")
	}
	if head.isTail() {
		t.Error("node-1 should not be detected as tail")
	}

	// Verify middle detection
	if middle.isHead() {
		t.Error("node-2 should not be detected as head")
	}
	if middle.isTail() {
		t.Error("node-2 should not be detected as tail")
	}

	// Verify tail detection
	if tail.isHead() {
		t.Error("node-3 should not be detected as head")
	}
	if !tail.isTail() {
		t.Error("node-3 should be detected as tail")
	}

	// Simulate middle node failure by setting its successor to nil
	// Head and tail should still be correctly identified
	middle.lock.Lock()
	middle.successor = nil
	middle.lock.Unlock()

	// Head and tail detection should still work correctly
	if !head.isHead() {
		t.Error("node-1 should still be head after middle node successor is nil")
	}
	if !tail.isTail() {
		t.Error("node-3 should still be tail after middle node successor is nil")
	}
	if middle.isTail() {
		t.Error("node-2 should not be detected as tail even with nil successor")
	}
}

// Test: acknowledgment timeout behavior
func TestAcknowledgmentTimeout(t *testing.T) {
	head := startTestServer("node-1", "localhost:50080")
	middle := startTestServer("node-2", "localhost:50081")

	nodes := []*pb.NodeInfo{
		{NodeId: "node-1", Address: "localhost:50080"},
		{NodeId: "node-2", Address: "localhost:50081"},
	}

	ctx := context.Background()
	head.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})
	middle.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})

	// Disconnect successor to simulate network failure
	head.lock.Lock()
	head.successorClient = nil
	head.lock.Unlock()

	user, err := head.CreateUser(ctx, &pb.CreateUserRequest{Name: "TimeoutTest"})

	// Should timeout and return user with committed=false
	if err != nil {
		t.Fatalf("Should not error on timeout: %v", err)
	}
	if user.Committed {
		t.Error("User should not be committed when acknowledgment times out")
	}
}

// Test: acknowledgment propagates backward through chain
func TestAcknowledgmentPropagation(t *testing.T) {
	head := startTestServer("node-1", "localhost:50082")
	middle := startTestServer("node-2", "localhost:50083")
	tail := startTestServer("node-3", "localhost:50084")

	nodes := []*pb.NodeInfo{
		{NodeId: "node-1", Address: "localhost:50082"},
		{NodeId: "node-2", Address: "localhost:50083"},
		{NodeId: "node-3", Address: "localhost:50084"},
	}

	ctx := context.Background()
	head.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})
	middle.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})
	tail.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})

	user, _ := head.CreateUser(ctx, &pb.CreateUserRequest{Name: "AckPropTest"})
	topic, _ := head.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "AckPropTopic"})

	time.Sleep(100 * time.Millisecond)

	// Post message and verify it gets committed
	msg, err := head.PostMessage(ctx, &pb.PostMessageRequest{
		TopicId: topic.Id,
		UserId:  user.Id,
		Text:    "Ack propagation test",
	})

	if err != nil {
		t.Fatalf("Failed to post message: %v", err)
	}

	if !msg.Committed {
		t.Error("Message should be committed after acknowledgment propagates from tail")
	}

	// Verify message exists on all nodes
	time.Sleep(100 * time.Millisecond)

	head.lock.RLock()
	headMsg := head.messages[topic.Id][msg.Id]
	head.lock.RUnlock()

	middle.lock.RLock()
	middleMsg := middle.messages[topic.Id][msg.Id]
	middle.lock.RUnlock()

	tail.lock.RLock()
	tailMsg := tail.messages[topic.Id][msg.Id]
	tail.lock.RUnlock()

	if headMsg == nil || middleMsg == nil || tailMsg == nil {
		t.Error("Message should exist on all nodes")
	}
}

// Test: single node chain works correctly as both head and tail
func TestSingleNodeChain(t *testing.T) {
	node := startTestServer("node-1", "localhost:50085")

	nodes := []*pb.NodeInfo{
		{NodeId: "node-1", Address: "localhost:50085"},
	}

	ctx := context.Background()
	node.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})

	// Should be both head and tail
	if !node.isHead() {
		t.Error("Single node should be head")
	}
	if !node.isTail() {
		t.Error("Single node should be tail")
	}

	// Should be able to perform both write and read operations
	user, err := node.CreateUser(ctx, &pb.CreateUserRequest{Name: "Solo"})
	if err != nil {
		t.Errorf("Should be able to create user on single node: %v", err)
	}

	// Should not wait for acknowledgment (immediate commit)
	if !user.Committed {
		t.Error("Single node operations should be immediately committed")
	}

	// Should be able to read
	topics, err := node.ListTopics(ctx, &emptypb.Empty{})
	if err != nil {
		t.Errorf("Should be able to list topics on single node: %v", err)
	}
	if topics == nil {
		t.Error("Expected non-nil topics response")
	}
}

// Test: ustvarjanje teme in razširjanje
func TestTopicPropagation(t *testing.T) {
	head := startTestServer("node-1", ":50071")
	tail := startTestServer("node-2", ":50072")

	nodes := []*pb.NodeInfo{
		{NodeId: "node-1", Address: ":50071"},
		{NodeId: "node-2", Address: ":50072"},
	}

	ctx := context.Background()
	head.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})
	tail.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})

	// Ustvari temo na headu
	topic, err := head.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "General"})
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Preveri, da se je tema propagirala na tail
	if tailTopic, ok := tail.topics[topic.Id]; !ok {
		t.Error("Topic not propagated to tail")
	} else if tailTopic.Name != "General" {
		t.Errorf("Expected topic name 'General', got '%s'", tailTopic.Name)
	}
}

// Test: objava sporočila in propagacija
func TestMessagePropagation(t *testing.T) {
	head := startTestServer("node-1", ":50073")
	tail := startTestServer("node-2", ":50074")

	nodes := []*pb.NodeInfo{
		{NodeId: "node-1", Address: ":50073"},
		{NodeId: "node-2", Address: ":50074"},
	}

	ctx := context.Background()
	head.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})
	tail.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})

	// Ustvari uporabnika in temo
	user, _ := head.CreateUser(ctx, &pb.CreateUserRequest{Name: "Dave"})
	topic, _ := head.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "Tech"})

	time.Sleep(100 * time.Millisecond)

	// Objavi sporočilo
	message, err := head.PostMessage(ctx, &pb.PostMessageRequest{
		TopicId: topic.Id,
		UserId:  user.Id,
		Text:    "Hello World!",
	})
	if err != nil {
		t.Fatalf("Failed to post message: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Preveri, da se je sporočilo propagiralo na tail
	if tailMessages, ok := tail.messages[topic.Id]; !ok {
		t.Error("Messages not propagated to tail")
	} else if tailMsg, ok := tailMessages[message.Id]; !ok {
		t.Error("Specific message not found in tail")
	} else if tailMsg.Text != "Hello World!" {
		t.Errorf("Expected message text 'Hello World!', got '%s'", tailMsg.Text)
	}
}

// Test: razširjanje posodobitve sporočila
func TestMessageUpdatePropagation(t *testing.T) {
	head := startTestServer("node-1", ":50075")
	tail := startTestServer("node-2", ":50076")

	nodes := []*pb.NodeInfo{
		{NodeId: "node-1", Address: ":50075"},
		{NodeId: "node-2", Address: ":50076"},
	}

	ctx := context.Background()
	head.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})
	tail.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})

	// Ustvari uporabnika, temo in sporočilo
	user, _ := head.CreateUser(ctx, &pb.CreateUserRequest{Name: "Eve"})
	topic, _ := head.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "News"})
	time.Sleep(100 * time.Millisecond)

	message, _ := head.PostMessage(ctx, &pb.PostMessageRequest{
		TopicId: topic.Id,
		UserId:  user.Id,
		Text:    "Original",
	})
	time.Sleep(100 * time.Millisecond)

	// Posodobi sporočilo
	_, err := head.UpdateMessage(ctx, &pb.UpdateMessageRequest{
		TopicId:   topic.Id,
		UserId:    user.Id,
		MessageId: message.Id,
		Text:      "Updated",
	})
	if err != nil {
		t.Fatalf("Failed to update message: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Preveri, da se je posodobitev razširila na tail
	if tailMsg, ok := tail.messages[topic.Id][message.Id]; !ok {
		t.Error("Updated message not found in tail")
	} else if tailMsg.Text != "Updated" {
		t.Errorf("Expected updated text 'Updated', got '%s'", tailMsg.Text)
	}
}

// Test: razširjanje brisanja sporočila
func TestMessageDeletePropagation(t *testing.T) {
	head := startTestServer("node-1", ":50077")
	tail := startTestServer("node-2", ":50078")

	nodes := []*pb.NodeInfo{
		{NodeId: "node-1", Address: ":50077"},
		{NodeId: "node-2", Address: ":50078"},
	}

	ctx := context.Background()
	head.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})
	tail.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})

	// Ustvari uporabnika, temo in sporočilo
	user, _ := head.CreateUser(ctx, &pb.CreateUserRequest{Name: "Frank"})
	topic, _ := head.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "Random"})
	time.Sleep(100 * time.Millisecond)

	message, _ := head.PostMessage(ctx, &pb.PostMessageRequest{
		TopicId: topic.Id,
		UserId:  user.Id,
		Text:    "To be deleted",
	})
	time.Sleep(100 * time.Millisecond)

	// Preveri, da sporočilo obstaja na tailu pred brisanjem
	if _, ok := tail.messages[topic.Id][message.Id]; !ok {
		t.Error("Message should exist in tail before deletion")
	}

	// Izbriši sporočilo
	_, err := head.DeleteMessage(ctx, &pb.DeleteMessageRequest{
		TopicId:   topic.Id,
		UserId:    user.Id,
		MessageId: message.Id,
	})
	if err != nil {
		t.Fatalf("Failed to delete message: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Preveri, da se je brisanje razširilo na tail
	if _, ok := tail.messages[topic.Id][message.Id]; ok {
		t.Error("Message should be deleted from tail")
	}
}

// Test: razširjanje všečkov
func TestLikePropagation(t *testing.T) {
	head := startTestServer("node-1", ":50079")
	tail := startTestServer("node-2", ":50080")

	nodes := []*pb.NodeInfo{
		{NodeId: "node-1", Address: ":50079"},
		{NodeId: "node-2", Address: ":50080"},
	}

	ctx := context.Background()
	head.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})
	tail.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})

	// Ustvari uporabnika, temo in sporočilo
	user, _ := head.CreateUser(ctx, &pb.CreateUserRequest{Name: "Grace"})
	topic, _ := head.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "Likes"})
	time.Sleep(100 * time.Millisecond)

	message, _ := head.PostMessage(ctx, &pb.PostMessageRequest{
		TopicId: topic.Id,
		UserId:  user.Id,
		Text:    "Like this!",
	})
	time.Sleep(100 * time.Millisecond)

	// Všečkaj sporočilo
	_, err := head.LikeMessage(ctx, &pb.LikeMessageRequest{
		TopicId:   topic.Id,
		MessageId: message.Id,
		UserId:    user.Id,
	})
	if err != nil {
		t.Fatalf("Failed to like message: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Preveri, da se je všečkanje razširilo na tail
	if tailMsg, ok := tail.messages[topic.Id][message.Id]; !ok {
		t.Error("Message not found in tail")
	} else if tailMsg.Likes != 1 {
		t.Errorf("Expected 1 like, got %d", tailMsg.Likes)
	}
}

// Test: avtentikacija - uporabnik lahko posodobi samo svoja sporočila
func TestUpdateAuthorization(t *testing.T) {
	server := startTestServer("node-1", ":50081")

	// Naredi vozlišče hkrati head in tail (single node chain)
	nodes := []*pb.NodeInfo{
		{NodeId: "node-1", Address: ":50081"},
	}
	ctx := context.Background()
	server.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})

	// Ustvari dva uporabnika
	user1, _ := server.CreateUser(ctx, &pb.CreateUserRequest{Name: "User1"})
	user2, _ := server.CreateUser(ctx, &pb.CreateUserRequest{Name: "User2"})
	topic, _ := server.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "Auth"})

	// User1 objavi sporočilo
	message, _ := server.PostMessage(ctx, &pb.PostMessageRequest{
		TopicId: topic.Id,
		UserId:  user1.Id,
		Text:    "User1's message",
	})

	// User2 poskuša posodobi sporočilo od User1 (ne uspe)
	_, err := server.UpdateMessage(ctx, &pb.UpdateMessageRequest{
		TopicId:   topic.Id,
		UserId:    user2.Id,
		MessageId: message.Id,
		Text:      "Hacked!",
	})
	if err == nil {
		t.Error("User2 should not be able to update User1's message")
	}

	// User1 posodobi svoje sporočilo (uspe)
	_, err = server.UpdateMessage(ctx, &pb.UpdateMessageRequest{
		TopicId:   topic.Id,
		UserId:    user1.Id,
		MessageId: message.Id,
		Text:      "Updated by User1",
	})
	if err != nil {
		t.Errorf("User1 should be able to update their own message: %v", err)
	}
}

// Test: napaka pri neobstoječem uporabniku
func TestNonExistentUser(t *testing.T) {
	server := startTestServer("node-1", ":50082")
	nodes := []*pb.NodeInfo{{NodeId: "node-1", Address: ":50082"}}
	ctx := context.Background()
	server.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})

	topic, _ := server.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "Test"})

	// Poskus objave sporočila z neobstoječim uporabnikom
	_, err := server.PostMessage(ctx, &pb.PostMessageRequest{
		TopicId: topic.Id,
		UserId:  9999,
		Text:    "Ghost message",
	})
	if err == nil {
		t.Error("Should fail when posting with non-existent user")
	}
}

// Test: napaka pri neobstoječi temi
func TestNonExistentTopic(t *testing.T) {
	server := startTestServer("node-1", ":50083")
	nodes := []*pb.NodeInfo{{NodeId: "node-1", Address: ":50083"}}
	ctx := context.Background()
	server.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})

	user, _ := server.CreateUser(ctx, &pb.CreateUserRequest{Name: "User"})

	// Poskus objave sporočila v neobstoječo temo
	_, err := server.PostMessage(ctx, &pb.PostMessageRequest{
		TopicId: 9999,
		UserId:  user.Id,
		Text:    "Lost message",
	})
	if err == nil {
		t.Error("Should fail when posting to non-existent topic")
	}
}

// Test: GetMessages s filtriranjem
func TestGetMessagesFiltering(t *testing.T) {
	server := startTestServer("node-1", ":50084")
	nodes := []*pb.NodeInfo{{NodeId: "node-1", Address: ":50084"}}
	ctx := context.Background()
	server.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})

	user, _ := server.CreateUser(ctx, &pb.CreateUserRequest{Name: "User"})
	topic, _ := server.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "Test"})

	// Objavi 5 sporočil
	for i := 1; i <= 5; i++ {
		server.PostMessage(ctx, &pb.PostMessageRequest{
			TopicId: topic.Id,
			UserId:  user.Id,
			Text:    "Message",
		})
	}

	// Pridobi sporočila z omejitvijo
	response, err := server.GetMessages(ctx, &pb.GetMessagesRequest{
		TopicId:       topic.Id,
		FromMessageId: 0,
		Limit:         3,
	})
	if err != nil {
		t.Fatalf("Failed to get messages: %v", err)
	}
	if len(response.Messages) != 3 {
		t.Errorf("Expected 3 messages, got %d", len(response.Messages))
	}

	// Pridobi sporočila, od določenega ID-ja naprej
	response, err = server.GetMessages(ctx, &pb.GetMessagesRequest{
		TopicId:       topic.Id,
		FromMessageId: 3,
		Limit:         0,
	})
	if err != nil {
		t.Fatalf("Failed to get messages: %v", err)
	}
	if len(response.Messages) != 3 {
		t.Errorf("Expected 3 messages (from ID 3 to 5), got %d", len(response.Messages))
	}
}

// Test: generiranje in validacija tokena za naročnino
func TestSubscriptionToken(t *testing.T) {
	server := startTestServer("node-1", ":50085")
	nodes := []*pb.NodeInfo{{NodeId: "node-1", Address: ":50085"}}
	ctx := context.Background()
	server.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})

	user, _ := server.CreateUser(ctx, &pb.CreateUserRequest{Name: "User"})
	topic, _ := server.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "Test"})

	// Pridobi vozlišče za naročnino (in token)
	response, err := server.GetSubscriptionNode(ctx, &pb.SubscriptionNodeRequest{
		UserId:  user.Id,
		TopicId: []int64{topic.Id},
	})
	if err != nil {
		t.Fatalf("Failed to get subscription node: %v", err)
	}
	if response.SubscribeToken == "" {
		t.Error("Expected non-empty subscription token")
	}
}

// Test: naročnina z backlogom
func TestSubscriptionWithBacklog(t *testing.T) {
	server := startTestServer("node-1", ":50086")
	nodes := []*pb.NodeInfo{{NodeId: "node-1", Address: ":50086"}}
	ctx := context.Background()
	server.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})

	user, _ := server.CreateUser(ctx, &pb.CreateUserRequest{Name: "User"})
	topic, _ := server.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "Test"})

	// Objavi nekaj sporočil pred naročitvijo
	for i := 1; i <= 3; i++ {
		server.PostMessage(ctx, &pb.PostMessageRequest{
			TopicId: topic.Id,
			UserId:  user.Id,
			Text:    "Backlog message",
		})
	}

	// Pridobi token za naročnino
	subResponse, _ := server.GetSubscriptionNode(ctx, &pb.SubscriptionNodeRequest{
		UserId:  user.Id,
		TopicId: []int64{topic.Id},
	})

	// Ustvari lažen stream
	type mockStream struct {
		events []*pb.MessageEvent
	}

	// Preveri, da je token shranjen
	if _, ok := server.tokens[subResponse.SubscribeToken]; !ok {
		t.Error("Subscription token should be stored")
	}
}

// Test: enoličnost uporabniškega imena
func TestUsernameUniqueness(t *testing.T) {
	server := startTestServer("node-1", ":50087")
	nodes := []*pb.NodeInfo{{NodeId: "node-1", Address: ":50087"}}
	ctx := context.Background()
	server.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})

	// Ustvari prvega uporabnika
	_, err := server.CreateUser(ctx, &pb.CreateUserRequest{Name: "Miha"})
	if err != nil {
		t.Fatalf("Failed to create first user: %v", err)
	}

	// Poskusi ustvariti uporabnika z enakim imenom
	_, err = server.CreateUser(ctx, &pb.CreateUserRequest{Name: "Miha"})
	if err == nil {
		t.Error("Should not allow duplicate usernames")
	}
}

// Test: validacija praznega sporočila
func TestEmptyMessageValidation(t *testing.T) {
	server := startTestServer("node-1", ":50088")
	nodes := []*pb.NodeInfo{{NodeId: "node-1", Address: ":50088"}}
	ctx := context.Background()
	server.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})

	user, _ := server.CreateUser(ctx, &pb.CreateUserRequest{Name: "User"})
	topic, _ := server.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "Test"})

	// Poskusi objaviti prazno sporočilo
	_, err := server.PostMessage(ctx, &pb.PostMessageRequest{
		TopicId: topic.Id,
		UserId:  user.Id,
		Text:    "",
	})
	if err == nil {
		t.Error("Should not allow empty message text")
	}
}

// Test: porazdelitev obremenitve z več vozlišči
func TestLoadBalancing(t *testing.T) {
	node1 := startTestServer("node-1", ":50089")
	node2 := startTestServer("node-2", ":50090")
	node3 := startTestServer("node-3", ":50091")

	nodes := []*pb.NodeInfo{
		{NodeId: "node-1", Address: ":50089"},
		{NodeId: "node-2", Address: ":50090"},
		{NodeId: "node-3", Address: ":50091"},
	}

	ctx := context.Background()
	node1.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})
	node2.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})
	node3.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})

	user, _ := node1.CreateUser(ctx, &pb.CreateUserRequest{Name: "User"})
	topic, _ := node1.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "Test"})

	time.Sleep(100 * time.Millisecond)

	// Zahtevaj vozlišče za naročnino večkrat in beleži izbrana vozlišča
	selectedNodes := make(map[string]int)

	for i := 0; i < 6; i++ {
		response, err := node1.GetSubscriptionNode(ctx, &pb.SubscriptionNodeRequest{
			UserId:  user.Id,
			TopicId: []int64{topic.Id},
		})
		if err != nil {
			t.Fatalf("Failed to get subscription node: %v", err)
		}
		selectedNodes[response.Node.NodeId]++
	}

	// Vsa vozlišča bi morala biti izbrana (porazdelitev obremenitve)
	if len(selectedNodes) != 3 {
		t.Errorf("Expected all 3 nodes to be selected, got %d", len(selectedNodes))
	}

	// Vsako vozlišče bi moralo imeti približno enako porazdelitev (2 vsako)
	for nodeID, count := range selectedNodes {
		if count != 2 {
			t.Logf("Node %s selected %d times (expected 2)", nodeID, count)
		}
	}
}

// Integracijski test: Celoten potek
func TestFullWorkflow(t *testing.T) {
	head := startTestServer("node-1", ":50092")
	middle := startTestServer("node-2", ":50093")
	tail := startTestServer("node-3", ":50094")

	nodes := []*pb.NodeInfo{
		{NodeId: "node-1", Address: ":50092"},
		{NodeId: "node-2", Address: ":50093"},
		{NodeId: "node-3", Address: ":50094"},
	}

	ctx := context.Background()
	head.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})
	middle.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})
	tail.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})

	// Ustvari uporabnike
	miha, _ := head.CreateUser(ctx, &pb.CreateUserRequest{Name: "Miha"})
	janez, _ := head.CreateUser(ctx, &pb.CreateUserRequest{Name: "Janez"})

	// Ustvari temo
	topic, _ := head.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "General"})

	time.Sleep(100 * time.Millisecond)

	// Miha objavi sporočilo
	msg1, _ := head.PostMessage(ctx, &pb.PostMessageRequest{
		TopicId: topic.Id,
		UserId:  miha.Id,
		Text:    "Hello everyone!",
	})

	// Janez objavi sporočilo
	msg2, _ := head.PostMessage(ctx, &pb.PostMessageRequest{
		TopicId: topic.Id,
		UserId:  janez.Id,
		Text:    "Hi Miha!",
	})

	// Miha všečka Janezovo sporočilo
	head.LikeMessage(ctx, &pb.LikeMessageRequest{
		TopicId:   topic.Id,
		MessageId: msg2.Id,
		UserId:    miha.Id,
	})

	time.Sleep(200 * time.Millisecond)

	// Branje s tail vozlišča
	messages, err := tail.GetMessages(ctx, &pb.GetMessagesRequest{
		TopicId:       topic.Id,
		FromMessageId: 0,
		Limit:         0,
	})
	if err != nil {
		t.Fatalf("Failed to get messages from tail: %v", err)
	}

	if len(messages.Messages) != 2 {
		t.Errorf("Expected 2 messages, got %d", len(messages.Messages))
	}

	// Preveri, da ima Janezovo sporočilo 1 všeček
	janezMsg := messages.Messages[1]
	if janezMsg.Likes != 1 {
		t.Errorf("Expected Janez's message to have 1 like, got %d", janezMsg.Likes)
	}

	// Miha posodobi svoje sporočilo
	head.UpdateMessage(ctx, &pb.UpdateMessageRequest{
		TopicId:   topic.Id,
		UserId:    miha.Id,
		MessageId: msg1.Id,
		Text:      "Hello everyone! (edited)",
	})

	time.Sleep(100 * time.Millisecond)

	// Preveri posodobitev na tailu
	updatedMsg := tail.messages[topic.Id][msg1.Id]
	if updatedMsg.Text != "Hello everyone! (edited)" {
		t.Errorf("Expected updated text, got '%s'", updatedMsg.Text)
	}

	// Miha izbriše svoje sporočilo
	head.DeleteMessage(ctx, &pb.DeleteMessageRequest{
		TopicId:   topic.Id,
		UserId:    miha.Id,
		MessageId: msg1.Id,
	})

	time.Sleep(100 * time.Millisecond)

	// Preveri izbris na tailu
	if _, ok := tail.messages[topic.Id][msg1.Id]; ok {
		t.Error("Message should be deleted from tail")
	}
}

// Meritve: razširjanje sporočil
func BenchmarkMessagePropagation(b *testing.B) {
	head := startTestServer("node-1", ":50095")
	tail := startTestServer("node-2", ":50096")

	nodes := []*pb.NodeInfo{
		{NodeId: "node-1", Address: ":50095"},
		{NodeId: "node-2", Address: ":50096"},
	}

	ctx := context.Background()
	head.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})
	tail.ConfigureChain(ctx, &pb.ConfigureChainRequest{Nodes: nodes})

	user, _ := head.CreateUser(ctx, &pb.CreateUserRequest{Name: "User"})
	topic, _ := head.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "Bench"})

	time.Sleep(100 * time.Millisecond)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		head.PostMessage(ctx, &pb.PostMessageRequest{
			TopicId: topic.Id,
			UserId:  user.Id,
			Text:    "Benchmark message",
		})
	}
}
