package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	pb "razpravljalnica/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestMain(m *testing.M) {
	old := log.Writer()
	log.SetOutput(io.Discard)
	code := m.Run()
	log.SetOutput(old)
	os.Exit(code)
}

func eventually(t *testing.T, timeout time.Duration, cond func() bool, msgAndArgs ...any) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(msgAndArgs) > 0 {
		if msg, ok := msgAndArgs[0].(string); ok {
			t.Fatalf(msg, msgAndArgs[1:]...)
		}
		t.Fatalf("%v", msgAndArgs...)
	}
	t.Fatalf("condition not met within %s", timeout)
}

type testNode struct {
	id      string
	addr    string
	server  *messageBoardServer
	grpcSrv *grpc.Server
	lis     net.Listener
}

func startTestNode(t *testing.T, nodeID string) *testNode {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	addr := lis.Addr().String()
	s := newMessageBoardServer(nodeID, addr, "", "test-secret")
	grpcSrv := grpc.NewServer()
	pb.RegisterMessageBoardServer(grpcSrv, s)

	go func() {
		_ = grpcSrv.Serve(lis)
	}()

	n := &testNode{id: nodeID, addr: addr, server: s, grpcSrv: grpcSrv, lis: lis}
	t.Cleanup(func() {
		grpcSrv.Stop()
		_ = lis.Close()
		s.lock.Lock()
		if s.successorConn != nil {
			_ = s.successorConn.Close()
			s.successorConn = nil
			s.successorClient = nil
		}
		s.lock.Unlock()
	})
	return n
}

func configureChain(t *testing.T, nodes []*testNode) []*pb.NodeInfo {
	t.Helper()
	chain := make([]*pb.NodeInfo, 0, len(nodes))
	for _, n := range nodes {
		chain = append(chain, &pb.NodeInfo{NodeId: n.id, Address: n.addr})
	}
	for _, n := range nodes {
		if err := n.server.applyChainConfig(chain); err != nil {
			t.Fatalf("applyChainConfig(%s): %v", n.id, err)
		}
	}
	return chain
}

type fakeSubscribeStream struct {
	ctx    context.Context
	mu     sync.Mutex
	events []*pb.MessageEvent
}

func (f *fakeSubscribeStream) Send(ev *pb.MessageEvent) error {
	f.mu.Lock()
	f.events = append(f.events, ev)
	f.mu.Unlock()
	return nil
}

func (f *fakeSubscribeStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakeSubscribeStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeSubscribeStream) SetTrailer(metadata.MD)       {}
func (f *fakeSubscribeStream) Context() context.Context     { return f.ctx }
func (f *fakeSubscribeStream) SendMsg(any) error            { return nil }
func (f *fakeSubscribeStream) RecvMsg(any) error            { return nil }

func (f *fakeSubscribeStream) snapshot() []*pb.MessageEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*pb.MessageEvent, len(f.events))
	copy(out, f.events)
	return out
}

func TestTokenHelpers_ValidAndInvalid(t *testing.T) {
	s := newMessageBoardServer("n", "127.0.0.1:1", "", "secret")

	tok, err := s.makeSubscribeToken(42, []int64{1, 2, 3}, time.Now().Add(1*time.Minute).Unix())
	if err != nil {
		t.Fatalf("makeSubscribeToken: %v", err)
	}
	topics, err := s.validateSubscribeToken(tok, 42)
	if err != nil {
		t.Fatalf("validateSubscribeToken valid: %v", err)
	}
	if fmt.Sprint(topics) != fmt.Sprint([]int64{1, 2, 3}) {
		t.Fatalf("topics mismatch: got %v", topics)
	}

	if _, err := s.validateSubscribeToken(tok, 43); err == nil {
		t.Fatalf("expected error for wrong user")
	}

	expired, _ := s.makeSubscribeToken(42, []int64{1}, time.Now().Add(-1*time.Minute).Unix())
	if _, err := s.validateSubscribeToken(expired, 42); err == nil {
		t.Fatalf("expected expired token error")
	}

	// Tamper signature
	if _, err := s.validateSubscribeToken(tok+"x", 42); err == nil {
		t.Fatalf("expected invalid token error")
	}
}

func TestValidateSeqLocked_OutOfOrder(t *testing.T) {
	s := newMessageBoardServer("n", "127.0.0.1:1", "", "secret")

	s.lock.Lock()
	defer s.lock.Unlock()

	if err := s.validateSeqLocked(1); err != nil {
		t.Fatalf("seq=1 should succeed: %v", err)
	}
	if err := s.validateSeqLocked(1); err != nil {
		t.Fatalf("duplicate seq should succeed: %v", err)
	}
	if err := s.validateSeqLocked(3); err == nil {
		t.Fatalf("expected out-of-order error")
	}
}

func TestWaitAckTimeout(t *testing.T) {
	s := newMessageBoardServer("n", "127.0.0.1:1", "", "secret")
	s.prepareAck(7)
	if ok := s.waitAck(7, 50*time.Millisecond); ok {
		t.Fatalf("expected timeout")
	}
}

func TestChainConfiguration_HeadMiddleTail(t *testing.T) {
	n1 := startTestNode(t, "node-1")
	n2 := startTestNode(t, "node-2")
	n3 := startTestNode(t, "node-3")
	configureChain(t, []*testNode{n1, n2, n3})

	if !n1.server.isHead() || n1.server.isTail() {
		t.Fatalf("node-1 should be head only")
	}
	if n2.server.isHead() || n2.server.isTail() {
		t.Fatalf("node-2 should be middle")
	}
	if !n3.server.isTail() || n3.server.isHead() {
		t.Fatalf("node-3 should be tail only")
	}
}

func TestWriteOnlyAtHead_ReadOnlyAtTail(t *testing.T) {
	n1 := startTestNode(t, "node-1")
	n2 := startTestNode(t, "node-2")
	n3 := startTestNode(t, "node-3")
	configureChain(t, []*testNode{n1, n2, n3})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if _, err := n2.server.CreateUser(ctx, &pb.CreateUserRequest{Name: "a", RequestId: "r1"}); err == nil {
		t.Fatalf("expected write rejected at non-head")
	}
	if _, err := n1.server.ListTopics(ctx, &emptypb.Empty{}); err == nil {
		t.Fatalf("expected read rejected at non-tail")
	}
}

func TestSingleNodeChain_WriteAndRead(t *testing.T) {
	n1 := startTestNode(t, "node-1")
	configureChain(t, []*testNode{n1})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	u, err := n1.server.CreateUser(ctx, &pb.CreateUserRequest{Name: "u1", RequestId: "rid-u1"})
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	if !u.GetCommitted() {
		t.Fatalf("expected committed user in single-node chain")
	}

	topic, err := n1.server.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "t1", RequestId: "rid-t1"})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if !topic.GetCommitted() {
		t.Fatalf("expected committed topic")
	}

	msg, err := n1.server.PostMessage(ctx, &pb.PostMessageRequest{UserId: u.GetId(), TopicId: topic.GetId(), Text: "hello", RequestId: "rid-m1"})
	if err != nil {
		t.Fatalf("PostMessage: %v", err)
	}
	if !msg.GetCommitted() {
		t.Fatalf("expected committed message")
	}

	// Reads on same node (it is also tail)
	topics, err := n1.server.ListTopics(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("ListTopics: %v", err)
	}
	if len(topics.GetTopics()) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(topics.GetTopics()))
	}

	msgs, err := n1.server.GetMessages(ctx, &pb.GetMessagesRequest{TopicId: topic.GetId(), FromMessageId: 1, Limit: 10})
	if err != nil {
		t.Fatalf("GetMessages: %v", err)
	}
	if len(msgs.GetMessages()) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs.GetMessages()))
	}
}

func TestIdempotencyAndUsernameUniqueness(t *testing.T) {
	head := startTestNode(t, "node-1")
	tail := startTestNode(t, "node-2")
	configureChain(t, []*testNode{head, tail})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	u1, err := head.server.CreateUser(ctx, &pb.CreateUserRequest{Name: "name", RequestId: "rid1"})
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	u1b, err := head.server.CreateUser(ctx, &pb.CreateUserRequest{Name: "name", RequestId: "rid1"})
	if err != nil {
		t.Fatalf("CreateUser idempotent: %v", err)
	}
	if u1b.GetId() != u1.GetId() {
		t.Fatalf("expected same user id on idempotent call")
	}

	if _, err := head.server.CreateUser(ctx, &pb.CreateUserRequest{Name: "name", RequestId: "rid2"}); err == nil {
		t.Fatalf("expected username taken")
	}
}

func TestReplicationWorkflow_AndCommittedFlags(t *testing.T) {
	n1 := startTestNode(t, "node-1")
	n2 := startTestNode(t, "node-2")
	n3 := startTestNode(t, "node-3")
	configureChain(t, []*testNode{n1, n2, n3})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	u, err := n1.server.CreateUser(ctx, &pb.CreateUserRequest{Name: "u", RequestId: "rid-u"})
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	topic, err := n1.server.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "t", RequestId: "rid-t"})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	m1, err := n1.server.PostMessage(ctx, &pb.PostMessageRequest{UserId: u.GetId(), TopicId: topic.GetId(), Text: "m1", RequestId: "rid-m1"})
	if err != nil {
		t.Fatalf("PostMessage: %v", err)
	}

	m2, err := n1.server.LikeMessage(ctx, &pb.LikeMessageRequest{UserId: u.GetId(), TopicId: topic.GetId(), MessageId: m1.GetId(), RequestId: "rid-like"})
	if err != nil {
		t.Fatalf("LikeMessage: %v", err)
	}
	if m2.GetLikes() != 1 {
		t.Fatalf("expected 1 like, got %d", m2.GetLikes())
	}

	_, err = n1.server.UpdateMessage(ctx, &pb.UpdateMessageRequest{UserId: u.GetId(), TopicId: topic.GetId(), MessageId: m1.GetId(), Text: "m1-upd", RequestId: "rid-upd"})
	if err != nil {
		t.Fatalf("UpdateMessage: %v", err)
	}

	if _, err := n1.server.DeleteMessage(ctx, &pb.DeleteMessageRequest{UserId: u.GetId(), TopicId: topic.GetId(), MessageId: m1.GetId(), RequestId: "rid-del"}); err != nil {
		t.Fatalf("DeleteMessage: %v", err)
	}

	// Tail reads should include committed topic, and messages should be empty after delete.
	eventually(t, 2*time.Second, func() bool {
		n3.server.lock.RLock()
		lc := n3.server.lastCommittedSeq
		n3.server.lock.RUnlock()
		return lc >= 6
	})

	topics, err := n3.server.ListTopics(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("ListTopics (tail): %v", err)
	}
	if len(topics.GetTopics()) != 1 {
		t.Fatalf("expected 1 topic at tail, got %d", len(topics.GetTopics()))
	}

	msgs, err := n3.server.GetMessages(ctx, &pb.GetMessagesRequest{TopicId: topic.GetId(), FromMessageId: 1, Limit: 10})
	if err != nil {
		// topic still exists; should not error
		t.Fatalf("GetMessages (tail): %v", err)
	}
	if got := len(msgs.GetMessages()); got != 0 {
		t.Fatalf("expected 0 messages after delete, got %d", got)
	}
}

func TestUpdateAuthorizationAndValidationErrors(t *testing.T) {
	head := startTestNode(t, "node-1")
	tail := startTestNode(t, "node-2")
	configureChain(t, []*testNode{head, tail})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if _, err := head.server.CreateUser(ctx, &pb.CreateUserRequest{Name: "", RequestId: "rid"}); err == nil {
		t.Fatalf("expected invalid name error")
	}
	if _, err := head.server.CreateUser(ctx, &pb.CreateUserRequest{Name: "u", RequestId: ""}); err == nil {
		t.Fatalf("expected missing request_id")
	}

	u, err := head.server.CreateUser(ctx, &pb.CreateUserRequest{Name: "u", RequestId: "rid-u"})
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	topic, err := head.server.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "t", RequestId: "rid-t"})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	msg, err := head.server.PostMessage(ctx, &pb.PostMessageRequest{UserId: u.GetId(), TopicId: topic.GetId(), Text: "x", RequestId: "rid-m"})
	if err != nil {
		t.Fatalf("PostMessage: %v", err)
	}

	if _, err := head.server.UpdateMessage(ctx, &pb.UpdateMessageRequest{UserId: 999, TopicId: topic.GetId(), MessageId: msg.GetId(), Text: "y", RequestId: "rid-upd"}); err == nil {
		t.Fatalf("expected non-existent user error")
	}
	if _, err := head.server.UpdateMessage(ctx, &pb.UpdateMessageRequest{UserId: u.GetId() + 1, TopicId: topic.GetId(), MessageId: msg.GetId(), Text: "y", RequestId: "rid-upd2"}); err == nil {
		t.Fatalf("expected author mismatch error")
	}
}

func TestGetMessagesFiltering_FromAndLimit(t *testing.T) {
	head := startTestNode(t, "node-1")
	tail := startTestNode(t, "node-2")
	configureChain(t, []*testNode{head, tail})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	u, err := head.server.CreateUser(ctx, &pb.CreateUserRequest{Name: "u", RequestId: "rid-u"})
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	topic, err := head.server.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "t", RequestId: "rid-t"})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	m1, err := head.server.PostMessage(ctx, &pb.PostMessageRequest{UserId: u.GetId(), TopicId: topic.GetId(), Text: "1", RequestId: "rid-m1"})
	if err != nil {
		t.Fatalf("PostMessage m1: %v", err)
	}
	m2, err := head.server.PostMessage(ctx, &pb.PostMessageRequest{UserId: u.GetId(), TopicId: topic.GetId(), Text: "2", RequestId: "rid-m2"})
	if err != nil {
		t.Fatalf("PostMessage m2: %v", err)
	}
	if _, err := head.server.PostMessage(ctx, &pb.PostMessageRequest{UserId: u.GetId(), TopicId: topic.GetId(), Text: "3", RequestId: "rid-m3"}); err != nil {
		t.Fatalf("PostMessage m3: %v", err)
	}

	resp, err := tail.server.GetMessages(ctx, &pb.GetMessagesRequest{TopicId: topic.GetId(), FromMessageId: m2.GetId(), Limit: 1})
	if err != nil {
		t.Fatalf("GetMessages: %v", err)
	}
	if len(resp.GetMessages()) != 1 {
		t.Fatalf("expected 1 message, got %d", len(resp.GetMessages()))
	}
	if resp.GetMessages()[0].GetId() != m2.GetId() {
		t.Fatalf("expected message id %d", m2.GetId())
	}
	if resp.GetMessages()[0].GetId() == m1.GetId() {
		t.Fatalf("filtering failed")
	}
}

func TestSequenceValidation_RejectSkips(t *testing.T) {
	n := startTestNode(t, "node-1")
	// default: ready, standalone
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// First apply seq=2 should fail (expects 1)
	_, err := n.server.PropagateEntry(ctx, &pb.LogEntry{SequenceNumber: 2, Op: pb.OpType_OP_CREATE_TOPIC, Topic: &pb.Topic{Id: 1, Name: "t"}})
	if err == nil {
		t.Fatalf("expected out-of-order error")
	}
}

func TestSubscriptionWithBacklog_SendsCommittedPosts(t *testing.T) {
	head := startTestNode(t, "node-1")
	mid := startTestNode(t, "node-2")
	tail := startTestNode(t, "node-3")
	configureChain(t, []*testNode{head, mid, tail})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	u, err := head.server.CreateUser(ctx, &pb.CreateUserRequest{Name: "u", RequestId: "rid-u"})
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	topic, err := head.server.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "t", RequestId: "rid-t"})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	msg, err := head.server.PostMessage(ctx, &pb.PostMessageRequest{UserId: u.GetId(), TopicId: topic.GetId(), Text: "hello", RequestId: "rid-m"})
	if err != nil {
		t.Fatalf("PostMessage: %v", err)
	}

	// token from head (routing is not tested here)
	token, err := head.server.makeSubscribeToken(u.GetId(), []int64{topic.GetId()}, time.Now().Add(2*time.Minute).Unix())
	if err != nil {
		t.Fatalf("makeSubscribeToken: %v", err)
	}

	subCtx, subCancel := context.WithCancel(context.Background())
	stream := &fakeSubscribeStream{ctx: subCtx}

	done := make(chan error, 1)
	go func() {
		done <- tail.server.SubscribeTopic(&pb.SubscribeTopicRequest{UserId: u.GetId(), TopicId: []int64{topic.GetId()}, FromMessageId: msg.GetId(), SubscribeToken: token}, stream)
	}()

	eventually(t, 2*time.Second, func() bool {
		events := stream.snapshot()
		if len(events) == 0 {
			return false
		}
		return events[0].GetOp() == pb.OpType_OP_POST && events[0].GetMessage().GetId() == msg.GetId()
	}, "expected backlog POST event")

	subCancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("SubscribeTopic returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("SubscribeTopic did not exit")
	}

	eventually(t, 2*time.Second, func() bool {
		return tail.server.subCount.Load() == 0
	}, "expected subCount to return to 0")
}
