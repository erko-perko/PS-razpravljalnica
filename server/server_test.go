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

// Test preveri ustvarjanje in validacijo subscribe tokenov v serverju.
// Najprej potrdi, da veljaven token vrne pričakovane topic ID-je.
// Nato preveri napake za napačnega uporabnika, pretečen token in prirejen podpis.
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

	if _, err := s.validateSubscribeToken(tok+"x", 42); err == nil {
		t.Fatalf("expected invalid token error")
	}
}

// Test preveri logično validacijo zaporednih številk pri apliciranju loga.
// Dovoljuje ponovitev iste seq (idempotentnost), vendar zavrne preskok v prihodnost.
// S tem ščiti replikacijo pred izpuščenimi vnosi.
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

// Test preveri timeout mehanizem čakanja na ack.
// Pripravi pričakovanje za določeno seq številko in nato čaka prekratek čas.
// Rezultat mora biti false, kar pomeni da ack ni prišel pravočasno.
func TestWaitAckTimeout(t *testing.T) {
	s := newMessageBoardServer("n", "127.0.0.1:1", "", "secret")
	s.prepareAck(7)
	if ok := s.waitAck(7, 50*time.Millisecond); ok {
		t.Fatalf("expected timeout")
	}
}

// Test preveri pravilno prepoznavanje head/middle/tail v verigi treh node-ov.
// Najprej konfigurira chain na vseh node-ih.
// Nato potrdi, da je prvi samo head, zadnji samo tail, srednji pa nobeno od obeh.
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

// Test preveri pravilila usmerjanja operacij glede na vlogo node-a.
// Write operacije morajo biti dovoljene samo na headu, read operacije pa samo na tailu.
// Zato poskusi write na middle node-u in read na headu ter pričakuje napako.
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

// Test preveri obnašanje enonodnega clustra, kjer je isti node hkrati head in tail.
// Izvede ustvarjanje uporabnika, teme in objavo sporočila ter pričakuje takojšnje commit stanje.
// Nato izvede read klice in preveri, da so podatki vidni na istem node-u.
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

// Test preveri idempotentnost write operacij prek RequestId in unikatnost uporabniškega imena.
// Najprej ustvari uporabnika in ponovi isti request, kjer pričakuje isti ID.
// Nato poskusi ustvariti uporabnika z istim imenom, a drugim RequestId, kar mora biti zavrnjeno.
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

// Test preveri celoten replikacijski workflow in pravilno nastavljanje committed zastavic.
// Izvede več write operacij na headu (user, topic, post, like, update, delete).
// Nato na tailu počaka na napredovanje lastCommittedSeq in preveri, da je topic viden ter da po brisanju sporočil ni več.
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
		t.Fatalf("GetMessages (tail): %v", err)
	}
	if got := len(msgs.GetMessages()); got != 0 {
		t.Fatalf("expected 0 messages after delete, got %d", got)
	}
}

// Test preveri validacijo vhodnih podatkov ter avtorizacijo pri urejanju sporočil.
// Najprej preveri, da so neveljavni parametri (prazen username, prazen request_id) zavrnjeni.
// Nato preveri, da update zavrne neobstoječega uporabnika in uporabnika, ki ni avtor sporočila.
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

// Test preveri pravilno filtriranje in omejevanje rezultatov pri branju sporočil.
// Najprej ustvari tri sporočila v temi in nato bere od določenega ID naprej z limitom 1.
// Potrdi, da dobi natanko eno sporočilo in da je to pričakovani ID.
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

// Test preveri, da replikacija zavrne log entry, ki preskoči pričakovano seq številko.
// Pošlje PropagateEntry s seq=2 na svež node, ki pričakuje seq=1.
// Rezultat mora biti napaka, kar varuje konsistentnost loga.
func TestSequenceValidation_RejectSkips(t *testing.T) {
	n := startTestNode(t, "node-1")
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	_, err := n.server.PropagateEntry(ctx, &pb.LogEntry{SequenceNumber: 2, Op: pb.OpType_OP_CREATE_TOPIC, Topic: &pb.Topic{Id: 1, Name: "t"}})
	if err == nil {
		t.Fatalf("expected out-of-order error")
	}
}

// Test preveri, da SubscribeTopic pošlje backlog že commit-anih dogodkov.
// Najprej objavi sporočilo in šele nato zažene subscription na tailu s FromMessageId nastavljenim na objavljeno sporočilo.
// Nato pričakuje, da stream dobi POST event za isti message ID.
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

// Test preveri, da noteAck pravilno napreduje committed mejo tudi pri out-of-order ackih.
// Najprej aplicira tri log entry-je in nato kliče noteAck v vrstnem redu 3,1,2.
// Na koncu mora lastCommittedSeq skočiti na 3 in vse pripadajoče entitete morajo imeti Committed=true.
func TestNoteAck_OutOfOrderAdvancesCommitAndCommittedFlags(t *testing.T) {
	s := newMessageBoardServer("n", "127.0.0.1:1", "", "test-secret")

	s.lock.Lock()

	e1 := &pb.LogEntry{
		SequenceNumber: 1,
		RequestId:      "rid-u",
		Op:             pb.OpType_OP_CREATE_USER,
		User:           &pb.User{Id: 1, Name: "u1"},
	}
	if err := s.applyEntryLocked(e1); err != nil {
		s.lock.Unlock()
		t.Fatalf("apply e1: %v", err)
	}

	e2 := &pb.LogEntry{
		SequenceNumber: 2,
		RequestId:      "rid-t",
		Op:             pb.OpType_OP_CREATE_TOPIC,
		Topic:          &pb.Topic{Id: 1, Name: "t1"},
	}
	if err := s.applyEntryLocked(e2); err != nil {
		s.lock.Unlock()
		t.Fatalf("apply e2: %v", err)
	}

	e3 := &pb.LogEntry{
		SequenceNumber: 3,
		RequestId:      "rid-m",
		Op:             pb.OpType_OP_POST,
		Message:        &pb.Message{Id: 1, TopicId: 1, UserId: 1, Text: "hello"},
	}
	if err := s.applyEntryLocked(e3); err != nil {
		s.lock.Unlock()
		t.Fatalf("apply e3: %v", err)
	}

	if s.users[1].Committed {
		s.lock.Unlock()
		t.Fatalf("expected user not committed before ack")
	}
	if s.topics[1].Committed {
		s.lock.Unlock()
		t.Fatalf("expected topic not committed before ack")
	}
	if s.messages[1][1].Committed {
		s.lock.Unlock()
		t.Fatalf("expected message not committed before ack")
	}

	s.noteAck(3)
	if s.lastCommittedSeq != 0 {
		s.lock.Unlock()
		t.Fatalf("expected committedSeq still 0 (hole at 1,2), got %d", s.lastCommittedSeq)
	}

	s.noteAck(1)
	if s.lastCommittedSeq != 1 {
		s.lock.Unlock()
		t.Fatalf("expected committedSeq=1, got %d", s.lastCommittedSeq)
	}

	s.noteAck(2)
	if s.lastCommittedSeq != 3 {
		s.lock.Unlock()
		t.Fatalf("expected committedSeq jump to 3, got %d", s.lastCommittedSeq)
	}

	if !s.users[1].Committed {
		s.lock.Unlock()
		t.Fatalf("expected user committed after ack")
	}
	if !s.topics[1].Committed {
		s.lock.Unlock()
		t.Fatalf("expected topic committed after ack")
	}
	if !s.messages[1][1].Committed {
		s.lock.Unlock()
		t.Fatalf("expected message committed after ack")
	}

	s.lock.Unlock()
}

// Test preveri end-to-end, da subscription na tailu prejme POST event po uspešni objavi na headu.
// Najprej ustvari userja in temo, nato odpre SubscribeTopic stream in objavi novo sporočilo.
// V roku timeouta mora stream vsebovati POST event z ID-jem objavljenega sporočila.
func TestSubscribeTopic_ReceivesPostAfterWriteWorkflow(t *testing.T) {
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

	token, err := head.server.makeSubscribeToken(u.GetId(), []int64{topic.GetId()}, time.Now().Add(2*time.Minute).Unix())
	if err != nil {
		t.Fatalf("makeSubscribeToken: %v", err)
	}

	subCtx, subCancel := context.WithCancel(context.Background())
	stream := &fakeSubscribeStream{ctx: subCtx}
	done := make(chan error, 1)
	go func() {
		done <- tail.server.SubscribeTopic(&pb.SubscribeTopicRequest{
			UserId:         u.GetId(),
			TopicId:        []int64{topic.GetId()},
			FromMessageId:  0,
			SubscribeToken: token,
		}, stream)
	}()

	msg, err := head.server.PostMessage(ctx, &pb.PostMessageRequest{
		UserId:    u.GetId(),
		TopicId:   topic.GetId(),
		Text:      "hello",
		RequestId: "rid-m1",
	})
	if err != nil {
		subCancel()
		t.Fatalf("PostMessage: %v", err)
	}

	eventually(t, 2*time.Second, func() bool {
		evs := stream.snapshot()
		if len(evs) == 0 {
			return false
		}
		for _, ev := range evs {
			if ev.GetOp() == pb.OpType_OP_POST && ev.GetMessage() != nil && ev.GetMessage().GetId() == msg.GetId() {
				return true
			}
		}
		return false
	}, "expected POST event for msg id=%d", msg.GetId())

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

// Test preveri, da PropagateEntry podpira idempotentno ponovno pošiljanje istega entry-ja.
// Najprej pošlje seq=1 in pričakuje uspeh, nato pošlje identičen entry še enkrat in pričakuje ponovno uspeh.
// Na koncu potrdi, da je lastAppliedSeq ostal na 1 in da ustvarjena tema obstaja.
func TestPropagateEntry_IdempotentDuplicateSequence(t *testing.T) {
	n := startTestNode(t, "node-1")
	configureChain(t, []*testNode{n})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	entry := &pb.LogEntry{
		SequenceNumber: 1,
		RequestId:      "rid-t",
		Op:             pb.OpType_OP_CREATE_TOPIC,
		Topic:          &pb.Topic{Id: 1, Name: "t"},
	}

	if _, err := n.server.PropagateEntry(ctx, entry); err != nil {
		t.Fatalf("first PropagateEntry: %v", err)
	}

	if _, err := n.server.PropagateEntry(ctx, entry); err != nil {
		t.Fatalf("second PropagateEntry (duplicate) should succeed: %v", err)
	}

	n.server.lock.RLock()
	defer n.server.lock.RUnlock()

	if n.server.lastAppliedSeq != 1 {
		t.Fatalf("expected lastAppliedSeq=1, got %d", n.server.lastAppliedSeq)
	}
	if _, ok := n.server.topics[1]; !ok {
		t.Fatalf("expected topic to exist after idempotent duplicate")
	}
	if _, ok := n.server.logEntries[1]; !ok {
		t.Fatalf("expected logEntries[1] to exist")
	}
}

// Test preveri, da nov node, ki se pozno pridruži verigi, dobi prekopiran log in stanje od predecessorja.
// Najprej se na obstoječi 2-node verigi izvedejo write operacije in tail aplicira vse entry-je.
// Nato se doda tretji node in pričakuje, da ulovi lastAppliedSeq ter vsebuje userja, topic in sporočilo.
func TestLateJoinCatchUp_CopiesLogAndState(t *testing.T) {
	n1 := startTestNode(t, "node-1")
	n2 := startTestNode(t, "node-2")
	configureChain(t, []*testNode{n1, n2})

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
	msg, err := n1.server.PostMessage(ctx, &pb.PostMessageRequest{
		UserId:    u.GetId(),
		TopicId:   topic.GetId(),
		Text:      "hello",
		RequestId: "rid-m1",
	})
	if err != nil {
		t.Fatalf("PostMessage: %v", err)
	}

	eventually(t, 2*time.Second, func() bool {
		n2.server.lock.RLock()
		defer n2.server.lock.RUnlock()
		return n2.server.lastAppliedSeq >= 3
	}, "expected tail to apply entries")

	n3 := startTestNode(t, "node-3")
	configureChain(t, []*testNode{n1, n2, n3})

	eventually(t, 2*time.Second, func() bool {
		n3.server.lock.RLock()
		defer n3.server.lock.RUnlock()
		return n3.server.lastAppliedSeq >= n2.server.lastAppliedSeq
	}, "expected n3 to catch up to predecessor")

	n3.server.lock.RLock()
	defer n3.server.lock.RUnlock()

	if _, ok := n3.server.users[u.GetId()]; !ok {
		t.Fatalf("n3 missing user %d after catch-up", u.GetId())
	}
	if _, ok := n3.server.topics[topic.GetId()]; !ok {
		t.Fatalf("n3 missing topic %d after catch-up", topic.GetId())
	}
	tm, ok := n3.server.messages[topic.GetId()]
	if !ok {
		t.Fatalf("n3 missing messages map for topic %d", topic.GetId())
	}
	if _, ok := tm[msg.GetId()]; !ok {
		t.Fatalf("n3 missing message %d after catch-up", msg.GetId())
	}
}

// Test preveri varovalko, ki prepreči write operacije, ko head še ni pripravljen.
// Najprej ročno nastavi headReady na false in potrdi, da CreateUser vrne napako.
// Nato preklopi headReady nazaj na true in pričakuje, da CreateUser uspe.
func TestRequireHeadReady_BlocksWritesWhenFalse(t *testing.T) {
	n := startTestNode(t, "node-1")
	configureChain(t, []*testNode{n})

	n.server.headReady.Store(false)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	if _, err := n.server.CreateUser(ctx, &pb.CreateUserRequest{Name: "u", RequestId: "rid"}); err == nil {
		t.Fatalf("expected error when headReady=false")
	}

	n.server.headReady.Store(true)
	if _, err := n.server.CreateUser(ctx, &pb.CreateUserRequest{Name: "u", RequestId: "rid2"}); err != nil {
		t.Fatalf("expected CreateUser to succeed when headReady=true, got %v", err)
	}
}
