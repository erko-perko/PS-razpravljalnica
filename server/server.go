package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pb "razpravljalnica/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type subscription struct {
	id      int64
	userID  int64
	topics  map[int64]struct{}
	channel chan *pb.MessageEvent
}

type tokenPayload struct {
	UserID int64   `json:"user_id"`
	Topics []int64 `json:"topics"`
	Exp    int64   `json:"exp"`
}

type messageBoardServer struct {
	pb.UnimplementedMessageBoardServer

	lock sync.RWMutex

	// In-memory DB
	users         map[int64]*pb.User
	userNameToID  map[string]int64
	topics        map[int64]*pb.Topic
	messages      map[int64]map[int64]*pb.Message // topicID -> messageID -> Message
	nextUserID    int64
	nextTopicID   int64
	nextMessageID map[int64]int64

	// Subscription
	subscribers map[int64]*subscription
	nextSubID   int64
	postEvents  map[int64]map[int64]*pb.MessageEvent // topicID -> messageID -> event backlog

	// Chain state
	nodeID      string
	address     string
	controlAddr string

	chain       []*pb.NodeInfo
	headAddress string
	tailAddress string

	predecessor *pb.NodeInfo
	successor   *pb.NodeInfo

	successorConn   *grpc.ClientConn
	successorClient pb.MessageBoardClient

	// Sequence & log
	nextSeq          int64
	lastAppliedSeq   int64
	lastCommittedSeq int64

	logEntries map[int64]*pb.LogEntry // seq -> entry (all applied, committed or not)

	// Idempotency: request_id -> seq
	requestToSeq map[string]int64

	// Ack waiting at head: seq -> chan
	ackLock     sync.Mutex
	ackChannels map[int64]chan struct{}

	// Load balancing for subscriptions at head
	nodeSubCount map[string]int64

	// Token signing
	tokenSecret []byte

	// Stop channels
	stopCh chan struct{}

	// Head readiness gate (when a node becomes head after reconfiguration)
	headReady atomic.Bool
}

func newMessageBoardServer(nodeID, address, controlAddr, tokenSecret string) *messageBoardServer {
	s := &messageBoardServer{
		users:         make(map[int64]*pb.User),
		userNameToID:  make(map[string]int64),
		topics:        make(map[int64]*pb.Topic),
		messages:      make(map[int64]map[int64]*pb.Message),
		nextMessageID: make(map[int64]int64),
		subscribers:   make(map[int64]*subscription),
		postEvents:    make(map[int64]map[int64]*pb.MessageEvent),
		chain:         make([]*pb.NodeInfo, 0),
		nodeID:        nodeID,
		address:       address,
		controlAddr:   controlAddr,
		headAddress:   address,
		tailAddress:   address,
		logEntries:    make(map[int64]*pb.LogEntry),
		requestToSeq:  make(map[string]int64),
		ackChannels:   make(map[int64]chan struct{}),
		nodeSubCount:  make(map[string]int64),
		tokenSecret:   []byte(tokenSecret),
		stopCh:        make(chan struct{}),
	}
	// If single node (initially), it's both head and tail, so head is ready.
	s.headReady.Store(true)
	return s
}

func (s *messageBoardServer) isHeadLocked() bool {
	return s.address == s.headAddress
}

func (s *messageBoardServer) isTailLocked() bool {
	return s.address == s.tailAddress
}

func (s *messageBoardServer) isHead() bool {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.isHeadLocked()
}

func (s *messageBoardServer) isTail() bool {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.isTailLocked()
}

func (s *messageBoardServer) requireHeadReady() error {
	if !s.headReady.Load() {
		return fmt.Errorf("head is not ready yet (catch-up in progress)")
	}
	return nil
}

func (s *messageBoardServer) allocateSeqLocked() int64 {
	s.nextSeq++
	return s.nextSeq
}

func (s *messageBoardServer) prepareAck(seq int64) {
	s.ackLock.Lock()
	defer s.ackLock.Unlock()
	if _, ok := s.ackChannels[seq]; ok {
		return
	}
	s.ackChannels[seq] = make(chan struct{}, 1)
}

func (s *messageBoardServer) waitAck(seq int64, timeout time.Duration) bool {
	s.ackLock.Lock()
	ch, ok := s.ackChannels[seq]
	s.ackLock.Unlock()
	if !ok {
		return false
	}

	select {
	case <-ch:
		s.ackLock.Lock()
		delete(s.ackChannels, seq)
		s.ackLock.Unlock()
		return true
	case <-time.After(timeout):
		s.ackLock.Lock()
		delete(s.ackChannels, seq)
		s.ackLock.Unlock()
		return false
	}
}

func (s *messageBoardServer) signalAck(seq int64) {
	s.ackLock.Lock()
	ch, ok := s.ackChannels[seq]
	s.ackLock.Unlock()
	if !ok {
		return
	}
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (s *messageBoardServer) validateAndAdvanceSeqLocked(seq int64) error {
	// Accept duplicates (idempotent propagation); ignore if already applied.
	if seq <= s.lastAppliedSeq {
		return nil
	}
	// Must be exactly next
	if seq != s.lastAppliedSeq+1 {
		return fmt.Errorf("out of order: expected %d got %d", s.lastAppliedSeq+1, seq)
	}
	s.lastAppliedSeq = seq
	return nil
}

func (s *messageBoardServer) applyEntryLocked(entry *pb.LogEntry) error {
	seq := entry.GetSequenceNumber()

	// If already applied, do nothing (idempotent).
	if _, ok := s.logEntries[seq]; ok {
		return nil
	}

	// Ensure order
	if err := s.validateAndAdvanceSeqLocked(seq); err != nil {
		return err
	}

	// Store in log
	s.logEntries[seq] = entry

	switch entry.GetOp() {
	case pb.OpType_OP_CREATE_USER:
		u := entry.GetUser()
		if u == nil {
			return fmt.Errorf("missing user in log entry")
		}
		// Enforce username uniqueness with map (idempotent safe)
		if existingID, ok := s.userNameToID[u.GetName()]; ok {
			if existingID != u.GetId() {
				return fmt.Errorf("username taken")
			}
		} else {
			s.userNameToID[u.GetName()] = u.GetId()
		}
		if u.GetId() > s.nextUserID {
			s.nextUserID = u.GetId()
		}
		user := &pb.User{
			Id:             u.GetId(),
			Name:           u.GetName(),
			SequenceNumber: seq,
			Committed:      false,
		}
		s.users[user.Id] = user

	case pb.OpType_OP_CREATE_TOPIC:
		t := entry.GetTopic()
		if t == nil {
			return fmt.Errorf("missing topic in log entry")
		}
		if t.GetId() > s.nextTopicID {
			s.nextTopicID = t.GetId()
		}
		topic := &pb.Topic{
			Id:             t.GetId(),
			Name:           t.GetName(),
			SequenceNumber: seq,
			Committed:      false,
		}
		s.topics[topic.Id] = topic

		if _, ok := s.messages[topic.Id]; !ok {
			s.messages[topic.Id] = make(map[int64]*pb.Message)
		}
		if _, ok := s.nextMessageID[topic.Id]; !ok {
			s.nextMessageID[topic.Id] = 0
		}
		if _, ok := s.postEvents[topic.Id]; !ok {
			s.postEvents[topic.Id] = make(map[int64]*pb.MessageEvent)
		}

	case pb.OpType_OP_POST:
		m := entry.GetMessage()
		if m == nil {
			return fmt.Errorf("missing message in log entry")
		}
		topicID := m.GetTopicId()
		if _, ok := s.messages[topicID]; !ok {
			s.messages[topicID] = make(map[int64]*pb.Message)
		}
		if m.GetId() > s.nextMessageID[topicID] {
			s.nextMessageID[topicID] = m.GetId()
		}
		msg := &pb.Message{
			Id:             m.GetId(),
			TopicId:        m.GetTopicId(),
			UserId:         m.GetUserId(),
			Text:           m.GetText(),
			CreatedAt:      m.GetCreatedAt(),
			Likes:          m.GetLikes(),
			SequenceNumber: seq,
			Committed:      false,
		}
		s.messages[topicID][msg.Id] = msg

		ev := s.newMessageEventLocked(pb.OpType_OP_POST, msg, seq)
		if _, ok := s.postEvents[topicID]; !ok {
			s.postEvents[topicID] = make(map[int64]*pb.MessageEvent)
		}
		s.postEvents[topicID][msg.Id] = ev
		s.broadcastEventLocked(ev)

	case pb.OpType_OP_UPDATE:
		m := entry.GetMessage()
		if m == nil {
			return fmt.Errorf("missing message in log entry")
		}
		topicID := m.GetTopicId()
		tm, ok := s.messages[topicID]
		if !ok {
			return fmt.Errorf("topic %d does not exist", topicID)
		}
		ex, ok := tm[m.GetId()]
		if !ok {
			return fmt.Errorf("message %d does not exist", m.GetId())
		}
		ex.Text = m.GetText()
		ex.SequenceNumber = seq
		ex.Committed = false

		ev := s.newMessageEventLocked(pb.OpType_OP_UPDATE, ex, seq)
		s.broadcastEventLocked(ev)

	case pb.OpType_OP_DELETE:
		m := entry.GetMessage()
		if m == nil {
			return fmt.Errorf("missing message in log entry")
		}
		topicID := m.GetTopicId()
		tm, ok := s.messages[topicID]
		if !ok {
			return fmt.Errorf("topic %d does not exist", topicID)
		}
		ex, ok := tm[m.GetId()]
		if !ok {
			return fmt.Errorf("message %d does not exist", m.GetId())
		}
		ex.SequenceNumber = seq
		ev := s.newMessageEventLocked(pb.OpType_OP_DELETE, ex, seq)
		delete(tm, m.GetId())
		s.broadcastEventLocked(ev)

	case pb.OpType_OP_LIKE:
		m := entry.GetMessage()
		if m == nil {
			return fmt.Errorf("missing message in log entry")
		}
		topicID := m.GetTopicId()
		tm, ok := s.messages[topicID]
		if !ok {
			return fmt.Errorf("topic %d does not exist", topicID)
		}
		ex, ok := tm[m.GetId()]
		if !ok {
			return fmt.Errorf("message %d does not exist", m.GetId())
		}
		ex.Likes++
		ex.SequenceNumber = seq
		ex.Committed = false

		ev := s.newMessageEventLocked(pb.OpType_OP_LIKE, ex, seq)
		s.broadcastEventLocked(ev)

	default:
		return fmt.Errorf("unknown op %v", entry.GetOp())
	}

	// Track request_id -> seq for idempotency if present
	rid := strings.TrimSpace(entry.GetRequestId())
	if rid != "" {
		s.requestToSeq[rid] = seq
	}

	return nil
}

func (s *messageBoardServer) markCommittedLocked(seq int64) {
	if seq <= s.lastCommittedSeq {
		return
	}
	s.lastCommittedSeq = seq

	entry, ok := s.logEntries[seq]
	if !ok {
		return
	}

	switch entry.GetOp() {
	case pb.OpType_OP_CREATE_USER:
		u := entry.GetUser()
		if u != nil {
			if ex, ok := s.users[u.GetId()]; ok {
				ex.Committed = true
			}
		}
	case pb.OpType_OP_CREATE_TOPIC:
		t := entry.GetTopic()
		if t != nil {
			if ex, ok := s.topics[t.GetId()]; ok {
				ex.Committed = true
			}
		}
	case pb.OpType_OP_POST, pb.OpType_OP_UPDATE, pb.OpType_OP_DELETE, pb.OpType_OP_LIKE:
		m := entry.GetMessage()
		if m != nil {
			if tm, ok := s.messages[m.GetTopicId()]; ok {
				if ex, ok := tm[m.GetId()]; ok {
					ex.Committed = true
				}
			}
		}
	}
}

func (s *messageBoardServer) newMessageEventLocked(op pb.OpType, msg *pb.Message, seq int64) *pb.MessageEvent {
	return &pb.MessageEvent{
		SequenceNumber: seq,
		Op:             op,
		Message:        msg,
		EventAt:        timestamppb.Now(),
	}
}

func (s *messageBoardServer) broadcastEventLocked(event *pb.MessageEvent) {
	if event.GetMessage() == nil {
		return
	}
	topicID := event.GetMessage().GetTopicId()
	for _, sub := range s.subscribers {
		if _, ok := sub.topics[topicID]; !ok {
			continue
		}
		select {
		case sub.channel <- event:
		default:
		}
	}
}

func (s *messageBoardServer) removeSubscription(id int64) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if sub, ok := s.subscribers[id]; ok {
		delete(s.subscribers, id)
		close(sub.channel)
	}
}

func (s *messageBoardServer) getEntryByRequestIDLocked(requestID string) (*pb.LogEntry, bool) {
	seq, ok := s.requestToSeq[requestID]
	if !ok {
		return nil, false
	}
	entry, ok := s.logEntries[seq]
	return entry, ok
}

func (s *messageBoardServer) propagateToSuccessor(ctx context.Context, entry *pb.LogEntry) error {
	s.lock.RLock()
	client := s.successorClient
	s.lock.RUnlock()
	if client == nil {
		return nil
	}
	_, err := client.PropagateEntry(ctx, entry)
	return err
}

func (s *messageBoardServer) reportNodeFailure(nodeID string) {
	if strings.TrimSpace(s.controlAddr) == "" {
		return
	}
	conn, err := grpc.Dial(s.controlAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return
	}
	defer conn.Close()

	client := pb.NewControlPlaneClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _ = client.Leave(ctx, &pb.LeaveRequest{NodeId: nodeID})
}

func (s *messageBoardServer) propagateWithRetry(ctx context.Context, entry *pb.LogEntry, maxRetries int) error {
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		err := s.propagateToSuccessor(ctx, entry)
		if err == nil {
			return nil
		}
		lastErr = err

		s.lock.RLock()
		succ := s.successor
		s.lock.RUnlock()
		if succ != nil {
			log.Printf("[%s] propagate failed (%d/%d) to succ=%s: %v", s.nodeID, i+1, maxRetries, succ.GetNodeId(), err)
			// Best-effort failure report (helps CP remove dead node and rewire chain)
			s.reportNodeFailure(succ.GetNodeId())
		} else {
			log.Printf("[%s] propagate failed (%d/%d) no successor: %v", s.nodeID, i+1, maxRetries, err)
		}

		// backoff + allow chain reconfiguration to arrive
		backoff := time.Duration(1<<uint(i)) * 200 * time.Millisecond
		if backoff > 2*time.Second {
			backoff = 2 * time.Second
		}
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return fmt.Errorf("propagation failed after %d retries: %v", maxRetries, lastErr)
}

func (s *messageBoardServer) sendAckToPredecessor(ctx context.Context, seq int64) {
	s.lock.RLock()
	pred := s.predecessor
	s.lock.RUnlock()
	if pred == nil {
		return
	}
	conn, err := grpc.Dial(pred.GetAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return
	}
	defer conn.Close()
	client := pb.NewMessageBoardClient(conn)
	_, _ = client.Ack(ctx, &pb.AckRequest{SequenceNumber: seq})
}

func (s *messageBoardServer) catchUpFromPredecessor() {
	s.lock.RLock()
	pred := s.predecessor
	last := s.lastAppliedSeq
	s.lock.RUnlock()

	if pred == nil {
		// No predecessor -> if we're head, we can become ready.
		s.headReady.Store(true)
		return
	}

	conn, err := grpc.Dial(pred.GetAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("[%s] catch-up dial failed: %v", s.nodeID, err)
		return
	}
	defer conn.Close()
	client := pb.NewMessageBoardClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.SyncFrom(ctx, &pb.SyncFromRequest{FromSeqExclusive: last})
	if err != nil {
		log.Printf("[%s] catch-up SyncFrom failed: %v", s.nodeID, err)
		return
	}

	s.lock.Lock()
	for _, e := range resp.GetEntries() {
		if err := s.applyEntryLocked(e); err != nil {
			log.Printf("[%s] catch-up apply failed: %v", s.nodeID, err)
			s.lock.Unlock()
			return
		}
	}
	isHead := s.isHeadLocked()
	s.lock.Unlock()

	if isHead {
		// After catch-up, head is ready to accept writes.
		s.headReady.Store(true)
	}

	log.Printf("[%s] catch-up complete up to %d", s.nodeID, s.lastAppliedSeq)
}

func (s *messageBoardServer) heartbeatLoop() {
	if strings.TrimSpace(s.controlAddr) == "" {
		return
	}

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.lock.RLock()
			last := s.lastAppliedSeq
			addr := s.address
			nodeID := s.nodeID
			s.lock.RUnlock()

			conn, err := grpc.Dial(s.controlAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				continue
			}
			client := pb.NewControlPlaneClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			_, _ = client.Heartbeat(ctx, &pb.HeartbeatRequest{
				NodeId:         nodeID,
				Address:        addr,
				LastAppliedSeq: last,
			})
			cancel()
			_ = conn.Close()
		}
	}
}

func (s *messageBoardServer) joinControlPlane() {
	if strings.TrimSpace(s.controlAddr) == "" {
		return
	}
	conn, err := grpc.Dial(s.controlAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("[%s] join dial failed: %v", s.nodeID, err)
		return
	}
	defer conn.Close()

	client := pb.NewControlPlaneClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := client.Join(ctx, &pb.JoinRequest{NodeId: s.nodeID, Address: s.address})
	if err != nil {
		log.Printf("[%s] join failed: %v", s.nodeID, err)
		return
	}

	_ = s.applyChainConfig(resp.GetChain())
}

func (s *messageBoardServer) applyChainConfig(nodes []*pb.NodeInfo) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	oldHead := s.headAddress
	oldPredAddr := ""
	if s.predecessor != nil {
		oldPredAddr = s.predecessor.GetAddress()
	}

	s.chain = nodes
	if len(nodes) == 0 {
		// standalone
		s.headAddress = s.address
		s.tailAddress = s.address
		s.predecessor = nil
		s.successor = nil
		s.successorClient = nil
		if s.successorConn != nil {
			_ = s.successorConn.Close()
			s.successorConn = nil
		}
		s.headReady.Store(true)
		return nil
	}

	s.headAddress = nodes[0].GetAddress()
	s.tailAddress = nodes[len(nodes)-1].GetAddress()

	// Find index
	myIndex := -1
	for i, n := range nodes {
		if n.GetNodeId() == s.nodeID {
			myIndex = i
			break
		}
	}
	if myIndex == -1 {
		// Not in chain anymore - treat as standalone
		s.predecessor = nil
		s.successor = nil
		s.headAddress = s.address
		s.tailAddress = s.address
		s.headReady.Store(true)
		return nil
	}

	var newPred *pb.NodeInfo
	if myIndex > 0 {
		newPred = nodes[myIndex-1]
	}
	var newSucc *pb.NodeInfo
	if myIndex < len(nodes)-1 {
		newSucc = nodes[myIndex+1]
	}

	predChanged := (s.predecessor == nil && newPred != nil) ||
		(s.predecessor != nil && newPred == nil) ||
		(s.predecessor != nil && newPred != nil && s.predecessor.GetAddress() != newPred.GetAddress())

	s.predecessor = newPred
	s.successor = newSucc

	// Reset successor client connection if changed
	if s.successorConn != nil {
		_ = s.successorConn.Close()
		s.successorConn = nil
		s.successorClient = nil
	}

	if s.successor != nil {
		conn, err := grpc.Dial(s.successor.GetAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("failed to dial successor: %v", err)
		}
		s.successorConn = conn
		s.successorClient = pb.NewMessageBoardClient(conn)
	}

	// Ensure sub count map for LB at head
	for _, n := range nodes {
		if _, ok := s.nodeSubCount[n.GetNodeId()]; !ok {
			s.nodeSubCount[n.GetNodeId()] = 0
		}
	}

	// Head readiness logic:
	// If we became head, we must catch up first (if we have predecessor).
	becameHead := (oldHead != s.headAddress && s.isHeadLocked()) || (oldHead == "" && s.isHeadLocked())
	if becameHead {
		s.headReady.Store(false)
	}
	// If predecessor changed, catch up.
	needCatchUp := predChanged
	if becameHead && s.predecessor != nil {
		needCatchUp = true
	}
	if becameHead && s.predecessor == nil {
		// Head with no predecessor -> immediately ready.
		s.headReady.Store(true)
	}

	// unlock scope ends here; do catch-up async outside lock
	newPredAddr := ""
	if s.predecessor != nil {
		newPredAddr = s.predecessor.GetAddress()
	}

	log.Printf("[%s] chain applied: head=%s tail=%s pred=%v succ=%v predChanged=%v becameHead=%v oldPred=%s newPred=%s",
		s.nodeID, s.headAddress, s.tailAddress,
		s.predecessor != nil, s.successor != nil, predChanged, becameHead, oldPredAddr, newPredAddr)

	if needCatchUp {
		go s.catchUpFromPredecessor()
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////
// External API (head writes)
////////////////////////////////////////////////////////////////////////////////

func (s *messageBoardServer) CreateUser(ctx context.Context, request *pb.CreateUserRequest) (*pb.User, error) {
	if !s.isHead() {
		return nil, fmt.Errorf("write operations only allowed at head node")
	}
	if err := s.requireHeadReady(); err != nil {
		return nil, err
	}
	name := strings.TrimSpace(request.GetName())
	if name == "" {
		return nil, fmt.Errorf("name not valid")
	}
	rid := strings.TrimSpace(request.GetRequestId())
	if rid == "" {
		return nil, fmt.Errorf("request_id required")
	}

	s.lock.Lock()
	if entry, ok := s.getEntryByRequestIDLocked(rid); ok {
		u := entry.GetUser()
		if u == nil {
			s.lock.Unlock()
			return nil, fmt.Errorf("stored entry missing user")
		}
		resp := s.users[u.GetId()]
		s.lock.Unlock()
		return resp, nil
	}
	if _, ok := s.userNameToID[name]; ok {
		s.lock.Unlock()
		return nil, fmt.Errorf("username taken")
	}

	s.nextUserID++
	id := s.nextUserID
	seq := s.allocateSeqLocked()

	user := &pb.User{Id: id, Name: name, SequenceNumber: seq, Committed: false}
	entry := &pb.LogEntry{
		SequenceNumber: seq,
		RequestId:      rid,
		Op:             pb.OpType_OP_CREATE_USER,
		User:           user,
	}

	if err := s.applyEntryLocked(entry); err != nil {
		s.lock.Unlock()
		return nil, err
	}
	s.prepareAck(seq)
	s.lock.Unlock()

	if err := s.propagateWithRetry(ctx, entry, 5); err != nil {
		return nil, err
	}
	if !s.waitAck(seq, 5*time.Second) {
		return nil, fmt.Errorf("timeout waiting for ack")
	}

	s.lock.Lock()
	s.markCommittedLocked(seq)
	resp := s.users[id]
	s.lock.Unlock()
	return resp, nil
}

func (s *messageBoardServer) CreateTopic(ctx context.Context, request *pb.CreateTopicRequest) (*pb.Topic, error) {
	if !s.isHead() {
		return nil, fmt.Errorf("write operations only allowed at head node")
	}
	if err := s.requireHeadReady(); err != nil {
		return nil, err
	}
	name := strings.TrimSpace(request.GetName())
	if name == "" {
		return nil, fmt.Errorf("topic name not valid")
	}
	rid := strings.TrimSpace(request.GetRequestId())
	if rid == "" {
		return nil, fmt.Errorf("request_id required")
	}

	s.lock.Lock()
	if entry, ok := s.getEntryByRequestIDLocked(rid); ok {
		t := entry.GetTopic()
		if t == nil {
			s.lock.Unlock()
			return nil, fmt.Errorf("stored entry missing topic")
		}
		resp := s.topics[t.GetId()]
		s.lock.Unlock()
		return resp, nil
	}

	s.nextTopicID++
	id := s.nextTopicID
	seq := s.allocateSeqLocked()

	topic := &pb.Topic{Id: id, Name: name, SequenceNumber: seq, Committed: false}
	entry := &pb.LogEntry{
		SequenceNumber: seq,
		RequestId:      rid,
		Op:             pb.OpType_OP_CREATE_TOPIC,
		Topic:          topic,
	}

	if err := s.applyEntryLocked(entry); err != nil {
		s.lock.Unlock()
		return nil, err
	}
	s.prepareAck(seq)
	s.lock.Unlock()

	if err := s.propagateWithRetry(ctx, entry, 5); err != nil {
		return nil, err
	}
	if !s.waitAck(seq, 5*time.Second) {
		return nil, fmt.Errorf("timeout waiting for ack")
	}

	s.lock.Lock()
	s.markCommittedLocked(seq)
	resp := s.topics[id]
	s.lock.Unlock()
	return resp, nil
}

func (s *messageBoardServer) PostMessage(ctx context.Context, request *pb.PostMessageRequest) (*pb.Message, error) {
	if !s.isHead() {
		return nil, fmt.Errorf("write operations only allowed at head node")
	}
	if err := s.requireHeadReady(); err != nil {
		return nil, err
	}
	text := strings.TrimSpace(request.GetText())
	if text == "" {
		return nil, fmt.Errorf("message text not valid")
	}
	rid := strings.TrimSpace(request.GetRequestId())
	if rid == "" {
		return nil, fmt.Errorf("request_id required")
	}

	s.lock.Lock()
	if entry, ok := s.getEntryByRequestIDLocked(rid); ok {
		m := entry.GetMessage()
		if m == nil {
			s.lock.Unlock()
			return nil, fmt.Errorf("stored entry missing message")
		}
		resp := s.messages[m.GetTopicId()][m.GetId()]
		s.lock.Unlock()
		return resp, nil
	}

	if _, ok := s.users[request.GetUserId()]; !ok {
		s.lock.Unlock()
		return nil, fmt.Errorf("user %d does not exist", request.GetUserId())
	}
	if _, ok := s.topics[request.GetTopicId()]; !ok {
		s.lock.Unlock()
		return nil, fmt.Errorf("topic %d does not exist", request.GetTopicId())
	}
	topicID := request.GetTopicId()
	if _, ok := s.messages[topicID]; !ok {
		s.messages[topicID] = make(map[int64]*pb.Message)
	}
	s.nextMessageID[topicID]++
	msgID := s.nextMessageID[topicID]
	seq := s.allocateSeqLocked()

	msg := &pb.Message{
		Id:             msgID,
		TopicId:        topicID,
		UserId:         request.GetUserId(),
		Text:           text,
		CreatedAt:      timestamppb.Now(),
		Likes:          0,
		SequenceNumber: seq,
		Committed:      false,
	}

	entry := &pb.LogEntry{
		SequenceNumber: seq,
		RequestId:      rid,
		Op:             pb.OpType_OP_POST,
		Message:        msg,
	}

	if err := s.applyEntryLocked(entry); err != nil {
		s.lock.Unlock()
		return nil, err
	}
	s.prepareAck(seq)
	s.lock.Unlock()

	if err := s.propagateWithRetry(ctx, entry, 5); err != nil {
		return nil, err
	}
	if !s.waitAck(seq, 5*time.Second) {
		return nil, fmt.Errorf("timeout waiting for ack")
	}

	s.lock.Lock()
	s.markCommittedLocked(seq)
	resp := s.messages[topicID][msgID]
	s.lock.Unlock()
	return resp, nil
}

func (s *messageBoardServer) UpdateMessage(ctx context.Context, request *pb.UpdateMessageRequest) (*pb.Message, error) {
	if !s.isHead() {
		return nil, fmt.Errorf("write operations only allowed at head node")
	}
	if err := s.requireHeadReady(); err != nil {
		return nil, err
	}
	rid := strings.TrimSpace(request.GetRequestId())
	if rid == "" {
		return nil, fmt.Errorf("request_id required")
	}
	newText := strings.TrimSpace(request.GetText())
	if newText == "" {
		return nil, fmt.Errorf("text not valid")
	}

	s.lock.Lock()
	if entry, ok := s.getEntryByRequestIDLocked(rid); ok {
		m := entry.GetMessage()
		if m == nil {
			s.lock.Unlock()
			return nil, fmt.Errorf("stored entry missing message")
		}
		resp := s.messages[m.GetTopicId()][m.GetId()]
		s.lock.Unlock()
		return resp, nil
	}

	if _, ok := s.users[request.GetUserId()]; !ok {
		s.lock.Unlock()
		return nil, fmt.Errorf("user %d does not exist", request.GetUserId())
	}
	tm, ok := s.messages[request.GetTopicId()]
	if !ok {
		s.lock.Unlock()
		return nil, fmt.Errorf("topic %d does not exist", request.GetTopicId())
	}
	ex, ok := tm[request.GetMessageId()]
	if !ok {
		s.lock.Unlock()
		return nil, fmt.Errorf("message %d does not exist", request.GetMessageId())
	}
	if ex.UserId != request.GetUserId() {
		s.lock.Unlock()
		return nil, fmt.Errorf("user %d is not the author of message %d", request.GetUserId(), request.GetMessageId())
	}

	seq := s.allocateSeqLocked()
	snapshot := &pb.Message{
		Id:             ex.GetId(),
		TopicId:        ex.GetTopicId(),
		UserId:         ex.GetUserId(),
		Text:           newText,
		CreatedAt:      ex.GetCreatedAt(),
		Likes:          ex.GetLikes(),
		SequenceNumber: seq,
		Committed:      false,
	}

	entry := &pb.LogEntry{
		SequenceNumber: seq,
		RequestId:      rid,
		Op:             pb.OpType_OP_UPDATE,
		Message:        snapshot,
	}

	if err := s.applyEntryLocked(entry); err != nil {
		s.lock.Unlock()
		return nil, err
	}
	s.prepareAck(seq)
	s.lock.Unlock()

	if err := s.propagateWithRetry(ctx, entry, 5); err != nil {
		return nil, err
	}
	if !s.waitAck(seq, 5*time.Second) {
		return nil, fmt.Errorf("timeout waiting for ack")
	}

	s.lock.Lock()
	s.markCommittedLocked(seq)
	resp := s.messages[request.GetTopicId()][request.GetMessageId()]
	s.lock.Unlock()
	return resp, nil
}

func (s *messageBoardServer) DeleteMessage(ctx context.Context, request *pb.DeleteMessageRequest) (*emptypb.Empty, error) {
	if !s.isHead() {
		return nil, fmt.Errorf("write operations only allowed at head node")
	}
	if err := s.requireHeadReady(); err != nil {
		return nil, err
	}
	rid := strings.TrimSpace(request.GetRequestId())
	if rid == "" {
		return nil, fmt.Errorf("request_id required")
	}

	s.lock.Lock()
	if _, ok := s.getEntryByRequestIDLocked(rid); ok {
		s.lock.Unlock()
		return &emptypb.Empty{}, nil
	}

	if _, ok := s.users[request.GetUserId()]; !ok {
		s.lock.Unlock()
		return nil, fmt.Errorf("user %d does not exist", request.GetUserId())
	}
	tm, ok := s.messages[request.GetTopicId()]
	if !ok {
		s.lock.Unlock()
		return nil, fmt.Errorf("topic %d does not exist", request.GetTopicId())
	}
	ex, ok := tm[request.GetMessageId()]
	if !ok {
		s.lock.Unlock()
		return nil, fmt.Errorf("message %d does not exist", request.GetMessageId())
	}
	if ex.UserId != request.GetUserId() {
		s.lock.Unlock()
		return nil, fmt.Errorf("user %d is not the author of message %d", request.GetUserId(), request.GetMessageId())
	}

	seq := s.allocateSeqLocked()
	snapshot := &pb.Message{
		Id:             ex.GetId(),
		TopicId:        ex.GetTopicId(),
		UserId:         ex.GetUserId(),
		Text:           ex.GetText(),
		CreatedAt:      ex.GetCreatedAt(),
		Likes:          ex.GetLikes(),
		SequenceNumber: seq,
		Committed:      false,
	}
	entry := &pb.LogEntry{
		SequenceNumber: seq,
		RequestId:      rid,
		Op:             pb.OpType_OP_DELETE,
		Message:        snapshot,
	}

	if err := s.applyEntryLocked(entry); err != nil {
		s.lock.Unlock()
		return nil, err
	}
	s.prepareAck(seq)
	s.lock.Unlock()

	if err := s.propagateWithRetry(ctx, entry, 5); err != nil {
		return nil, err
	}
	if !s.waitAck(seq, 5*time.Second) {
		return nil, fmt.Errorf("timeout waiting for ack")
	}

	s.lock.Lock()
	s.markCommittedLocked(seq)
	s.lock.Unlock()
	return &emptypb.Empty{}, nil
}

func (s *messageBoardServer) LikeMessage(ctx context.Context, request *pb.LikeMessageRequest) (*pb.Message, error) {
	if !s.isHead() {
		return nil, fmt.Errorf("write operations only allowed at head node")
	}
	if err := s.requireHeadReady(); err != nil {
		return nil, err
	}
	rid := strings.TrimSpace(request.GetRequestId())
	if rid == "" {
		return nil, fmt.Errorf("request_id required")
	}

	s.lock.Lock()
	if entry, ok := s.getEntryByRequestIDLocked(rid); ok {
		m := entry.GetMessage()
		if m == nil {
			s.lock.Unlock()
			return nil, fmt.Errorf("stored entry missing message")
		}
		resp := s.messages[m.GetTopicId()][m.GetId()]
		s.lock.Unlock()
		return resp, nil
	}

	if _, ok := s.users[request.GetUserId()]; !ok {
		s.lock.Unlock()
		return nil, fmt.Errorf("user %d does not exist", request.GetUserId())
	}
	tm, ok := s.messages[request.GetTopicId()]
	if !ok {
		s.lock.Unlock()
		return nil, fmt.Errorf("topic %d does not exist", request.GetTopicId())
	}
	ex, ok := tm[request.GetMessageId()]
	if !ok {
		s.lock.Unlock()
		return nil, fmt.Errorf("message %d does not exist", request.GetMessageId())
	}

	seq := s.allocateSeqLocked()
	snapshot := &pb.Message{
		Id:             ex.GetId(),
		TopicId:        ex.GetTopicId(),
		UserId:         ex.GetUserId(),
		Text:           ex.GetText(),
		CreatedAt:      ex.GetCreatedAt(),
		Likes:          ex.GetLikes(),
		SequenceNumber: seq,
		Committed:      false,
	}
	entry := &pb.LogEntry{
		SequenceNumber: seq,
		RequestId:      rid,
		Op:             pb.OpType_OP_LIKE,
		Message:        snapshot,
	}

	if err := s.applyEntryLocked(entry); err != nil {
		s.lock.Unlock()
		return nil, err
	}
	s.prepareAck(seq)
	s.lock.Unlock()

	if err := s.propagateWithRetry(ctx, entry, 5); err != nil {
		return nil, err
	}
	if !s.waitAck(seq, 5*time.Second) {
		return nil, fmt.Errorf("timeout waiting for ack")
	}

	s.lock.Lock()
	s.markCommittedLocked(seq)
	resp := s.messages[request.GetTopicId()][request.GetMessageId()]
	s.lock.Unlock()
	return resp, nil
}

////////////////////////////////////////////////////////////////////////////////
// Reads (tail only)
////////////////////////////////////////////////////////////////////////////////

func (s *messageBoardServer) ListTopics(ctx context.Context, _ *emptypb.Empty) (*pb.ListTopicsResponse, error) {
	if !s.isTail() {
		return nil, fmt.Errorf("read operations only allowed at tail node")
	}
	s.lock.RLock()
	defer s.lock.RUnlock()

	out := &pb.ListTopicsResponse{Topics: make([]*pb.Topic, 0, len(s.topics))}
	for _, t := range s.topics {
		if t.Committed {
			out.Topics = append(out.Topics, t)
		}
	}
	sort.Slice(out.Topics, func(i, j int) bool { return out.Topics[i].Id < out.Topics[j].Id })
	return out, nil
}

func (s *messageBoardServer) GetMessages(ctx context.Context, request *pb.GetMessagesRequest) (*pb.GetMessagesResponse, error) {
	if !s.isTail() {
		return nil, fmt.Errorf("read operations only allowed at tail node")
	}
	s.lock.RLock()
	defer s.lock.RUnlock()

	tm, ok := s.messages[request.GetTopicId()]
	if !ok {
		return nil, fmt.Errorf("topic %d does not exist", request.GetTopicId())
	}

	fromID := request.GetFromMessageId()
	limit := request.GetLimit()

	ids := make([]int64, 0, len(tm))
	for id, msg := range tm {
		if id < fromID {
			continue
		}
		if !msg.Committed {
			continue
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	out := &pb.GetMessagesResponse{Messages: make([]*pb.Message, 0, len(ids))}
	for _, id := range ids {
		out.Messages = append(out.Messages, tm[id])
		if limit > 0 && int32(len(out.Messages)) >= limit {
			break
		}
	}
	return out, nil
}

////////////////////////////////////////////////////////////////////////////////
// Subscription load balancing (head)
////////////////////////////////////////////////////////////////////////////////

func (s *messageBoardServer) GetSubscriptionNode(ctx context.Context, request *pb.SubscriptionNodeRequest) (*pb.SubscriptionNodeResponse, error) {
	if !s.isHead() {
		return nil, fmt.Errorf("subscription routing only allowed at head node")
	}
	if err := s.requireHeadReady(); err != nil {
		return nil, err
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	if _, ok := s.users[request.GetUserId()]; !ok {
		return nil, fmt.Errorf("user %d does not exist", request.GetUserId())
	}
	if len(request.GetTopicId()) == 0 {
		return nil, fmt.Errorf("at least one topic required")
	}

	var selected *pb.NodeInfo
	min := int64(-1)

	if len(s.chain) == 0 {
		selected = &pb.NodeInfo{NodeId: s.nodeID, Address: s.address}
	} else {
		for _, n := range s.chain {
			c := s.nodeSubCount[n.GetNodeId()]
			if min == -1 || c < min {
				min = c
				selected = n
			}
		}
	}
	if selected == nil {
		selected = &pb.NodeInfo{NodeId: s.nodeID, Address: s.address}
	}
	s.nodeSubCount[selected.GetNodeId()]++

	token, err := s.makeSubscribeToken(request.GetUserId(), request.GetTopicId(), time.Now().Add(10*time.Minute).Unix())
	if err != nil {
		return nil, err
	}

	return &pb.SubscriptionNodeResponse{
		SubscribeToken: token,
		Node:           selected,
	}, nil
}

func (s *messageBoardServer) SubscribeTopic(request *pb.SubscribeTopicRequest, stream pb.MessageBoard_SubscribeTopicServer) error {
	// validate token (works on any node)
	topicsFromToken, err := s.validateSubscribeToken(request.GetSubscribeToken(), request.GetUserId())
	if err != nil {
		return err
	}

	topics := request.GetTopicId()
	if len(topics) == 0 {
		topics = topicsFromToken
	}

	topicsSet := make(map[int64]struct{}, len(topics))
	for _, t := range topics {
		topicsSet[t] = struct{}{}
	}

	s.lock.Lock()
	s.nextSubID++
	subID := s.nextSubID
	sub := &subscription{
		id:      subID,
		userID:  request.GetUserId(),
		topics:  topicsSet,
		channel: make(chan *pb.MessageEvent, 64),
	}
	s.subscribers[subID] = sub

	fromID := request.GetFromMessageId()
	backlog := make([]*pb.MessageEvent, 0)

	for topicID := range topicsSet {
		tm, ok := s.messages[topicID]
		if !ok {
			continue
		}
		ids := make([]int64, 0, len(tm))
		for msgID, msg := range tm {
			if msgID < fromID {
				continue
			}
			if !msg.Committed {
				continue
			}
			ids = append(ids, msgID)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

		evMap, ok := s.postEvents[topicID]
		if !ok {
			continue
		}
		for _, msgID := range ids {
			if ev, ok := evMap[msgID]; ok {
				backlog = append(backlog, ev)
			}
		}
	}
	s.lock.Unlock()

	for _, ev := range backlog {
		if err := stream.Send(ev); err != nil {
			s.removeSubscription(subID)
			return err
		}
	}

	for {
		select {
		case <-stream.Context().Done():
			s.removeSubscription(subID)
			return nil
		case ev, ok := <-sub.channel:
			if !ok {
				return nil
			}
			s.lock.RLock()
			committed := ev.GetSequenceNumber() <= s.lastCommittedSeq
			s.lock.RUnlock()
			if !committed {
				continue
			}
			if err := stream.Send(ev); err != nil {
				s.removeSubscription(subID)
				return err
			}
		}
	}
}

////////////////////////////////////////////////////////////////////////////////
// Internal chain RPCs
////////////////////////////////////////////////////////////////////////////////

func (s *messageBoardServer) PropagateEntry(ctx context.Context, entry *pb.LogEntry) (*emptypb.Empty, error) {
	s.lock.Lock()
	if err := s.applyEntryLocked(entry); err != nil {
		s.lock.Unlock()
		return nil, err
	}
	isTail := s.isTailLocked()
	s.lock.Unlock()

	// Forward to successor with retry (lets CP rewire chain on failure)
	if err := s.propagateWithRetry(ctx, entry, 5); err != nil {
		// If we cannot forward and we're not tail, caller should retry after chain reconfig.
		// We still keep the entry locally; it will be forwarded once successor changes.
		return nil, err
	}

	if isTail {
		s.lock.Lock()
		s.markCommittedLocked(entry.GetSequenceNumber())
		s.lock.Unlock()
		s.sendAckToPredecessor(ctx, entry.GetSequenceNumber())
	}

	return &emptypb.Empty{}, nil
}

func (s *messageBoardServer) Ack(ctx context.Context, request *pb.AckRequest) (*emptypb.Empty, error) {
	seq := request.GetSequenceNumber()

	s.lock.Lock()
	s.markCommittedLocked(seq)
	isHead := s.isHeadLocked()
	s.lock.Unlock()

	if isHead {
		s.signalAck(seq)
		return &emptypb.Empty{}, nil
	}

	s.sendAckToPredecessor(ctx, seq)
	return &emptypb.Empty{}, nil
}

func (s *messageBoardServer) GetLastApplied(ctx context.Context, _ *pb.GetLastAppliedRequest) (*pb.GetLastAppliedResponse, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return &pb.GetLastAppliedResponse{LastAppliedSeq: s.lastAppliedSeq}, nil
}

func (s *messageBoardServer) SyncFrom(ctx context.Context, request *pb.SyncFromRequest) (*pb.SyncFromResponse, error) {
	from := request.GetFromSeqExclusive()
	s.lock.RLock()
	defer s.lock.RUnlock()

	seqs := make([]int64, 0, len(s.logEntries))
	for seq := range s.logEntries {
		if seq > from {
			seqs = append(seqs, seq)
		}
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })

	out := &pb.SyncFromResponse{Entries: make([]*pb.LogEntry, 0, len(seqs))}
	for _, seq := range seqs {
		out.Entries = append(out.Entries, s.logEntries[seq])
	}
	return out, nil
}

////////////////////////////////////////////////////////////////////////////////
// Control plane -> node chain configuration
////////////////////////////////////////////////////////////////////////////////

func (s *messageBoardServer) ConfigureChain(ctx context.Context, request *pb.ConfigureChainRequest) (*emptypb.Empty, error) {
	if err := s.applyChainConfig(request.GetNodes()); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

////////////////////////////////////////////////////////////////////////////////
// Token helpers (HMAC signed)
////////////////////////////////////////////////////////////////////////////////

func (s *messageBoardServer) makeSubscribeToken(userID int64, topics []int64, exp int64) (string, error) {
	payload := tokenPayload{UserID: userID, Topics: append([]int64(nil), topics...), Exp: exp}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	b64 := base64.RawURLEncoding.EncodeToString(raw)
	sig := s.sign([]byte(b64))
	return b64 + "." + sig, nil
}

func (s *messageBoardServer) validateSubscribeToken(token string, userID int64) ([]int64, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return nil, fmt.Errorf("subscribe token not valid")
	}
	b64 := parts[0]
	sig := parts[1]
	if !hmac.Equal([]byte(sig), []byte(s.sign([]byte(b64)))) {
		return nil, fmt.Errorf("subscribe token not valid")
	}
	raw, err := base64.RawURLEncoding.DecodeString(b64)
	if err != nil {
		return nil, fmt.Errorf("subscribe token not valid")
	}
	var p tokenPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("subscribe token not valid")
	}
	if p.UserID != userID {
		return nil, fmt.Errorf("subscribe token not valid")
	}
	if time.Now().Unix() > p.Exp {
		return nil, fmt.Errorf("subscribe token expired")
	}
	return p.Topics, nil
}

func (s *messageBoardServer) sign(data []byte) string {
	mac := hmac.New(sha256.New, s.tokenSecret)
	_, _ = mac.Write(data)
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

////////////////////////////////////////////////////////////////////////////////
// main
////////////////////////////////////////////////////////////////////////////////

func main() {
	nodeID := flag.String("n", "node-1", "node ID")
	port := flag.Int("a", 50051, "listen port")
	controlAddr := flag.String("c", "localhost:60000", "control plane address host:port")
	secret := flag.String("secret", "dev-secret-change-me", "token signing secret (must match across nodes)")
	flag.Parse()

	addr := fmt.Sprintf("localhost:%d", *port)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	s := newMessageBoardServer(*nodeID, addr, *controlAddr, *secret)

	grpcServer := grpc.NewServer()
	pb.RegisterMessageBoardServer(grpcServer, s)

	// Start join + heartbeat
	s.joinControlPlane()
	go s.heartbeatLoop()

	log.Printf("MessageBoard node %s listening on %s (control=%s)", *nodeID, addr, *controlAddr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatal(err)
	}
}
