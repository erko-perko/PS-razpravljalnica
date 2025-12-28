// ali se lahko kreira več uporabnikov z istim imenom
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"sort"
	"sync"
	"time"

	pb "razpravljalnica/pb" //import pb.go datoteke

	"google.golang.org/grpc" //glavna knjižnica za grpc
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"     //import zaradi google.protobuf.Empty, ki ga uporabimo v metodah, kazalec na "nic"
	"google.golang.org/protobuf/types/known/timestamppb" //uporabimo zaradi create_at, event_at
)

// struktura za beleženje naročnine za specifičen topic
type SubscriptionTokenInfo struct {
	UserID int64
	Topics []int64
}

// struktura za naročnino
type subscription struct {
	id      int64
	userID  int64
	topics  map[int64]struct{}
	channel chan *pb.MessageEvent
}

/*
Glavna struktura strežnika
-baza (users, topics, messages)
-subscription kanali
-mutex (zaklepanje)
-ID generatorji
-vse RPC metode
*/
type messageBoardServer struct {
	pb.UnimplementedMessageBoardServer
	pb.UnimplementedControlPlaneServer

	lock sync.RWMutex // zaklep za varno delo z mapami iz več gorutin

	//baza v pomnilniku
	users       map[int64]*pb.User
	topics      map[int64]*pb.Topic
	messages    map[int64]map[int64]*pb.Message   // topicID -> messageID -> Message
	tokens      map[string]*SubscriptionTokenInfo //shranjevanje naročniških tokenov
	nextTokenID int64

	// števci za generiranje ID-jev
	nextUserID    int64
	nextTopicID   int64
	nextMessageID map[int64]int64 //messagei bodo imeli svoj ID glede na topic, vsak topic se začne z messageId = 0

	// identiteta tega vozlišča (za ControlPlane)
	nodeID  string
	address string

	// chain replication
	successor             *pb.NodeInfo          // naslednje vozlišče v verigi
	predecessor           *pb.NodeInfo          // prejšnje vozlišče v verigi
	chain                 []*pb.NodeInfo        // celotna veriga vozlišč
	successorClient       pb.MessageBoardClient // gRPC klient za komunikacijo s successorjem
	nodeSubscriptionCount map[string]int64      // število naročnin na vozlišče (za load balancing)
	headAddress           string                // address of the head node
	tailAddress           string                // address of the tail node

	// naročnina
	postEvents  map[int64]map[int64]*pb.MessageEvent
	subscribers map[int64]*subscription
	nextSubID   int64
	nextSeq     int64

	// acknowledgment tracking for two-phase commit
	ackChannels map[int64]chan bool // sequence_number -> acknowledgment channel
	ackLock     sync.Mutex          // separate lock for ack channels

	// sequence number validation
	lastSeqNumber int64 // last sequence number received (for validating order)
}

// ustvari preprost naključni token; kliče se pod lockom
func (s *messageBoardServer) newSubscribeToken(userID int64, topics []int64) string {
	s.nextTokenID++

	token := fmt.Sprintf("token-%d-%d", userID, s.nextTokenID)

	s.tokens[token] = &SubscriptionTokenInfo{
		UserID: userID,
		Topics: append([]int64(nil), topics...),
	}
	return token
}

// allocateSeqNumber allocates a new sequence number for an operation
func (s *messageBoardServer) allocateSeqNumber() int64 {
	s.nextSeq++
	return s.nextSeq
}

// prepareAck creates an acknowledgment channel for a sequence number
func (s *messageBoardServer) prepareAck(seqNum int64) {
	s.ackLock.Lock()
	defer s.ackLock.Unlock()
	if s.isTail() {
		return
	}
	s.ackChannels[seqNum] = make(chan bool, 1)
}

// waitForAck waits for acknowledgment from tail with timeout
func (s *messageBoardServer) waitForAck(seqNum int64) bool {
	s.ackLock.Lock()

	// if we are tail, no need to wait
	if s.isTail() {
		s.ackLock.Unlock()
		return true
	}

	// get acknowledgment channel
	ackChan, ok := s.ackChannels[seqNum]
	s.ackLock.Unlock()

	if !ok {
		return false
	}

	select {
	case <-ackChan:
		// Clean up the channel after receiving
		s.ackLock.Lock()
		delete(s.ackChannels, seqNum)
		s.ackLock.Unlock()
		return true
	case <-time.After(5 * time.Second):
		// cleanup channel on timeout
		s.ackLock.Lock()
		delete(s.ackChannels, seqNum)
		s.ackLock.Unlock()
		return false
	}
}

// sendAck sends acknowledgment signal for a sequence number
func (s *messageBoardServer) sendAck(seqNum int64) {
	s.ackLock.Lock()
	defer s.ackLock.Unlock()

	if ackChan, ok := s.ackChannels[seqNum]; ok {
		select {
		case ackChan <- true:
		default:
		}
		// Don't delete here - let waitForAck clean up after receiving
	}
}

// validateSequenceNumber validates that the sequence number is the next expected one
// Returns error if sequence number is out of order (more than 1 greater than last)
func (s *messageBoardServer) validateSequenceNumber(seqNum int64) error {
	if s.lastSeqNumber == 0 {
		// First operation, accept any sequence number
		s.lastSeqNumber = seqNum
		return nil
	}

	if seqNum != s.lastSeqNumber+1 {
		return fmt.Errorf("out of order operation: expected sequence %d, got %d", s.lastSeqNumber+1, seqNum)
	}

	s.lastSeqNumber = seqNum
	return nil
}

// newMessageEvent uporabimo, da za naročnino zabeležimo vse evente, ki se zgodijo in jih broadcastamo vsem naročnikom
func (s *messageBoardServer) newMessageEvent(op pb.OpType, message *pb.Message) *pb.MessageEvent {
	return &pb.MessageEvent{
		SequenceNumber: s.nextSeq,
		Op:             op,
		Message:        message,
		EventAt:        timestamppb.Now(),
	}
}

// pošlje sporočilo o eventu vsem naročnikom, ki so naročeni na topic
func (s *messageBoardServer) broadcastEvent(event *pb.MessageEvent) {
	if event.GetMessage() == nil {
		return
	}
	topicID := event.GetMessage().GetTopicId()

	for _, subscriber := range s.subscribers {
		if _, ok := subscriber.topics[topicID]; !ok {
			continue
		}
		//select zaradi možnosti polnega kanala, da se ga izpusti
		select {
		case subscriber.channel <- event: //channel je vezan na subscription
		default:
			//če je kanal poln ali prepočasen se ga izpusti
		}
	}
}

// RemoveSubscription zaustavi celoten kanal(subscription), če se naročnik odjavi od specifične teme.
// Posledično se z zaprtjem kanal odjavi tudi od vseh ostalih tem, na katere se je prijavil z enim Subscription requestom
func (s *messageBoardServer) removeSubscription(id int64) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if subscriber, ok := s.subscribers[id]; ok {
		delete(s.subscribers, id)
		close(subscriber.channel)
	}
}

// CreateUser ustvari novega uporabnika in mu dodeli ID.
// Write operacije so dovoljene samo na head vozlišču.
func (s *messageBoardServer) CreateUser(ctx context.Context, request *pb.CreateUserRequest) (*pb.User, error) {
	// samo head lahko sprejema write operacije
	if !s.isHead() {
		return nil, fmt.Errorf("write operations only allowed at head node")
	}

	if request.GetName() == "" {
		return nil, fmt.Errorf("name not valid")
	}

	// zaklenemo za pisanje, ker bomo spreminjali stanje
	s.lock.Lock()

	//preverjanje unikatnosti imena
	for _, user := range s.users {
		if user.Name == request.GetName() {
			s.lock.Unlock()
			return nil, fmt.Errorf("username taken")
		}
	}

	// generiramo nov ID in sequence number
	s.nextUserID++
	id := s.nextUserID
	seqNum := s.allocateSeqNumber()

	// sestavimo novega userja
	user := &pb.User{
		Id:             id,
		Name:           request.GetName(),
		SequenceNumber: seqNum,
		Committed:      false,
	}

	// shranimo v bazo
	s.users[id] = user
	s.lock.Unlock()

	// prepare ack channel before propagation
	s.prepareAck(seqNum)

	// posredujemo naprej v verigo
	if err := s.propagateCreateUser(ctx, user); err != nil {
		return nil, err
	}

	// wait for acknowledgment from tail
	if s.waitForAck(seqNum) {
		user.Committed = true
	} else {
		return nil, fmt.Errorf("timeout waiting for acknowledgment from tail")
	}

	return user, nil
}

// CreateTopic ustvari novo temo in ji dodeli ID.
// Write operacije so dovoljene samo na head vozlišču.
func (s *messageBoardServer) CreateTopic(ctx context.Context, request *pb.CreateTopicRequest) (*pb.Topic, error) {
	// samo head lahko sprejema write operacije
	if !s.isHead() {
		return nil, fmt.Errorf("write operations only allowed at head node")
	}

	if request.GetName() == "" {
		return nil, fmt.Errorf("topic name not valid")
	}

	s.lock.Lock()

	s.nextTopicID++
	id := s.nextTopicID
	seqNum := s.allocateSeqNumber()

	topic := &pb.Topic{
		Id:             id,
		Name:           request.GetName(),
		SequenceNumber: seqNum,
		Committed:      false,
	}

	s.topics[id] = topic

	// za vsak nov topic pripravimo mapo za sporočila
	if _, ok := s.messages[id]; !ok {
		s.messages[id] = make(map[int64]*pb.Message)
	}

	if _, ok := s.nextMessageID[id]; !ok {
		s.nextMessageID[id] = 0
	}

	if _, ok := s.postEvents[id]; !ok {
		s.postEvents[id] = make(map[int64]*pb.MessageEvent)
	}

	s.lock.Unlock()

	// prepare ack channel before propagation
	s.prepareAck(seqNum)

	// posredujemo naprej v verigo
	if err := s.propagateCreateTopic(ctx, topic); err != nil {
		return nil, err
	}

	// wait for acknowledgment from tail
	if s.waitForAck(seqNum) {
		topic.Committed = true
	} else {
		return nil, fmt.Errorf("timeout waiting for acknowledgment from tail")
	}

	return topic, nil
}

// ListTopics vrne vse teme.
// Read operacije servira samo tail vozlišče.
func (s *messageBoardServer) ListTopics(ctx context.Context, _ *emptypb.Empty) (*pb.ListTopicsResponse, error) { //context.context nujen del rpc metode
	s.lock.RLock()         // samo beremo - RLock
	defer s.lock.RUnlock() // sprostimo lock na koncu

	// samo tail lahko streže read operacije
	if !s.isTail() {
		return nil, fmt.Errorf("read operations only allowed at tail node")
	}

	response := &pb.ListTopicsResponse{
		Topics: make([]*pb.Topic, 0, len(s.topics)),
	}

	for _, topic := range s.topics {
		response.Topics = append(response.Topics, topic)
	}

	return response, nil
}

// PostMessage doda novo sporočilo v temo.
// Uspe le, če user in topic obstajata.
// Write operacije so dovoljene samo na head vozlišču.
func (s *messageBoardServer) PostMessage(ctx context.Context, request *pb.PostMessageRequest) (*pb.Message, error) {

	// samo head lahko sprejema write operacije
	if !s.isHead() {
		return nil, fmt.Errorf("write operations only allowed at head node")
	}

	if request.GetText() == "" {
		return nil, fmt.Errorf("message text not valid")
	}

	s.lock.Lock()

	//preverimo, da user obstaja
	if _, ok := s.users[request.GetUserId()]; !ok {
		s.lock.Unlock()
		return nil, fmt.Errorf("user %d does not exist", request.GetUserId())
	}

	//preverimo, da topic obstaja
	if _, ok := s.topics[request.GetTopicId()]; !ok {
		s.lock.Unlock()
		return nil, fmt.Errorf("topic %d does not exist", request.GetTopicId())
	}

	//topic mora imeti mapo za sporočila
	if _, ok := s.messages[request.GetTopicId()]; !ok {
		s.messages[request.GetTopicId()] = make(map[int64]*pb.Message)
	}

	//pridobimo topic, da povečamo števec sporočila za specifičen topic
	topicID := request.GetTopicId()

	//nov ID
	s.nextMessageID[topicID]++
	id := s.nextMessageID[topicID]
	seqNum := s.allocateSeqNumber()

	message := &pb.Message{
		Id:             id,
		TopicId:        request.GetTopicId(),
		UserId:         request.GetUserId(),
		Text:           request.GetText(),
		CreatedAt:      timestamppb.Now(),
		Likes:          0,
		SequenceNumber: seqNum,
		Committed:      false,
	}

	// shranimo v bazo
	s.messages[topicID][id] = message

	event := s.newMessageEvent(pb.OpType_OP_POST, message) //kreiramo event za broadcast naročnino

	// shranimo originalni POST event za backlog
	if _, ok := s.postEvents[topicID]; !ok {
		s.postEvents[topicID] = make(map[int64]*pb.MessageEvent)
	}
	s.postEvents[topicID][id] = event

	s.broadcastEvent(event) //sprotni broadcast vsem naročnikom

	s.lock.Unlock()

	// prepare ack channel before propagation
	s.prepareAck(seqNum)

	// posredujemo naprej v verigo
	if err := s.propagateToSuccessor(ctx, pb.OpType_OP_POST, message); err != nil {
		return nil, err
	}

	// wait for acknowledgment from tail
	if s.waitForAck(seqNum) {
		message.Committed = true
	} else {
		return nil, fmt.Errorf("timeout waiting for acknowledgment from tail")
	}

	return message, nil
}

// GetMessages vrne sporočila znotraj določene teme, urejena po ID-ju naraščajoče.
// from_message_id = 0 pomeni "od začetka"
// limit določa maksimalno število sporočil (0 = brez omejitve).
// Read operacije so dovoljene samo na tail vozlišču.
func (s *messageBoardServer) GetMessages(ctx context.Context, request *pb.GetMessagesRequest) (*pb.GetMessagesResponse, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	// samo tail lahko streže read operacije
	if !s.isTail() {
		return nil, fmt.Errorf("read operations only allowed at tail node")
	}

	// preverimo, če tema obstaja
	tMessages, ok := s.messages[request.GetTopicId()]
	if !ok {
		return nil, fmt.Errorf("topic %d does not exist", request.GetTopicId())
	}

	fromID := request.GetFromMessageId()
	limit := request.GetLimit()

	//zberemo vse ID-je sporočil
	messagesID := make([]int64, 0, len(tMessages))
	for messageID := range tMessages {
		if messageID < fromID {
			continue
		}
		messagesID = append(messagesID, messageID)
	}

	//uredimo ID-je naraščajoče
	sort.Slice(messagesID, func(i, j int) bool {
		return messagesID[i] < messagesID[j]
	})

	response := &pb.GetMessagesResponse{
		Messages: make([]*pb.Message, 0, len(messagesID)),
	}

	//dodamo sporočila v urejenem vrstnem redu in upoštevamo limit
	for _, messageID := range messagesID {
		response.Messages = append(response.Messages, tMessages[messageID])
		if limit > 0 && int32(len(response.Messages)) >= limit {
			break
		}
	}

	return response, nil
}

// UpdateMessage posodobi obstoječe sporočilo.
// Dovoli se samo uporabniku, ki je sporočilo napisal.
// Write operacije so dovoljene samo na head vozlišču.
func (s *messageBoardServer) UpdateMessage(ctx context.Context, request *pb.UpdateMessageRequest) (*pb.Message, error) {
	// samo head lahko sprejema write operacije
	if !s.isHead() {
		return nil, fmt.Errorf("write operations only allowed at head node")
	}

	s.lock.Lock()

	//preverimo, če user obstaja
	if _, ok := s.users[request.GetUserId()]; !ok {
		s.lock.Unlock()
		return nil, fmt.Errorf("user %d does not exist", request.GetUserId())
	}

	//preverimo, če tema obstaja
	tMessages, ok := s.messages[request.GetTopicId()]
	if !ok {
		s.lock.Unlock()
		return nil, fmt.Errorf("topic %d does not exist", request.GetTopicId())
	}

	//preverimo, če sporočilo obstaja
	message, ok := tMessages[request.GetMessageId()]
	if !ok {
		s.lock.Unlock()
		return nil, fmt.Errorf("message %d does not exist", request.GetMessageId())
	}

	//preverimo, da je to sporočilo od ustreznega uporabnika
	if message.UserId != request.GetUserId() {
		s.lock.Unlock()
		return nil, fmt.Errorf("user %d is not the author of message %d", request.GetUserId(), request.GetMessageId())
	}

	//posodobimo besedilo
	seqNum := s.allocateSeqNumber()
	message.Text = request.GetText()
	message.SequenceNumber = seqNum
	message.Committed = false

	event := s.newMessageEvent(pb.OpType_OP_UPDATE, message)
	s.broadcastEvent(event) //sprotni broadcast naročnikom

	s.lock.Unlock()

	// prepare ack channel before propagation
	s.prepareAck(seqNum)

	// posredujemo naprej v verigo
	if err := s.propagateToSuccessor(ctx, pb.OpType_OP_UPDATE, message); err != nil {
		return nil, err
	}

	// wait for acknowledgment from tail
	if s.waitForAck(seqNum) {
		message.Committed = true
	} else {
		return nil, fmt.Errorf("timeout waiting for acknowledgment from tail")
	}

	return message, nil
}

// DeleteMessage izbriše obstoječe sporočilo.
// Dovoljeno samo avtorju sporočila.
// Write operacije so dovoljene samo na head vozlišču.
func (s *messageBoardServer) DeleteMessage(ctx context.Context, request *pb.DeleteMessageRequest) (*emptypb.Empty, error) {
	// samo head lahko sprejema write operacije
	if !s.isHead() {
		return nil, fmt.Errorf("write operations only allowed at head node")
	}

	s.lock.Lock()

	//preverimo, če user obstaja
	if _, ok := s.users[request.GetUserId()]; !ok {
		s.lock.Unlock()
		return nil, fmt.Errorf("user %d does not exist", request.GetUserId())
	}

	// preverimo, če user obstaja
	tMessages, ok := s.messages[request.GetTopicId()]
	if !ok {
		s.lock.Unlock()
		return nil, fmt.Errorf("topic %d does not exist", request.GetTopicId())
	}

	//preverimo, če sporočilo obstaja
	message, ok := tMessages[request.GetMessageId()]
	if !ok {
		s.lock.Unlock()
		return nil, fmt.Errorf("message %d does not exist", request.GetMessageId())
	}

	//preverimo, da je to sporočilo od ustreznega uporabnika
	if message.UserId != request.GetUserId() {
		s.lock.Unlock()
		return nil, fmt.Errorf("user %d is not the author of message %d", request.GetUserId(), request.GetMessageId())
	}

	// zabeležimo zaporedno številko za operacijo
	seqNum := s.allocateSeqNumber()
	message.SequenceNumber = seqNum

	event := s.newMessageEvent(pb.OpType_OP_DELETE, message)

	// izbrišemo sporočilo
	delete(tMessages, request.GetMessageId())

	s.broadcastEvent(event) //sprotni broadcast naročnikom

	s.lock.Unlock()

	// prepare ack channel before propagation
	s.prepareAck(seqNum)

	// posredujemo naprej v verigo
	if err := s.propagateToSuccessor(ctx, pb.OpType_OP_DELETE, message); err != nil {
		return nil, err
	}

	// wait for acknowledgment from tail
	if !s.waitForAck(seqNum) {
		return nil, fmt.Errorf("timeout waiting for acknowledgment from tail")
	}

	return &emptypb.Empty{}, nil
}

// LikeMessage poveča število všečkov na sporočilu.
// Sporočilo lahko like-a katerikoli obstoječi uporabnik
// Write operacije so dovoljene samo na head vozlišču.
func (s *messageBoardServer) LikeMessage(ctx context.Context, request *pb.LikeMessageRequest) (*pb.Message, error) {
	// samo head lahko sprejema write operacije
	if !s.isHead() {
		return nil, fmt.Errorf("write operations only allowed at head node")
	}

	s.lock.Lock()

	// preverimo, da uporabnik obstaja, da lahko všečka
	if _, ok := s.users[request.GetUserId()]; !ok {
		s.lock.Unlock()
		return nil, fmt.Errorf("user %d does not exist", request.GetUserId())
	}

	// preverimo, da tema obstaja
	tMessages, ok := s.messages[request.GetTopicId()]
	if !ok {
		s.lock.Unlock()
		return nil, fmt.Errorf("topic %d does not exist", request.GetTopicId())
	}

	// preverimo, da sporočilo obstaja
	message, ok := tMessages[request.GetMessageId()]
	if !ok {
		s.lock.Unlock()
		return nil, fmt.Errorf("message %d does not exist", request.GetMessageId())
	}

	// povečamo število všečkov
	seqNum := s.allocateSeqNumber()
	message.Likes++
	message.SequenceNumber = seqNum
	message.Committed = false

	event := s.newMessageEvent(pb.OpType_OP_LIKE, message)
	s.broadcastEvent(event) //sprotni broadcast naročnikom

	s.lock.Unlock()

	// prepare ack channel before propagation
	s.prepareAck(seqNum)

	// posredujemo naprej v verigo
	if err := s.propagateToSuccessor(ctx, pb.OpType_OP_LIKE, message); err != nil {
		return nil, err
	}

	// wait for acknowledgment from tail
	if s.waitForAck(seqNum) {
		message.Committed = true
	} else {
		return nil, fmt.Errorf("timeout waiting for acknowledgment from tail")
	}

	return message, nil
}

// isHead preverja, ali je to vozlišče glava verige
func (s *messageBoardServer) isHead() bool {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.address == s.headAddress
}

// isTail preverja, ali je to vozlišče rep verige
func (s *messageBoardServer) isTail() bool {
	// s.lock.RLock()
	// defer s.lock.RUnlock()
	return s.address == s.tailAddress
}

// propagateToSuccessor posreduje operacijo naslednjemu vozlišču v verigi
func (s *messageBoardServer) propagateToSuccessor(ctx context.Context, op pb.OpType, message *pb.Message) error {
	s.lock.RLock()
	successorClient := s.successorClient
	s.lock.RUnlock()

	if successorClient == nil {
		// ni successorja, smo tail
		return nil
	}

	// posredujemo operacijo naslednjemu vozlišču
	switch op {
	case pb.OpType_OP_POST:
		req := &pb.PostMessageRequest{
			UserId:         message.UserId,
			TopicId:        message.TopicId,
			Text:           message.Text,
			MessageId:      message.Id, // posreduj ID, ki ga je generiral head
			SequenceNumber: message.SequenceNumber,
		}
		_, err := successorClient.PropagatePost(ctx, req)
		return err

	case pb.OpType_OP_UPDATE:
		req := &pb.UpdateMessageRequest{
			UserId:         message.UserId,
			TopicId:        message.TopicId,
			MessageId:      message.Id,
			Text:           message.Text,
			SequenceNumber: message.SequenceNumber,
		}
		_, err := successorClient.PropagateUpdate(ctx, req)
		return err

	case pb.OpType_OP_DELETE:
		req := &pb.DeleteMessageRequest{
			UserId:         message.UserId,
			TopicId:        message.TopicId,
			MessageId:      message.Id,
			SequenceNumber: message.SequenceNumber,
		}
		_, err := successorClient.PropagateDelete(ctx, req)
		return err

	case pb.OpType_OP_LIKE:
		req := &pb.LikeMessageRequest{
			UserId:         message.UserId,
			TopicId:        message.TopicId,
			MessageId:      message.Id,
			SequenceNumber: message.SequenceNumber,
		}
		_, err := successorClient.PropagateLike(ctx, req)
		return err
	}

	return nil
}

// propagateCreateUser posreduje CreateUser operacijo naslednjemu vozlišču
func (s *messageBoardServer) propagateCreateUser(ctx context.Context, user *pb.User) error {
	s.lock.RLock()
	successorClient := s.successorClient
	s.lock.RUnlock()

	if successorClient == nil {
		// ni successorja, smo tail
		return nil
	}

	req := &pb.CreateUserRequest{
		Name:           user.Name,
		Id:             user.Id, // posreduj ID, ki ga je generiral head
		SequenceNumber: user.SequenceNumber,
	}
	_, err := successorClient.PropagateCreateUser(ctx, req)
	return err
}

// propagateCreateTopic posreduje CreateTopic operacijo naslednjemu vozlišču
func (s *messageBoardServer) propagateCreateTopic(ctx context.Context, topic *pb.Topic) error {
	s.lock.RLock()
	successorClient := s.successorClient
	s.lock.RUnlock()

	if successorClient == nil {
		// ni successorja, smo tail
		return nil
	}

	req := &pb.CreateTopicRequest{
		Name:           topic.Name,
		Id:             topic.Id, // posreduj ID, ki ga je generiral head
		SequenceNumber: topic.SequenceNumber,
	}
	_, err := successorClient.PropagateCreateTopic(ctx, req)
	return err
}

// gre za nadzorno ravnino, ki spremlja katera vozlišča so head in tail
func (s *messageBoardServer) GetClusterState(ctx context.Context, _ *emptypb.Empty) (*pb.GetClusterStateResponse, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	var head, tail *pb.NodeInfo

	if len(s.chain) > 0 {
		head = s.chain[0]
		tail = s.chain[len(s.chain)-1]
	} else {
		// če ni verige, vrnemo samo ta node
		node := &pb.NodeInfo{
			NodeId:  s.nodeID,
			Address: s.address,
		}
		head = node
		tail = node
	}

	return &pb.GetClusterStateResponse{
		Head: head,
		Tail: tail,
	}, nil
}

// GetSubscriptionNode vrne node, na katerega naj se klient naroči in generira subscribe_token, ki ga bo kasneje preveril SubscribeTopic.
// Izvaja load balancing tako, da izbere vozlišče z najmanj naročninami.
func (s *messageBoardServer) GetSubscriptionNode(ctx context.Context, request *pb.SubscriptionNodeRequest) (*pb.SubscriptionNodeResponse, error) {
	s.lock.Lock()

	// preverimo, da user obstaja
	if _, ok := s.users[request.GetUserId()]; !ok {
		s.lock.Unlock()
		return nil, fmt.Errorf("user %d does not exist", request.GetUserId())
	}

	// generiramo in shranimo token lokalno
	token := s.newSubscribeToken(request.GetUserId(), request.GetTopicId())

	// izberi vozlišče z najmanj naročninami za load balancing
	var selectedNode *pb.NodeInfo
	var minCount int64 = -1

	if len(s.chain) > 0 {
		// iteriramo skozi vso verigo in poiščemo vozlišče z najmanj naročninami
		for _, node := range s.chain {
			count := s.nodeSubscriptionCount[node.GetNodeId()]
			if minCount == -1 || count < minCount {
				minCount = count
				selectedNode = node
			}
		}
		// povečamo števec za izbrano vozlišče
		if selectedNode != nil {
			s.nodeSubscriptionCount[selectedNode.GetNodeId()]++
		}
	} else {
		// če ni verige, vrnemo samo ta node
		selectedNode = &pb.NodeInfo{
			NodeId:  s.nodeID,
			Address: s.address,
		}
		s.nodeSubscriptionCount[s.nodeID]++
	}

	s.lock.Unlock()

	// če je izbrano vozlišče različno od trenutnega, propagirajmo token
	if selectedNode.GetNodeId() != s.nodeID {
		conn, err := grpc.NewClient(selectedNode.GetAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("failed to connect to selected node: %v", err)
		}
		defer conn.Close()

		client := pb.NewMessageBoardClient(conn)
		_, err = client.PropagateToken(ctx, &pb.TokenPropagationRequest{
			Token:   token,
			UserId:  request.GetUserId(),
			TopicId: request.GetTopicId(),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to propagate token: %v", err)
		}
	}

	return &pb.SubscriptionNodeResponse{
		SubscribeToken: token,
		Node:           selectedNode,
	}, nil
}

// SubscribeTopic odpre stream dogodkov za izbrane topice.
// Uporabi subscribe_token, ki ga je prej vrnil GetSubscriptionNode.
func (s *messageBoardServer) SubscribeTopic(request *pb.SubscribeTopicRequest, stream pb.MessageBoard_SubscribeTopicServer) error {
	//preverimo token in pripravimo subscription pod Lockom
	s.lock.Lock()

	tokenInfo, ok := s.tokens[request.GetSubscribeToken()]
	if !ok || tokenInfo.UserID != request.GetUserId() {
		s.lock.Unlock()
		return fmt.Errorf("subscribe token not valid")
	}

	// če client ne navede topicov, uporabimo tiste iz tokena
	topicsID := request.GetTopicId()
	if len(topicsID) == 0 {
		topicsID = tokenInfo.Topics
	}

	topicsSet := make(map[int64]struct{})
	for _, t := range topicsID {
		topicsSet[t] = struct{}{}
	}

	// ustvarimo subscription
	s.nextSubID++
	subID := s.nextSubID

	sub := &subscription{
		id:      subID,
		userID:  request.GetUserId(),
		topics:  topicsSet,
		channel: make(chan *pb.MessageEvent, 16),
	}
	s.subscribers[subID] = sub

	// pripravimo backlog sporočil od from_message_id naprej, deluje kot nekakšen sync za vsa prejšnja sporočila, ko še ni bil naročen
	fromID := request.GetFromMessageId()
	backlog := make([]*pb.MessageEvent, 0)

	for topicID := range topicsSet {
		tMessages, ok := s.messages[topicID]
		if !ok {
			continue
		}

		// zberemo ID-je in jih uredimo
		messages := make([]int64, 0, len(tMessages))
		for message := range tMessages {
			if message < fromID {
				continue
			}
			messages = append(messages, message)
		}
		sort.Slice(messages, func(i, j int) bool {
			return messages[i] < messages[j]
		})

		//backlog je sestavljen iz sporočil, ki smo si jih shranili v mapo
		postEvMap, ok := s.postEvents[topicID]
		if !ok {
			continue
		}

		for _, messageID := range messages {
			if ev, ok := postEvMap[messageID]; ok {
				backlog = append(backlog, ev)
			}
		}

	}

	s.lock.Unlock()

	//najprej pošljemo backlog
	for _, event := range backlog {
		if err := stream.Send(event); err != nil {
			s.removeSubscription(subID)
			return err
		}
	}

	//nato v zanki pošiljamo nove evente iz sub.ch
	for {
		select {
		case <-stream.Context().Done():
			// klient se je odklopil
			s.removeSubscription(subID)
			return nil
		case event, ok := <-sub.channel:
			if !ok {
				// kanal zaprt
				return nil
			}
			if err := stream.Send(event); err != nil {
				s.removeSubscription(subID)
				return err
			}
		}
	}
}

// PropagatePost prejme POST operacijo od predecessor vozlišča in jo posreduje naprej
func (s *messageBoardServer) PropagatePost(ctx context.Context, request *pb.PostMessageRequest) (*pb.Message, error) {
	s.lock.Lock()

	// preverimo, da user obstaja
	if _, ok := s.users[request.GetUserId()]; !ok {
		s.lock.Unlock()
		return nil, fmt.Errorf("user %d does not exist", request.GetUserId())
	}

	// preverimo, da topic obstaja
	if _, ok := s.topics[request.GetTopicId()]; !ok {
		s.lock.Unlock()
		return nil, fmt.Errorf("topic %d does not exist", request.GetTopicId())
	}

	// topic mora imeti mapo za sporočila
	if _, ok := s.messages[request.GetTopicId()]; !ok {
		s.messages[request.GetTopicId()] = make(map[int64]*pb.Message)
	}

	topicID := request.GetTopicId()

	// uporabimo ID, ki ga je določil head (poslan v requestu), namesto da ustvarjamo novega
	id := request.GetMessageId()
	if id == 0 {
		// generiraj ID, če ni podan (naj se ne bi zgodilo)
		s.nextMessageID[topicID]++
		id = s.nextMessageID[topicID]
	} else {
		// posodobi števec ID-jev, da ustreza prejetemu ID-ju (od heada)
		if id > s.nextMessageID[topicID] {
			s.nextMessageID[topicID] = id
		}
	}

	seqNum := request.GetSequenceNumber()

	// Validate sequence number to ensure operations arrive in order
	if err := s.validateSequenceNumber(seqNum); err != nil {
		s.lock.Unlock()
		return nil, err
	}

	message := &pb.Message{
		Id:             id,
		TopicId:        request.GetTopicId(),
		UserId:         request.GetUserId(),
		Text:           request.GetText(),
		CreatedAt:      timestamppb.Now(),
		Likes:          0,
		SequenceNumber: seqNum,
		Committed:      false,
	}

	// shranimo v bazo
	s.messages[topicID][id] = message

	event := s.newMessageEvent(pb.OpType_OP_POST, message)

	// shranimo originalni POST event za backlog
	if _, ok := s.postEvents[topicID]; !ok {
		s.postEvents[topicID] = make(map[int64]*pb.MessageEvent)
	}
	s.postEvents[topicID][id] = event

	s.broadcastEvent(event)

	isTail := s.isTail()
	pred := s.predecessor

	s.lock.Unlock()

	// posredujemo naprej v verigo
	if err := s.propagateToSuccessor(ctx, pb.OpType_OP_POST, message); err != nil {
		return nil, err
	}

	// if we are tail, send acknowledgment back
	if isTail && pred != nil {
		conn, err := grpc.NewClient(pred.GetAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			defer conn.Close()
			client := pb.NewMessageBoardClient(conn)
			ackReq := &pb.AckRequest{
				SequenceNumber: seqNum,
				OpType:         pb.OpType_OP_POST,
				EntityId:       id,
				TopicId:        topicID,
			}
			_, _ = client.AckPost(ctx, ackReq)
		}
	}

	return message, nil
}

// PropagateUpdate prejme UPDATE operacijo od predecessor vozlišča
func (s *messageBoardServer) PropagateUpdate(ctx context.Context, request *pb.UpdateMessageRequest) (*pb.Message, error) {
	s.lock.Lock()

	// preverimo, če user obstaja
	if _, ok := s.users[request.GetUserId()]; !ok {
		s.lock.Unlock()
		return nil, fmt.Errorf("user %d does not exist", request.GetUserId())
	}

	// preverimo, če tema obstaja
	tMessages, ok := s.messages[request.GetTopicId()]
	if !ok {
		s.lock.Unlock()
		return nil, fmt.Errorf("topic %d does not exist", request.GetTopicId())
	}

	// preverimo, če sporočilo obstaja
	message, ok := tMessages[request.GetMessageId()]
	if !ok {
		s.lock.Unlock()
		return nil, fmt.Errorf("message %d does not exist", request.GetMessageId())
	}

	// preverimo, da je to sporočilo od ustreznega uporabnika
	if message.UserId != request.GetUserId() {
		s.lock.Unlock()
		return nil, fmt.Errorf("user %d is not the author of message %d", request.GetUserId(), request.GetMessageId())
	}

	// posodobimo besedilo
	seqNum := request.GetSequenceNumber()

	// Validate sequence number to ensure operations arrive in order
	if err := s.validateSequenceNumber(seqNum); err != nil {
		s.lock.Unlock()
		return nil, err
	}

	message.Text = request.GetText()
	message.SequenceNumber = seqNum
	message.Committed = false

	event := s.newMessageEvent(pb.OpType_OP_UPDATE, message)
	s.broadcastEvent(event)

	isTail := s.isTail()
	pred := s.predecessor
	topicID := request.GetTopicId()
	messageID := request.GetMessageId()

	s.lock.Unlock()

	// posredujemo naprej v verigo
	if err := s.propagateToSuccessor(ctx, pb.OpType_OP_UPDATE, message); err != nil {
		return nil, err
	}

	// if we are tail, send acknowledgment back
	if isTail && pred != nil {
		conn, err := grpc.NewClient(pred.GetAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			defer conn.Close()
			client := pb.NewMessageBoardClient(conn)
			ackReq := &pb.AckRequest{
				SequenceNumber: seqNum,
				OpType:         pb.OpType_OP_UPDATE,
				EntityId:       messageID,
				TopicId:        topicID,
			}
			_, _ = client.AckUpdate(ctx, ackReq)
		}
	}

	return message, nil
}

// PropagateDelete prejme DELETE operacijo od predecessor vozlišča
func (s *messageBoardServer) PropagateDelete(ctx context.Context, request *pb.DeleteMessageRequest) (*emptypb.Empty, error) {
	s.lock.Lock()

	// preverimo, če user obstaja
	if _, ok := s.users[request.GetUserId()]; !ok {
		s.lock.Unlock()
		return nil, fmt.Errorf("user %d does not exist", request.GetUserId())
	}

	// preverimo, če user obstaja
	tMessages, ok := s.messages[request.GetTopicId()]
	if !ok {
		s.lock.Unlock()
		return nil, fmt.Errorf("topic %d does not exist", request.GetTopicId())
	}

	// preverimo, če sporočilo obstaja
	message, ok := tMessages[request.GetMessageId()]
	if !ok {
		s.lock.Unlock()
		return nil, fmt.Errorf("message %d does not exist", request.GetMessageId())
	}

	// preverimo, da je to sporočilo od ustreznega uporabnika
	if message.UserId != request.GetUserId() {
		s.lock.Unlock()
		return nil, fmt.Errorf("user %d is not the author of message %d", request.GetUserId(), request.GetMessageId())
	}

	seqNum := request.GetSequenceNumber()
	message.SequenceNumber = seqNum

	// Validate sequence number to ensure operations arrive in order
	if err := s.validateSequenceNumber(seqNum); err != nil {
		s.lock.Unlock()
		return nil, err
	}

	event := s.newMessageEvent(pb.OpType_OP_DELETE, message)

	// izbrišemo sporočilo
	delete(tMessages, request.GetMessageId())

	s.broadcastEvent(event)

	isTail := s.isTail()
	pred := s.predecessor
	topicID := request.GetTopicId()
	messageID := request.GetMessageId()

	s.lock.Unlock()

	// posredujemo naprej v verigo
	if err := s.propagateToSuccessor(ctx, pb.OpType_OP_DELETE, message); err != nil {
		return nil, err
	}

	// if we are tail, send acknowledgment back
	if isTail && pred != nil {
		conn, err := grpc.NewClient(pred.GetAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			defer conn.Close()
			client := pb.NewMessageBoardClient(conn)
			ackReq := &pb.AckRequest{
				SequenceNumber: seqNum,
				OpType:         pb.OpType_OP_DELETE,
				EntityId:       messageID,
				TopicId:        topicID,
			}
			_, _ = client.AckDelete(ctx, ackReq)
		}
	}

	return &emptypb.Empty{}, nil
}

// PropagateLike prejme LIKE operacijo od predecessor vozlišča
func (s *messageBoardServer) PropagateLike(ctx context.Context, request *pb.LikeMessageRequest) (*pb.Message, error) {
	s.lock.Lock()

	// preverimo, da uporabnik obstaja, da lahko všečka
	if _, ok := s.users[request.GetUserId()]; !ok {
		s.lock.Unlock()
		return nil, fmt.Errorf("user %d does not exist", request.GetUserId())
	}

	// preverimo, da tema obstaja
	tMessages, ok := s.messages[request.GetTopicId()]
	if !ok {
		s.lock.Unlock()
		return nil, fmt.Errorf("topic %d does not exist", request.GetTopicId())
	}

	// preverimo, da sporočilo obstaja
	message, ok := tMessages[request.GetMessageId()]
	if !ok {
		s.lock.Unlock()
		return nil, fmt.Errorf("message %d does not exist", request.GetMessageId())
	}

	// povečamo število všečkov
	seqNum := request.GetSequenceNumber()

	// Validate sequence number to ensure operations arrive in order
	if err := s.validateSequenceNumber(seqNum); err != nil {
		s.lock.Unlock()
		return nil, err
	}

	message.Likes++
	message.SequenceNumber = seqNum
	message.Committed = false

	event := s.newMessageEvent(pb.OpType_OP_LIKE, message)
	s.broadcastEvent(event)

	isTail := s.isTail()
	pred := s.predecessor
	topicID := request.GetTopicId()
	messageID := request.GetMessageId()

	s.lock.Unlock()

	// posredujemo naprej v verigo
	if err := s.propagateToSuccessor(ctx, pb.OpType_OP_LIKE, message); err != nil {
		return nil, err
	}

	// if we are tail, send acknowledgment back
	if isTail && pred != nil {
		conn, err := grpc.NewClient(pred.GetAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			defer conn.Close()
			client := pb.NewMessageBoardClient(conn)
			ackReq := &pb.AckRequest{
				SequenceNumber: seqNum,
				OpType:         pb.OpType_OP_LIKE,
				EntityId:       messageID,
				TopicId:        topicID,
			}
			_, _ = client.AckLike(ctx, ackReq)
		}
	}

	return message, nil
}

// PropagateCreateUser prejme CreateUser operacijo od predecessor vozlišča
func (s *messageBoardServer) PropagateCreateUser(ctx context.Context, request *pb.CreateUserRequest) (*pb.User, error) {
	s.lock.Lock()

	// preverjanje unikatnosti imena
	for _, user := range s.users {
		if user.Name == request.GetName() {
			s.lock.Unlock()
			return nil, fmt.Errorf("username taken")
		}
	}

	// uporabimo ID, ki ga je določil head (poslan v requestu), namesto da ustvarjamo novega
	id := request.GetId()
	if id == 0 {
		// generiraj ID, če ni podan (naj se ne bi zgodilo)
		s.nextUserID++
		id = s.nextUserID
	} else {
		// posodobi števec ID-jev, da ustreza prejetemu ID-ju (od heada)
		if id > s.nextUserID {
			s.nextUserID = id
		}
	}

	seqNum := request.GetSequenceNumber()

	// Validate sequence number to ensure operations arrive in order
	if err := s.validateSequenceNumber(seqNum); err != nil {
		s.lock.Unlock()
		return nil, err
	}

	// sestavimo novega userja
	user := &pb.User{
		Id:             id,
		Name:           request.GetName(),
		SequenceNumber: seqNum,
		Committed:      false,
	}

	// shranimo v bazo
	s.users[id] = user

	isTail := s.isTail()
	pred := s.predecessor

	s.lock.Unlock()

	// posredujemo naprej v verigo samo če nismo tail
	if !isTail {
		if err := s.propagateCreateUser(ctx, user); err != nil {
			return nil, err
		}
	}

	// if we are tail, send acknowledgment back
	if isTail && pred != nil {
		conn, err := grpc.NewClient(pred.GetAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("failed to connect to predecessor for ack: %v", err)
		}
		defer conn.Close()
		client := pb.NewMessageBoardClient(conn)
		ackReq := &pb.AckRequest{
			SequenceNumber: seqNum,
			OpType:         pb.OpType_OP_POST, // using POST as generic op type for create
			EntityId:       id,
			TopicId:        0,
		}
		_, err = client.AckCreateUser(ctx, ackReq)
		if err != nil {
			return nil, fmt.Errorf("failed to send ack: %v", err)
		}
	}

	return user, nil
}

// PropagateCreateTopic prejme CreateTopic operacijo od predecessor vozlišča
func (s *messageBoardServer) PropagateCreateTopic(ctx context.Context, request *pb.CreateTopicRequest) (*pb.Topic, error) {
	s.lock.Lock()

	// uporabimo ID, ki ga je določil head (poslan v requestu), namesto da ustvarjamo novega
	id := request.GetId()
	if id == 0 {
		// generiraj ID, če ni podan (naj se ne bi zgodilo)
		s.nextTopicID++
		id = s.nextTopicID
	} else {
		// posodobi števec ID-jev, da ustreza prejetemu ID-ju (od heada)
		if id > s.nextTopicID {
			s.nextTopicID = id
		}
	}

	seqNum := request.GetSequenceNumber()

	// Validate sequence number to ensure operations arrive in order
	if err := s.validateSequenceNumber(seqNum); err != nil {
		s.lock.Unlock()
		return nil, err
	}

	topic := &pb.Topic{
		Id:             id,
		Name:           request.GetName(),
		SequenceNumber: seqNum,
		Committed:      false,
	}

	s.topics[id] = topic

	// za vsak nov topic pripravimo mapo za sporočila
	if _, ok := s.messages[id]; !ok {
		s.messages[id] = make(map[int64]*pb.Message)
	}

	if _, ok := s.nextMessageID[id]; !ok {
		s.nextMessageID[id] = 0
	}

	if _, ok := s.postEvents[id]; !ok {
		s.postEvents[id] = make(map[int64]*pb.MessageEvent)
	}

	isTail := s.isTail()
	pred := s.predecessor

	s.lock.Unlock()

	// posredujemo naprej v verigo samo če nismo tail
	if !isTail {
		if err := s.propagateCreateTopic(ctx, topic); err != nil {
			return nil, err
		}
	}

	// if we are tail, send acknowledgment back
	if isTail && pred != nil {
		conn, err := grpc.NewClient(pred.GetAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("failed to connect to predecessor for ack: %v", err)
		}
		defer conn.Close()
		client := pb.NewMessageBoardClient(conn)
		ackReq := &pb.AckRequest{
			SequenceNumber: seqNum,
			OpType:         pb.OpType_OP_POST, // using POST as generic op type for create
			EntityId:       id,
			TopicId:        id,
		}
		_, err = client.AckCreateTopic(ctx, ackReq)
		if err != nil {
			return nil, fmt.Errorf("failed to send ack: %v", err)
		}
	}

	return topic, nil
}

// PropagateToken prejme token od drugega vozlišča in ga shrani lokalno
// To omogoča load balancing subscriptions across nodes
func (s *messageBoardServer) PropagateToken(ctx context.Context, request *pb.TokenPropagationRequest) (*emptypb.Empty, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// shranimo token v lokalnem store-u
	s.tokens[request.GetToken()] = &SubscriptionTokenInfo{
		UserID: request.GetUserId(),
		Topics: request.GetTopicId(),
	}

	return &emptypb.Empty{}, nil
}

// Acknowledgment RPC handlers - called by successor to confirm operation completion

// AckPost handles acknowledgment for POST operations
func (s *messageBoardServer) AckPost(ctx context.Context, request *pb.AckRequest) (*emptypb.Empty, error) {
	if s.isHead() {
		s.sendAck(request.GetSequenceNumber())
		return &emptypb.Empty{}, nil
	}

	s.lock.RLock()
	pred := s.predecessor
	s.lock.RUnlock()

	if pred != nil {
		conn, err := grpc.NewClient(pred.GetAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			defer conn.Close()
			client := pb.NewMessageBoardClient(conn)
			_, _ = client.AckPost(ctx, request)
		}
	}

	return &emptypb.Empty{}, nil
}

// AckUpdate handles acknowledgment for UPDATE operations
func (s *messageBoardServer) AckUpdate(ctx context.Context, request *pb.AckRequest) (*emptypb.Empty, error) {
	if s.isHead() {
		s.sendAck(request.GetSequenceNumber())
		return &emptypb.Empty{}, nil
	}

	s.lock.RLock()
	pred := s.predecessor
	s.lock.RUnlock()

	if pred != nil {
		conn, err := grpc.NewClient(pred.GetAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			defer conn.Close()
			client := pb.NewMessageBoardClient(conn)
			_, _ = client.AckUpdate(ctx, request)
		}
	}

	return &emptypb.Empty{}, nil
}

// AckDelete handles acknowledgment for DELETE operations
func (s *messageBoardServer) AckDelete(ctx context.Context, request *pb.AckRequest) (*emptypb.Empty, error) {
	if s.isHead() {
		s.sendAck(request.GetSequenceNumber())
		return &emptypb.Empty{}, nil
	}

	s.lock.RLock()
	pred := s.predecessor
	s.lock.RUnlock()

	if pred != nil {
		conn, err := grpc.NewClient(pred.GetAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			defer conn.Close()
			client := pb.NewMessageBoardClient(conn)
			_, _ = client.AckDelete(ctx, request)
		}
	}

	return &emptypb.Empty{}, nil
}

// AckLike handles acknowledgment for LIKE operations
func (s *messageBoardServer) AckLike(ctx context.Context, request *pb.AckRequest) (*emptypb.Empty, error) {
	if s.isHead() {
		s.sendAck(request.GetSequenceNumber())
		return &emptypb.Empty{}, nil
	}

	s.lock.RLock()
	pred := s.predecessor
	s.lock.RUnlock()

	if pred != nil {
		conn, err := grpc.NewClient(pred.GetAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			defer conn.Close()
			client := pb.NewMessageBoardClient(conn)
			_, _ = client.AckLike(ctx, request)
		}
	}

	return &emptypb.Empty{}, nil
}

// AckCreateUser handles acknowledgment for CREATE USER operations
func (s *messageBoardServer) AckCreateUser(ctx context.Context, request *pb.AckRequest) (*emptypb.Empty, error) {
	if s.isHead() {
		s.sendAck(request.GetSequenceNumber())
		return &emptypb.Empty{}, nil
	}

	s.lock.RLock()
	pred := s.predecessor
	s.lock.RUnlock()

	if pred != nil {
		conn, err := grpc.NewClient(pred.GetAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			defer conn.Close()
			client := pb.NewMessageBoardClient(conn)
			_, _ = client.AckCreateUser(ctx, request)
		}
	}

	return &emptypb.Empty{}, nil
}

// AckCreateTopic handles acknowledgment for CREATE TOPIC operations
func (s *messageBoardServer) AckCreateTopic(ctx context.Context, request *pb.AckRequest) (*emptypb.Empty, error) {
	if s.isHead() {
		s.sendAck(request.GetSequenceNumber())
		return &emptypb.Empty{}, nil
	}

	s.lock.RLock()
	pred := s.predecessor
	s.lock.RUnlock()

	if pred != nil {
		conn, err := grpc.NewClient(pred.GetAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			defer conn.Close()
			client := pb.NewMessageBoardClient(conn)
			_, _ = client.AckCreateTopic(ctx, request)
		}
	}

	return &emptypb.Empty{}, nil
}

// ConfigureChain konfigurira verigo replikacije
func (s *messageBoardServer) ConfigureChain(ctx context.Context, request *pb.ConfigureChainRequest) (*emptypb.Empty, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	nodes := request.GetNodes()
	s.chain = nodes

	// set head and tail addresses
	if len(nodes) > 0 {
		s.headAddress = nodes[0].GetAddress()
		s.tailAddress = nodes[len(nodes)-1].GetAddress()
	} else {
		s.headAddress = s.address
		s.tailAddress = s.address
	}

	// inicializiramo števce naročnin za vsa vozlišča v verigi
	for _, node := range nodes {
		if _, ok := s.nodeSubscriptionCount[node.GetNodeId()]; !ok {
			s.nodeSubscriptionCount[node.GetNodeId()] = 0
		}
	}

	// poiščemo svoj položaj v verigi
	var myIndex int = -1
	for i, node := range nodes {
		if node.GetNodeId() == s.nodeID {
			myIndex = i
			break
		}
	}

	if myIndex == -1 {
		return nil, fmt.Errorf("this node (%s) is not in the chain", s.nodeID)
	}

	// nastavimo predecessor
	if myIndex > 0 {
		s.predecessor = nodes[myIndex-1]
	} else {
		s.predecessor = nil
	}

	// nastavimo successor
	if myIndex < len(nodes)-1 {
		s.successor = nodes[myIndex+1]

		// ustvarimo gRPC povezavo do successorja
		conn, err := grpc.NewClient(s.successor.GetAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("failed to connect to successor: %v", err)
		}
		s.successorClient = pb.NewMessageBoardClient(conn)
	} else {
		s.successor = nil
		s.successorClient = nil
	}

	log.Printf("Chain configured: position %d/%d, predecessor=%v, successor=%v",
		myIndex+1, len(nodes), s.predecessor != nil, s.successor != nil)

	return &emptypb.Empty{}, nil
}

/*
Konstruktor
*/
func newMessageBoardServer(nodeID, address string) *messageBoardServer {
	return &messageBoardServer{
		users:                 make(map[int64]*pb.User),
		topics:                make(map[int64]*pb.Topic),
		messages:              make(map[int64]map[int64]*pb.Message),
		nextMessageID:         make(map[int64]int64),
		postEvents:            make(map[int64]map[int64]*pb.MessageEvent),
		tokens:                make(map[string]*SubscriptionTokenInfo),
		subscribers:           make(map[int64]*subscription),
		nodeID:                nodeID,
		address:               address,
		chain:                 make([]*pb.NodeInfo, 0),
		nodeSubscriptionCount: make(map[string]int64),
		ackChannels:           make(map[int64]chan bool),
		headAddress:           address, // initially, this node is both head and tail
		tailAddress:           address,
	}
}

// glavna funkcija - gRPC strežnik
func main() {
	nodeID := flag.String("n", "node-1", "node ID")
	address := flag.Int("a", 50051, "listen address")
	flag.Parse()

	grpcStreznik := grpc.NewServer() //ustvari nov gRPC strežnik

	url := fmt.Sprintf("localhost:%d", *address)

	streznik := newMessageBoardServer(*nodeID, url) // ustvarimo strežnik

	pb.RegisterMessageBoardServer(grpcStreznik, streznik) // registriramo oba servisa, ta je glavni za večino funkcij odjemalca
	pb.RegisterControlPlaneServer(grpcStreznik, streznik) //nadzorna ravnina

	// odpremo TCP port
	listen, err := net.Listen("tcp", url)
	if err != nil {
		panic(err)
	}

	log.Printf("MessageBoard server %s listening on %s", *nodeID, url)

	if err := grpcStreznik.Serve(listen); err != nil {
		panic(err)
	}
}
