// ali se lahko kreira več uporabnikov z istim imenom
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"sort"
	"sync"

	pb "razpravljalnica/pb" //import pb.go datoteke

	"google.golang.org/grpc"                             //glavna knjižnica za grpc
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

	// naročnina
	postEvents  map[int64]map[int64]*pb.MessageEvent
	subscribers map[int64]*subscription
	nextSubID   int64
	nextSeq     int64
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

// newMessageEvent uporabimo, da za naročnino zabeležimo vse evente, ki se zgodijo in jih broadcastamo vsem naročnikom
func (s *messageBoardServer) newMessageEvent(op pb.OpType, message *pb.Message) *pb.MessageEvent {
	s.nextSeq++
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
func (s *messageBoardServer) CreateUser(ctx context.Context, request *pb.CreateUserRequest) (*pb.User, error) {
	if request.GetName() == "" {
		return nil, fmt.Errorf("name not valid")
	}

	// zaklenemo za pisanje, ker bomo spreminjali stanje
	s.lock.Lock()
	defer s.lock.Unlock()

	//preverjanje unikatnosti imena
	for _, user := range s.users {
		if user.Name == request.GetName() {
			return nil, fmt.Errorf("username taken")
		}
	}

	// generiramo nov ID
	s.nextUserID++
	id := s.nextUserID

	// sestavimo novega userja
	user := &pb.User{
		Id:   id,
		Name: request.GetName(),
	}

	// shranimo v bazo
	s.users[id] = user

	// vrnemo uporabnika nazaj odjemalcu
	return user, nil
}

// CreateTopic ustvari novo temo in ji dodeli ID.
func (s *messageBoardServer) CreateTopic(ctx context.Context, request *pb.CreateTopicRequest) (*pb.Topic, error) {
	if request.GetName() == "" {
		return nil, fmt.Errorf("topic name not valid")
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	s.nextTopicID++
	id := s.nextTopicID

	topic := &pb.Topic{
		Id:   id,
		Name: request.GetName(),
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

	return topic, nil
}

// ListTopics vrne vse teme.
func (s *messageBoardServer) ListTopics(ctx context.Context, _ *emptypb.Empty) (*pb.ListTopicsResponse, error) { //context.context nujen del rpc metode
	s.lock.RLock()         // samo beremo - RLock
	defer s.lock.RUnlock() // sprostimo lock na koncu

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
func (s *messageBoardServer) PostMessage(ctx context.Context, request *pb.PostMessageRequest) (*pb.Message, error) {

	if request.GetText() == "" {
		return nil, fmt.Errorf("message text not valid")
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	//preverimo, da user obstaja
	if _, ok := s.users[request.GetUserId()]; !ok {
		return nil, fmt.Errorf("user %d does not exist", request.GetUserId())
	}

	//preverimo, da topic obstaja
	if _, ok := s.topics[request.GetTopicId()]; !ok {
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

	message := &pb.Message{
		Id:        id,
		TopicId:   request.GetTopicId(),
		UserId:    request.GetUserId(),
		Text:      request.GetText(),
		CreatedAt: timestamppb.Now(),
		Likes:     0,
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

	return message, nil
}

// GetMessages vrne sporočila znotraj določene teme, urejena po ID-ju naraščajoče.
// from_message_id = 0 pomeni "od začetka"
// limit določa maksimalno število sporočil (0 = brez omejitve).
func (s *messageBoardServer) GetMessages(ctx context.Context, request *pb.GetMessagesRequest) (*pb.GetMessagesResponse, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

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
func (s *messageBoardServer) UpdateMessage(ctx context.Context, request *pb.UpdateMessageRequest) (*pb.Message, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	//preverimo, če user obstaja
	if _, ok := s.users[request.GetUserId()]; !ok {
		return nil, fmt.Errorf("user %d does not exist", request.GetUserId())
	}

	//preverimo, če tema obstaja
	tMessages, ok := s.messages[request.GetTopicId()]
	if !ok {
		return nil, fmt.Errorf("topic %d does not exist", request.GetTopicId())
	}

	//preverimo, če sporočilo obstaja
	message, ok := tMessages[request.GetMessageId()]
	if !ok {
		return nil, fmt.Errorf("message %d does not exist", request.GetMessageId())
	}

	//preverimo, da je to sporočilo od ustreznega uporabnika
	if message.UserId != request.GetUserId() {
		return nil, fmt.Errorf("user %d is not the author of message %d", request.GetUserId(), request.GetMessageId())
	}

	//posodobimo besedilo
	message.Text = request.GetText()

	event := s.newMessageEvent(pb.OpType_OP_UPDATE, message)
	s.broadcastEvent(event) //sprotni broadcast naročnikom

	return message, nil
}

// DeleteMessage izbriše obstoječe sporočilo.
// Dovoljeno samo avtorju sporočila.
func (s *messageBoardServer) DeleteMessage(ctx context.Context, request *pb.DeleteMessageRequest) (*emptypb.Empty, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	//preverimo, če user obstaja
	if _, ok := s.users[request.GetUserId()]; !ok {
		return nil, fmt.Errorf("user %d does not exist", request.GetUserId())
	}

	// preverimo, če user obstaja
	tMessages, ok := s.messages[request.GetTopicId()]
	if !ok {
		return nil, fmt.Errorf("topic %d does not exist", request.GetTopicId())
	}

	//preverimo, če sporočilo obstaja
	message, ok := tMessages[request.GetMessageId()]
	if !ok {
		return nil, fmt.Errorf("message %d does not exist", request.GetMessageId())
	}

	//preverimo, da je to sporočilo od ustreznega uporabnika
	if message.UserId != request.GetUserId() {
		return nil, fmt.Errorf("user %d is not the author of message %d", request.GetUserId(), request.GetMessageId())
	}

	event := s.newMessageEvent(pb.OpType_OP_DELETE, message)

	// izbrišemo sporočilo
	delete(tMessages, request.GetMessageId())

	s.broadcastEvent(event) //sprotni broadcast naročnikom

	return &emptypb.Empty{}, nil
}

// LikeMessage poveča število všečkov na sporočilu.
// Sporočilo lahko like-a katerikoli obstoječi uporabnik
func (s *messageBoardServer) LikeMessage(ctx context.Context, request *pb.LikeMessageRequest) (*pb.Message, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// preverimo, da uporabnik obstaja, da lahko všečka
	if _, ok := s.users[request.GetUserId()]; !ok {
		return nil, fmt.Errorf("user %d does not exist", request.GetUserId())
	}

	// preverimo, da tema obstaja
	tMessages, ok := s.messages[request.GetTopicId()]
	if !ok {
		return nil, fmt.Errorf("topic %d does not exist", request.GetTopicId())
	}

	// preverimo, da sporočilo obstaja
	message, ok := tMessages[request.GetMessageId()]
	if !ok {
		return nil, fmt.Errorf("message %d does not exist", request.GetMessageId())
	}

	// povečamo število všečkov
	message.Likes++

	event := s.newMessageEvent(pb.OpType_OP_LIKE, message)
	s.broadcastEvent(event) //sprotni broadcast naročnikom

	return message, nil
}

// gre za nadzorno ravnino, ki spremlja katera vozlišča so head in tail, zaenkrat vse isti strežnik, ker je samo en
func (s *messageBoardServer) GetClusterState(ctx context.Context, _ *emptypb.Empty) (*pb.GetClusterStateResponse, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	node := &pb.NodeInfo{
		NodeId:  s.nodeID,
		Address: s.address,
	}

	return &pb.GetClusterStateResponse{
		Head: node,
		Tail: node,
	}, nil
}

// GetSubscriptionNode vrne node, na katerega naj se klient naroči in generira subscribe_token, ki ga bo kasneje preveril SubscribeTopic.
func (s *messageBoardServer) GetSubscriptionNode(ctx context.Context, request *pb.SubscriptionNodeRequest) (*pb.SubscriptionNodeResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// preverimo, da user obstaja
	if _, ok := s.users[request.GetUserId()]; !ok {
		return nil, fmt.Errorf("user %d does not exist", request.GetUserId())
	}

	// generiramo in shranimo token
	token := s.newSubscribeToken(request.GetUserId(), request.GetTopicId())

	// vrnemo ta strežnik kot node
	return &pb.SubscriptionNodeResponse{
		SubscribeToken: token,
		Node: &pb.NodeInfo{
			NodeId:  s.nodeID,
			Address: s.address,
		},
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

/*
Konstruktor
*/
func newMessageBoardServer() *messageBoardServer {
	return &messageBoardServer{
		users:         make(map[int64]*pb.User),
		topics:        make(map[int64]*pb.Topic),
		messages:      make(map[int64]map[int64]*pb.Message),
		nextMessageID: make(map[int64]int64),
		postEvents:    make(map[int64]map[int64]*pb.MessageEvent),
		tokens:        make(map[string]*SubscriptionTokenInfo),
		subscribers:   make(map[int64]*subscription),
		nodeID:        "node-1",
		address:       ":50051",
	}
}

// glavna funkcija - gRPC strežnik
func main() {

	address := ":50051" //standardni gRPC port

	grpcStreznik := grpc.NewServer() //ustvari nov gRPC strežnik

	streznik := newMessageBoardServer() // ustvarimo strežnik, glavni strežnik

	pb.RegisterMessageBoardServer(grpcStreznik, streznik) // registriramo oba servisa, ta je glavni za večino funkcij odjemalca
	pb.RegisterControlPlaneServer(grpcStreznik, streznik) //nadzorna ravnina

	// odpremo TCP port
	listen, err := net.Listen("tcp", address)
	if err != nil {
		panic(err)
	}

	log.Printf("MessageBoard server listening on %s", address)

	if err := grpcStreznik.Serve(listen); err != nil {
		panic(err)
	}
}
