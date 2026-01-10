package main

import (
	"context"
	"flag"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	pb "razpravljalnica/pb" //import pb.go datoteke

	"google.golang.org/grpc" //glavna knjižnica za grpc
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb" //import zaradi google.protobuf.Empty, ki ga uporabimo v metodah, kazalec na "nic"
)

var reqCounter atomic.Int64

func newRequestID(prefix string) string {
	n := reqCounter.Add(1)
	return fmt.Sprintf("%s-%d-%d", prefix, time.Now().UnixNano(), n)
}

func dial(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

func withTimeout(parent context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, d)
}

func recvEvents(name string, stream pb.MessageBoard_SubscribeTopicClient, stopAfter int, done chan<- struct{}) {
	defer func() { done <- struct{}{} }()

	count := 0
	for {
		ev, err := stream.Recv()
		if err != nil {
			fmt.Printf("[%s] subscription ended: %v\n", name, err)
			return
		}
		count++
		fmt.Printf("[%s] %s %v seq=%d msgId=%d topic=%d\n",
			name,
			ev.GetEventAt().AsTime().Format("2006-01-02 15:04:05"),
			ev.GetOp(),
			ev.GetSequenceNumber(),
			ev.GetMessage().GetId(),
			ev.GetMessage().GetTopicId(),
		)
		if stopAfter > 0 && count >= stopAfter {
			return
		}
	}
}

func main() {
	// preberemo argumente iz ukazne vrstice
	hostPtr := flag.String("s", "localhost", "server host (default localhost)")
	hPtr := flag.Int("h", 50051, "head port number")
	tPtr := flag.Int("t", 50053, "tail port number")
	cpPtr := flag.String("c", "", "control plane address host:port (optional; if set, discovers head/tail)")
	timeoutPtr := flag.Duration("timeout", 3*time.Second, "per-RPC timeout")
	subSecondsPtr := flag.Duration("sub", 10*time.Second, "how long to keep subscriptions open")
	flag.Parse()

	// Small pause after each action so replication/heartbeat propagation can catch up.
	actionDelay := 500 * time.Millisecond

	baseCtx := context.Background()

	headUrl := fmt.Sprintf("%v:%v", *hostPtr, *hPtr)
	tailUrl := fmt.Sprintf("%v:%v", *hostPtr, *tPtr)

	// Optional: discover head/tail from control plane.
	if *cpPtr != "" {
		fmt.Printf("Discovering head/tail from control plane at %v\n", *cpPtr)
		cpConn, err := dial(*cpPtr)
		if err != nil {
			fmt.Printf("Control plane dial failed, using flags: %v\n", err)
		} else {
			cpClient := pb.NewControlPlaneClient(cpConn)
			ctx, cancel := withTimeout(baseCtx, *timeoutPtr)
			st, err := cpClient.GetClusterState(ctx, &emptypb.Empty{})
			cancel()
			_ = cpConn.Close()
			if err != nil {
				fmt.Printf("GetClusterState failed, using flags: %v\n", err)
			} else {
				if st.GetHead() != nil && st.GetHead().GetAddress() != "" {
					headUrl = st.GetHead().GetAddress()
				}
				if st.GetTail() != nil && st.GetTail().GetAddress() != "" {
					tailUrl = st.GetTail().GetAddress()
				}
				fmt.Printf("Discovered head=%s tail=%s\n", headUrl, tailUrl)
			}
		}
		time.Sleep(actionDelay)
	}

	fmt.Printf("gRPC client connecting to head at %v\n", headUrl)
	headConn, err := dial(headUrl)
	if err != nil {
		fmt.Printf("Failed to dial head: %v\n", err)
		return
	}
	fmt.Printf("gRPC client connecting to tail at %v\n", tailUrl)
	tailConn, err := dial(tailUrl)
	if err != nil {
		_ = headConn.Close()
		fmt.Printf("Failed to dial tail: %v\n", err)
		return
	}
	defer headConn.Close()
	defer tailConn.Close()

	// vzpostavimo vmesnik gRPC
	grpcHeadClient := pb.NewMessageBoardClient(headConn)
	grpcTailClient := pb.NewMessageBoardClient(tailConn)

	// --- Basic negative tests (real clients handle errors, don't panic) ---
	{
		ctx, cancel := withTimeout(baseCtx, *timeoutPtr)
		_, err := grpcTailClient.CreateUser(ctx, &pb.CreateUserRequest{Name: "should-fail", RequestId: newRequestID("create-user")})
		cancel()
		fmt.Printf("Write at tail (expected error): %v\n", err)
	}
	time.Sleep(actionDelay)
	{
		ctx, cancel := withTimeout(baseCtx, *timeoutPtr)
		_, err := grpcHeadClient.ListTopics(ctx, &emptypb.Empty{})
		cancel()
		fmt.Printf("Read at head (expected error): %v\n", err)
	}
	time.Sleep(actionDelay)

	var wg sync.WaitGroup
	subDone := make(chan struct{}, 2)

	fmt.Println("\nCreating user ID=1.")
	createUserRID := newRequestID("create-user")
	ctx, cancel := withTimeout(baseCtx, *timeoutPtr)
	user, err := grpcHeadClient.CreateUser(ctx, &pb.CreateUserRequest{Name: "janez", RequestId: createUserRID})
	cancel()
	if err != nil {
		fmt.Printf("CreateUser failed: %v\n", err)
		return
	}
	fmt.Printf("Created user: %v (ID: %v)\n", user.Name, user.Id)
	time.Sleep(actionDelay)

	// Idempotency demo (same request_id should return same result).
	ctx, cancel = withTimeout(baseCtx, *timeoutPtr)
	userAgain, err := grpcHeadClient.CreateUser(ctx, &pb.CreateUserRequest{Name: "janez", RequestId: createUserRID})
	cancel()
	fmt.Printf("CreateUser idempotent: id=%d err=%v\n", userAgain.GetId(), err)
	time.Sleep(actionDelay)

	fmt.Println("\nCreating user ID=2.")
	ctx, cancel = withTimeout(baseCtx, *timeoutPtr)
	user1, err := grpcHeadClient.CreateUser(ctx, &pb.CreateUserRequest{Name: "Miha", RequestId: newRequestID("create-user")})
	cancel()
	if err != nil {
		fmt.Printf("CreateUser failed: %v\n", err)
		return
	}
	fmt.Printf("Created user: %v (ID: %v)\n", user1.Name, user1.Id)
	time.Sleep(actionDelay)

	fmt.Println("\nCreating topics.")
	ctx, cancel = withTimeout(baseCtx, *timeoutPtr)
	topic1, err := grpcHeadClient.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "Prva tema", RequestId: newRequestID("create-topic")})
	cancel()
	if err != nil {
		fmt.Printf("CreateTopic failed: %v\n", err)
		return
	}
	time.Sleep(actionDelay)
	ctx, cancel = withTimeout(baseCtx, *timeoutPtr)
	topic2, err := grpcHeadClient.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "Druga tema", RequestId: newRequestID("create-topic")})
	cancel()
	if err != nil {
		fmt.Printf("CreateTopic failed: %v\n", err)
		return
	}
	fmt.Printf("Created topics: %v (ID: %v), %v (ID: %v)\n", topic1.Name, topic1.Id, topic2.Name, topic2.Id)
	time.Sleep(actionDelay)

	fmt.Println("\nListing topics.")
	ctx, cancel = withTimeout(baseCtx, *timeoutPtr)
	topics, err := grpcTailClient.ListTopics(ctx, &emptypb.Empty{})
	cancel()
	if err != nil {
		fmt.Printf("ListTopics failed: %v\n", err)
		return
	}
	fmt.Println("Topics:")
	for _, topic := range topics.Topics {
		fmt.Printf("- %v (ID: %v)\n", topic.Name, topic.Id)
	}
	time.Sleep(actionDelay)

	fmt.Println("\nSubscribing to topics (user janez).")
	ctx, cancel = withTimeout(baseCtx, *timeoutPtr)
	subNode, err := grpcHeadClient.GetSubscriptionNode(ctx, &pb.SubscriptionNodeRequest{UserId: user.Id, TopicId: []int64{topic1.Id, topic2.Id}})
	cancel()
	if err != nil {
		fmt.Printf("GetSubscriptionNode failed: %v\n", err)
		return
	}
	fmt.Printf("Subscribed to topics with token: %v from node %v\n", subNode.SubscribeToken, subNode.Node.NodeId)
	time.Sleep(actionDelay)

	// Start subscription stream for janez (auto-stops after a few events or after sub timeout).
	{
		subConn, err := dial(subNode.Node.Address)
		if err != nil {
			fmt.Printf("Subscription dial failed: %v\n", err)
			return
		}
		grpcSubClient := pb.NewMessageBoardClient(subConn)
		subCtx, subCancel := context.WithTimeout(baseCtx, *subSecondsPtr)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer subCancel()
			defer subConn.Close()
			fmt.Println("\n[janez] Starting to listen for new messages...")
			stream, err := grpcSubClient.SubscribeTopic(subCtx, &pb.SubscribeTopicRequest{TopicId: []int64{topic1.Id, topic2.Id}, UserId: user.Id, FromMessageId: 0, SubscribeToken: subNode.SubscribeToken})
			if err != nil {
				fmt.Printf("[janez] SubscribeTopic failed: %v\n", err)
				subDone <- struct{}{}
				return
			}
			recvEvents("janez", stream, 0, subDone)
		}()
	}

	fmt.Println("\nPosting message1.")
	ctx, cancel = withTimeout(baseCtx, *timeoutPtr)
	message1, err := grpcHeadClient.PostMessage(ctx, &pb.PostMessageRequest{Text: "Prvo sporočilo v prvi temi", TopicId: topic1.Id, UserId: user.Id, RequestId: newRequestID("post")})
	cancel()
	if err != nil {
		fmt.Printf("PostMessage failed: %v\n", err)
		return
	}
	fmt.Printf("Posted message: %v (ID: %v)\n", message1.Text, message1.Id)
	time.Sleep(actionDelay)

	fmt.Println("\nPosting message2.")
	ctx, cancel = withTimeout(baseCtx, *timeoutPtr)
	message2, err := grpcHeadClient.PostMessage(ctx, &pb.PostMessageRequest{Text: "Drugo sporočilo v prvi temi", TopicId: topic1.Id, UserId: user.Id, RequestId: newRequestID("post")})
	cancel()
	if err != nil {
		fmt.Printf("PostMessage failed: %v\n", err)
		return
	}
	fmt.Printf("Posted message: %v (ID: %v)\n", message2.Text, message2.Id)
	time.Sleep(actionDelay)

	fmt.Println("\nPrinting messages.")
	ctx, cancel = withTimeout(baseCtx, *timeoutPtr)
	messages, err := grpcTailClient.GetMessages(ctx, &pb.GetMessagesRequest{TopicId: topic1.Id, FromMessageId: 0, Limit: 0})
	cancel()
	if err != nil {
		fmt.Printf("GetMessages failed: %v\n", err)
		return
	}
	fmt.Println("Messages in topic 1:")
	for _, message := range messages.Messages {
		fmt.Printf("- %v (ID: %v, Likes: %v)\n", message.Text, message.Id, message.Likes)
	}
	time.Sleep(actionDelay)

	fmt.Println("\nPosting message3.")
	ctx, cancel = withTimeout(baseCtx, *timeoutPtr)
	message3, err := grpcHeadClient.PostMessage(ctx, &pb.PostMessageRequest{Text: "Prvo sporočilo v drugi temi", TopicId: topic2.Id, UserId: user.Id, RequestId: newRequestID("post")})
	cancel()
	if err != nil {
		fmt.Printf("PostMessage failed: %v\n", err)
		return
	}
	fmt.Printf("Posted message: %v (ID: %v)\n", message3.Text, message3.Id)
	time.Sleep(actionDelay)

	fmt.Println("\nUpdating message2.")
	ctx, cancel = withTimeout(baseCtx, *timeoutPtr)
	_, err = grpcHeadClient.UpdateMessage(ctx, &pb.UpdateMessageRequest{TopicId: topic1.Id, UserId: user.Id, MessageId: message2.Id, Text: "Posodobljeno drugo sporočilo v prvi temi", RequestId: newRequestID("update")})
	cancel()
	if err != nil {
		fmt.Printf("UpdateMessage failed: %v\n", err)
		return
	}
	fmt.Println("Message2 updated.")
	time.Sleep(actionDelay)

	// Authorization error demo: wrong user updates
	ctx, cancel = withTimeout(baseCtx, *timeoutPtr)
	_, err = grpcHeadClient.UpdateMessage(ctx, &pb.UpdateMessageRequest{TopicId: topic1.Id, UserId: user1.Id, MessageId: message2.Id, Text: "this should fail", RequestId: newRequestID("update")})
	cancel()
	fmt.Printf("Update by non-author (expected error): %v\n", err)
	time.Sleep(actionDelay)

	fmt.Println("\nPrinting messages from message ID 2.")
	ctx, cancel = withTimeout(baseCtx, *timeoutPtr)
	messages, err = grpcTailClient.GetMessages(ctx, &pb.GetMessagesRequest{TopicId: topic1.Id, FromMessageId: 2, Limit: 0})
	cancel()
	if err != nil {
		fmt.Printf("GetMessages failed: %v\n", err)
		return
	}
	fmt.Println("Messages in topic 1 from message ID 2:")
	for _, message := range messages.Messages {
		fmt.Printf("- %v (ID: %v, Likes: %v)\n", message.Text, message.Id, message.Likes)
	}
	time.Sleep(actionDelay)

	fmt.Println("\nDeleting message1.")
	deleteRID := newRequestID("delete")
	ctx, cancel = withTimeout(baseCtx, *timeoutPtr)
	_, err = grpcHeadClient.DeleteMessage(ctx, &pb.DeleteMessageRequest{TopicId: topic1.Id, UserId: user.Id, MessageId: message1.Id, RequestId: deleteRID})
	cancel()
	if err != nil {
		fmt.Printf("DeleteMessage failed: %v\n", err)
		return
	}
	fmt.Println("Message1 deleted.")
	time.Sleep(actionDelay)

	// Idempotent delete: repeat with same request_id.
	ctx, cancel = withTimeout(baseCtx, *timeoutPtr)
	_, err = grpcHeadClient.DeleteMessage(ctx, &pb.DeleteMessageRequest{TopicId: topic1.Id, UserId: user.Id, MessageId: message1.Id, RequestId: deleteRID})
	cancel()
	fmt.Printf("DeleteMessage idempotent: err=%v\n", err)
	time.Sleep(actionDelay)

	fmt.Println("\nPrinting messages with limit 1.")
	ctx, cancel = withTimeout(baseCtx, *timeoutPtr)
	messages, err = grpcTailClient.GetMessages(ctx, &pb.GetMessagesRequest{TopicId: topic1.Id, FromMessageId: 0, Limit: 1})
	cancel()
	if err != nil {
		fmt.Printf("GetMessages failed: %v\n", err)
		return
	}
	fmt.Println("Messages in topic 1 with limit 1:")
	for _, message := range messages.Messages {
		fmt.Printf("- %v (ID: %v, Likes: %v)\n", message.Text, message.Id, message.Likes)
	}
	time.Sleep(actionDelay)

	fmt.Println("\nLiking message3.")
	ctx, cancel = withTimeout(baseCtx, *timeoutPtr)
	liked, err := grpcHeadClient.LikeMessage(ctx, &pb.LikeMessageRequest{TopicId: topic2.Id, UserId: user.Id, MessageId: message3.Id, RequestId: newRequestID("like")})
	cancel()
	if err != nil {
		fmt.Printf("LikeMessage failed: %v\n", err)
		return
	}
	fmt.Println("Message3 liked.")
	fmt.Printf("Message3 likes=%d committed=%v\n", liked.GetLikes(), liked.GetCommitted())
	time.Sleep(actionDelay)

	fmt.Println("\nPrinting messages.")
	ctx, cancel = withTimeout(baseCtx, *timeoutPtr)
	messagesTopic2, err := grpcTailClient.GetMessages(ctx, &pb.GetMessagesRequest{TopicId: topic2.Id, FromMessageId: 0, Limit: 0})
	cancel()
	if err != nil {
		fmt.Printf("GetMessages failed: %v\n", err)
		return
	}
	fmt.Println("Messages in topic 2:")
	for _, message := range messagesTopic2.Messages {
		fmt.Printf("- %v (ID: %v, Likes: %v)\n", message.Text, message.Id, message.Likes)
	}
	time.Sleep(actionDelay)

	fmt.Println("\nSubscribing to topics (user Miha).")
	ctx, cancel = withTimeout(baseCtx, *timeoutPtr)
	subNode1, err := grpcHeadClient.GetSubscriptionNode(ctx, &pb.SubscriptionNodeRequest{UserId: user1.Id, TopicId: []int64{topic1.Id, topic2.Id}})
	cancel()
	if err != nil {
		fmt.Printf("GetSubscriptionNode failed: %v\n", err)
		return
	}
	fmt.Printf("Subscribed to topics with token: %v from node %v\n", subNode1.SubscribeToken, subNode1.Node.NodeId)
	time.Sleep(actionDelay)

	{
		subConn, err := dial(subNode1.Node.Address)
		if err != nil {
			fmt.Printf("Subscription dial failed: %v\n", err)
			return
		}
		grpcSubClient := pb.NewMessageBoardClient(subConn)
		subCtx, subCancel := context.WithTimeout(baseCtx, *subSecondsPtr)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer subCancel()
			defer subConn.Close()
			fmt.Println("\n[Miha] Starting to listen for new messages...")
			stream, err := grpcSubClient.SubscribeTopic(subCtx, &pb.SubscribeTopicRequest{TopicId: []int64{topic1.Id, topic2.Id}, UserId: user1.Id, FromMessageId: 0, SubscribeToken: subNode1.SubscribeToken})
			if err != nil {
				fmt.Printf("[Miha] SubscribeTopic failed: %v\n", err)
				subDone <- struct{}{}
				return
			}
			recvEvents("Miha", stream, 0, subDone)
		}()
	}

	// Wait for subscriptions to finish (either by receiving events or timing out).
	<-subDone
	<-subDone
	wg.Wait()
	fmt.Println("\nClient finished.")
}
