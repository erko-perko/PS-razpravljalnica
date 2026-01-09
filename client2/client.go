package main

import (
	"context"
	"flag"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	pb "razpravljalnica/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
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

	grpcHeadClient := pb.NewMessageBoardClient(headConn)
	grpcTailClient := pb.NewMessageBoardClient(tailConn)

	// --- Basic negative tests ---
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

	// IMPORTANT: different usernames from the other client.
	userNameA := "janez-2"
	userNameB := "miha-2"

	fmt.Println("\nCreating user A.")
	createUserRID := newRequestID("create-user")
	ctx, cancel := withTimeout(baseCtx, *timeoutPtr)
	user, err := grpcHeadClient.CreateUser(ctx, &pb.CreateUserRequest{Name: userNameA, RequestId: createUserRID})
	cancel()
	if err != nil {
		fmt.Printf("CreateUser failed: %v\n", err)
		return
	}
	fmt.Printf("Created user: %v (ID: %v)\n", user.Name, user.Id)
	time.Sleep(actionDelay)

	// Idempotency demo
	ctx, cancel = withTimeout(baseCtx, *timeoutPtr)
	userAgain, err := grpcHeadClient.CreateUser(ctx, &pb.CreateUserRequest{Name: userNameA, RequestId: createUserRID})
	cancel()
	fmt.Printf("CreateUser idempotent: id=%d err=%v\n", userAgain.GetId(), err)
	time.Sleep(actionDelay)

	fmt.Println("\nCreating user B.")
	ctx, cancel = withTimeout(baseCtx, *timeoutPtr)
	user1, err := grpcHeadClient.CreateUser(ctx, &pb.CreateUserRequest{Name: userNameB, RequestId: newRequestID("create-user")})
	cancel()
	if err != nil {
		fmt.Printf("CreateUser failed: %v\n", err)
		return
	}
	fmt.Printf("Created user: %v (ID: %v)\n", user1.Name, user1.Id)
	time.Sleep(actionDelay)

	fmt.Println("\nCreating topics.")
	ctx, cancel = withTimeout(baseCtx, *timeoutPtr)
	topic1, err := grpcHeadClient.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "Tretja tema", RequestId: newRequestID("create-topic")})
	cancel()
	if err != nil {
		fmt.Printf("CreateTopic failed: %v\n", err)
		return
	}
	time.Sleep(actionDelay)
	ctx, cancel = withTimeout(baseCtx, *timeoutPtr)
	topic2, err := grpcHeadClient.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "Četrta tema", RequestId: newRequestID("create-topic")})
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

	fmt.Println("\nSubscribing to topics (user A).")
	ctx, cancel = withTimeout(baseCtx, *timeoutPtr)
	subNode, err := grpcHeadClient.GetSubscriptionNode(ctx, &pb.SubscriptionNodeRequest{UserId: user.Id, TopicId: []int64{topic1.Id, topic2.Id, 1}})
	cancel()
	if err != nil {
		fmt.Printf("GetSubscriptionNode failed: %v\n", err)
		return
	}
	fmt.Printf("Subscribed with token from node %v\n", subNode.Node.NodeId)
	time.Sleep(actionDelay)

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
			fmt.Println("\n[janez-2] Starting to listen for new messages...")
			stream, err := grpcSubClient.SubscribeTopic(subCtx, &pb.SubscribeTopicRequest{TopicId: []int64{topic1.Id, topic2.Id, 1}, UserId: user.Id, FromMessageId: 0, SubscribeToken: subNode.SubscribeToken})
			if err != nil {
				fmt.Printf("[janez-2] SubscribeTopic failed: %v\n", err)
				subDone <- struct{}{}
				return
			}
			recvEvents("janez-2", stream, 6, subDone)
		}()
	}

	fmt.Println("\nPosting message1.")
	ctx, cancel = withTimeout(baseCtx, *timeoutPtr)
	message1, err := grpcHeadClient.PostMessage(ctx, &pb.PostMessageRequest{Text: "Prvo sporočilo v tretji temi", TopicId: topic1.Id, UserId: user.Id, RequestId: newRequestID("post")})
	cancel()
	if err != nil {
		fmt.Printf("PostMessage failed: %v\n", err)
		return
	}
	fmt.Printf("Posted message: %v (ID: %v)\n", message1.Text, message1.Id)
	time.Sleep(actionDelay)

	fmt.Println("\nPosting message2.")
	ctx, cancel = withTimeout(baseCtx, *timeoutPtr)
	message2, err := grpcHeadClient.PostMessage(ctx, &pb.PostMessageRequest{Text: "Drugo sporočilo v tretji temi", TopicId: topic1.Id, UserId: user.Id, RequestId: newRequestID("post")})
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
	fmt.Println("Messages in topic 3:")
	for _, message := range messages.Messages {
		fmt.Printf("- %v (ID: %v, Likes: %v)\n", message.Text, message.Id, message.Likes)
	}
	time.Sleep(actionDelay)

	fmt.Println("\nPosting message3.")
	ctx, cancel = withTimeout(baseCtx, *timeoutPtr)
	message3, err := grpcHeadClient.PostMessage(ctx, &pb.PostMessageRequest{Text: "Prvo sporočilo v četrti temi", TopicId: topic2.Id, UserId: user.Id, RequestId: newRequestID("post")})
	cancel()
	if err != nil {
		fmt.Printf("PostMessage failed: %v\n", err)
		return
	}
	fmt.Printf("Posted message: %v (ID: %v)\n", message3.Text, message3.Id)
	time.Sleep(actionDelay)

	fmt.Println("\nUpdating message2.")
	ctx, cancel = withTimeout(baseCtx, *timeoutPtr)
	_, err = grpcHeadClient.UpdateMessage(ctx, &pb.UpdateMessageRequest{TopicId: topic1.Id, UserId: user.Id, MessageId: message2.Id, Text: "Posodobljeno drugo sporočilo v tretji temi", RequestId: newRequestID("update")})
	cancel()
	if err != nil {
		fmt.Printf("UpdateMessage failed: %v\n", err)
		return
	}
	fmt.Println("Message2 updated.")
	time.Sleep(actionDelay)

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
	fmt.Println("Messages in topic 3 from message ID 2:")
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
	fmt.Println("Messages in topic 3 with limit 1:")
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
	fmt.Println("Messages in topic 4:")
	for _, message := range messagesTopic2.Messages {
		fmt.Printf("- %v (ID: %v, Likes: %v)\n", message.Text, message.Id, message.Likes)
	}
	time.Sleep(actionDelay)

	fmt.Println("\nSubscribing to topics (user B).")
	ctx, cancel = withTimeout(baseCtx, *timeoutPtr)
	subNode1, err := grpcHeadClient.GetSubscriptionNode(ctx, &pb.SubscriptionNodeRequest{UserId: user1.Id, TopicId: []int64{topic1.Id, topic2.Id, 1, 2}})
	cancel()
	if err != nil {
		fmt.Printf("GetSubscriptionNode failed: %v\n", err)
		return
	}
	fmt.Printf("Subscribed with token from node %v\n", subNode1.Node.NodeId)
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
			fmt.Println("\n[miha-2] Starting to listen for new messages...")
			stream, err := grpcSubClient.SubscribeTopic(subCtx, &pb.SubscribeTopicRequest{TopicId: []int64{topic1.Id, topic2.Id, 1, 2}, UserId: user1.Id, FromMessageId: 0, SubscribeToken: subNode1.SubscribeToken})
			if err != nil {
				fmt.Printf("[miha-2] SubscribeTopic failed: %v\n", err)
				subDone <- struct{}{}
				return
			}
			recvEvents("miha-2", stream, 6, subDone)
		}()
	}

	<-subDone
	<-subDone
	wg.Wait()
	fmt.Println("\nClient finished.")
}
