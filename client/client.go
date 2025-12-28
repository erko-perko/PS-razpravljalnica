package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	pb "razpravljalnica/pb" //import pb.go datoteke

	"google.golang.org/grpc" //glavna knjižnica za grpc
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb" //import zaradi google.protobuf.Empty, ki ga uporabimo v metodah, kazalec na "nic"
)

func main() {
	// preberemo argumente iz ukazne vrstice
	sPtr := flag.String("s", "", "server URL")
	hPtr := flag.Int("h", 50051, "head port number")
	tPtr := flag.Int("t", 50053, "tail port number")
	flag.Parse()

	// zaženemo strežnik ali odjemalca
	headUrl := fmt.Sprintf("%v:%v", *sPtr, *hPtr)
	tailUrl := fmt.Sprintf("%v:%v", *sPtr, *tPtr)

	fmt.Printf("gRPC client connecting to head at %v\n", headUrl)
	headConn, err := grpc.NewClient(headUrl, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	fmt.Printf("gRPC client connecting to tail at %v\n", tailUrl)
	tailConn, err := grpc.NewClient(tailUrl, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer headConn.Close()
	defer tailConn.Close()
	// vzpostavimo izvajalno okolje
	contextCRUD, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// vzpostavimo vmesnik gRPC
	grpcHeadClient := pb.NewMessageBoardClient(headConn)
	grpcTailClient := pb.NewMessageBoardClient(tailConn)

	done := make(chan bool)

	fmt.Println("\nCreating user ID=1.")
	user, err := grpcHeadClient.CreateUser(contextCRUD, &pb.CreateUserRequest{Name: "janez"})
	if err != nil {
		panic(err)
	}
	fmt.Printf("Created user: %v (ID: %v)\n", user.Name, user.Id)

	fmt.Println("\nCreating user ID=2.")
	user1, err := grpcHeadClient.CreateUser(contextCRUD, &pb.CreateUserRequest{Name: "Miha"})
	if err != nil {
		panic(err)
	}
	fmt.Printf("Created user: %v (ID: %v)\n", user1.Name, user1.Id)

	time.Sleep(time.Second)

	fmt.Println("\nCreating topics.")
	topic1, err := grpcHeadClient.CreateTopic(contextCRUD, &pb.CreateTopicRequest{Name: "Prva tema"})
	if err != nil {
		panic(err)
	}

	time.Sleep(time.Second)

	topic2, err := grpcHeadClient.CreateTopic(contextCRUD, &pb.CreateTopicRequest{Name: "Druga tema"})
	if err != nil {
		panic(err)
	}
	fmt.Printf("Created topics: %v (ID: %v), %v (ID: %v)\n", topic1.Name, topic1.Id, topic2.Name, topic2.Id)

	time.Sleep(time.Second)

	fmt.Println("\nListing topics.")
	topics, err := grpcTailClient.ListTopics(contextCRUD, &emptypb.Empty{})
	if err != nil {
		panic(err)
	}
	fmt.Println("Topics:")
	for _, topic := range topics.Topics {
		fmt.Printf("- %v (ID: %v)\n", topic.Name, topic.Id)
	}

	time.Sleep(time.Second)

	fmt.Println("\nSubscribing to topics.")
	subNode, err := grpcHeadClient.GetSubscriptionNode(contextCRUD, &pb.SubscriptionNodeRequest{UserId: user.Id, TopicId: []int64{topic1.Id, topic2.Id}})
	if err != nil {
		panic(err)
	}
	fmt.Printf("Subscribed to topics with token: %v from node %v\n", subNode.SubscribeToken, subNode.Node.NodeId)

	go func() {
		defer func() { done <- true }()

		subConn, err := grpc.NewClient(subNode.Node.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			panic(err)
		}
		grpcSubClient := pb.NewMessageBoardClient(subConn)
		fmt.Println("\nStarting to listen for new messages...")
		if stream, err := grpcSubClient.SubscribeTopic(contextCRUD, &pb.SubscribeTopicRequest{TopicId: []int64{topic1.Id, topic2.Id}, UserId: user.Id, FromMessageId: 0, SubscribeToken: subNode.SubscribeToken}); err != nil {
			panic(err)
		} else {
			for {
				messageEvent, err := stream.Recv()
				if err != nil {
					fmt.Printf("Error receiving message event: %v\n", err)
					return
				}
				fmt.Printf("\n%v %v: Num: %v Id: %v\n", messageEvent.EventAt.AsTime().Format("2006-01-02 15:04:05"), messageEvent.Op, messageEvent.SequenceNumber, messageEvent.Message.Id)
			}
		}
	}()

	time.Sleep(time.Second)

	fmt.Println("\nPosting message1.")
	message1, err := grpcHeadClient.PostMessage(contextCRUD, &pb.PostMessageRequest{Text: "Prvo sporočilo v prvi temi", TopicId: topic1.Id, UserId: user.Id})
	if err != nil {
		panic(err)
	}
	fmt.Printf("Posted message: %v (ID: %v)\n", message1.Text, message1.Id)

	time.Sleep(time.Second)

	fmt.Println("\nPosting message2.")
	message2, err := grpcHeadClient.PostMessage(contextCRUD, &pb.PostMessageRequest{Text: "Drugo sporočilo v prvi temi", TopicId: topic1.Id, UserId: user.Id})
	if err != nil {
		panic(err)
	}
	fmt.Printf("Posted message: %v (ID: %v)\n", message2.Text, message2.Id)

	time.Sleep(time.Second)

	fmt.Println("\nPrinting messages.")
	messages, err := grpcTailClient.GetMessages(contextCRUD, &pb.GetMessagesRequest{TopicId: topic1.Id, FromMessageId: 0, Limit: 0})
	if err != nil {
		panic(err)
	}
	fmt.Println("Messages in topic 1:")
	for _, message := range messages.Messages {
		fmt.Printf("- %v (ID: %v, Likes: %v)\n", message.Text, message.Id, message.Likes)
	}

	time.Sleep(time.Second)

	fmt.Println("\nPosting message3.")
	message3, err := grpcHeadClient.PostMessage(contextCRUD, &pb.PostMessageRequest{Text: "Prvo sporočilo v drugi temi", TopicId: topic2.Id, UserId: user.Id})
	if err != nil {
		panic(err)
	}
	fmt.Printf("Posted message: %v (ID: %v)\n", message3.Text, message3.Id)

	time.Sleep(time.Second)

	fmt.Println("\nUpdating message2.")
	_, err = grpcHeadClient.UpdateMessage(contextCRUD, &pb.UpdateMessageRequest{TopicId: topic1.Id, UserId: user.Id, MessageId: message2.Id, Text: "Posodobljeno drugo sporočilo v prvi temi"})
	if err != nil {
		panic(err)
	}
	fmt.Println("Message2 updated.")

	time.Sleep(time.Second)

	fmt.Println("\nPrinting messages from message ID 2.")
	messages, err = grpcTailClient.GetMessages(contextCRUD, &pb.GetMessagesRequest{TopicId: topic1.Id, FromMessageId: 2, Limit: 0})
	if err != nil {
		panic(err)
	}
	fmt.Println("Messages in topic 1 from message ID 2:")
	for _, message := range messages.Messages {
		fmt.Printf("- %v (ID: %v, Likes: %v)\n", message.Text, message.Id, message.Likes)
	}

	time.Sleep(time.Second)

	fmt.Println("\nDeleting message1.")
	_, err = grpcHeadClient.DeleteMessage(contextCRUD, &pb.DeleteMessageRequest{TopicId: topic1.Id, UserId: user.Id, MessageId: message1.Id})
	if err != nil {
		panic(err)
	}
	fmt.Println("Message1 deleted.")

	time.Sleep(time.Second)

	fmt.Println("\nPrinting messages with limit 1.")
	messages, err = grpcTailClient.GetMessages(contextCRUD, &pb.GetMessagesRequest{TopicId: topic1.Id, FromMessageId: 0, Limit: 1})
	if err != nil {
		panic(err)
	}
	fmt.Println("Messages in topic 1 with limit 1:")
	for _, message := range messages.Messages {
		fmt.Printf("- %v (ID: %v, Likes: %v)\n", message.Text, message.Id, message.Likes)
	}

	time.Sleep(time.Second)

	fmt.Println("\nLiking message3.")
	_, err = grpcHeadClient.LikeMessage(contextCRUD, &pb.LikeMessageRequest{TopicId: topic2.Id, UserId: user.Id, MessageId: message3.Id})
	if err != nil {
		panic(err)
	}
	fmt.Println("Message3 liked.")

	time.Sleep(time.Second)

	fmt.Println("\nPrinting messages.")
	messagesTopic2, err := grpcTailClient.GetMessages(contextCRUD, &pb.GetMessagesRequest{TopicId: topic2.Id, FromMessageId: 0, Limit: 0})
	if err != nil {
		panic(err)
	}
	fmt.Println("Messages in topic 2:")
	for _, message := range messagesTopic2.Messages {
		fmt.Printf("- %v (ID: %v, Likes: %v)\n", message.Text, message.Id, message.Likes)
	}

	time.Sleep(time.Second)

	fmt.Println("\nSubscribing to topics.")
	subNode1, err := grpcHeadClient.GetSubscriptionNode(contextCRUD, &pb.SubscriptionNodeRequest{UserId: user1.Id, TopicId: []int64{topic1.Id, topic2.Id}})
	if err != nil {
		panic(err)
	}
	fmt.Printf("Subscribed to topics with token: %v from node %v\n", subNode1.SubscribeToken, subNode1.Node.NodeId)

	go func() {
		defer func() { done <- true }()

		subConn, err := grpc.NewClient(subNode.Node.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			panic(err)
		}
		grpcSubClient := pb.NewMessageBoardClient(subConn)
		fmt.Println("\nStarting to listen for new messages...")
		if stream, err := grpcSubClient.SubscribeTopic(contextCRUD, &pb.SubscribeTopicRequest{TopicId: []int64{topic1.Id, topic2.Id}, UserId: user1.Id, FromMessageId: 0, SubscribeToken: subNode1.SubscribeToken}); err != nil {
			panic(err)
		} else {
			for {
				messageEvent, err := stream.Recv()
				if err != nil {
					fmt.Printf("Error receiving message event: %v\n", err)
					return
				}
				fmt.Printf("\n%v %v: Num: %v Id: %v\n", messageEvent.EventAt.AsTime().Format("2006-01-02 15:04:05"), messageEvent.Op, messageEvent.SequenceNumber, messageEvent.Message.Id)
			}
		}
	}()

	time.Sleep(time.Second)

	<-done
}
