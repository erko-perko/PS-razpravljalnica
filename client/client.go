package main

import (
	"context"
	"fmt"
	"flag"
	"time"

	pb "razpravljalnica/pb" //import pb.go datoteke

	"google.golang.org/grpc"                             //glavna knjižnica za grpc
	"google.golang.org/protobuf/types/known/emptypb"     //import zaradi google.protobuf.Empty, ki ga uporabimo v metodah, kazalec na "nic"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// preberemo argumente iz ukazne vrstice
	sPtr := flag.String("s", "", "server URL")
	pPtr := flag.Int("p", 50051, "port number")
	flag.Parse()

	// zaženemo strežnik ali odjemalca
	url := fmt.Sprintf("%v:%v", *sPtr, *pPtr)
	
	fmt.Printf("gRPC client connecting to %v\n", url)
	conn, err := grpc.NewClient(url, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// vzpostavimo izvajalno okolje
	contextCRUD, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// vzpostavimo vmesnik gRPC
	grpcClient := pb.NewMessageBoardClient(conn)

	user, err := grpcClient.CreateUser(contextCRUD, &pb.CreateUserRequest{Name: "janez"})
	if err != nil {
		panic(err)
	}

	topic1, err := grpcClient.CreateTopic(contextCRUD, &pb.CreateTopicRequest{Name: "Prva tema"})
	if err != nil {
		panic(err)
	}

	topic2, err := grpcClient.CreateTopic(contextCRUD, &pb.CreateTopicRequest{Name: "Druga tema"})
	if err != nil {
		panic(err)
	}

	topics, err := grpcClient.ListTopics(contextCRUD, &emptypb.Empty{})
	if err != nil {
		panic(err)
	}
	fmt.Println("Topics:")
	for _, topic := range topics.Topics {
		fmt.Printf("- %v (ID: %v)\n", topic.Name, topic.Id)
	}

	ready := make(chan bool)
	subNode, err := grpcClient.GetSubscriptionNode(contextCRUD, &pb.SubscriptionNodeRequest{UserId: user.Id, TopicId: []int64{topic1.Id, topic2.Id},})
	if err != nil {
		panic(err)
	}
	ready <- true

	go func() {
		<-ready
		if stream, err := grpcClient.SubscribeTopic(contextCRUD, &pb.SubscribeTopicRequest{TopicId: []int64{topic1.Id, topic2.Id}, UserId: user.Id, FromMessageId: 0, SubscribeToken: subNode.SubscribeToken}); err != nil {
			panic(err)
		} else {
			for {
				messageEvent, err := stream.Recv()
				if err != nil {
					fmt.Println("No more action.")
					return
				}
				fmt.Printf("%v %v: #%v %v\n", messageEvent.EventAt, messageEvent.Op, messageEvent.SequenceNumber, messageEvent.Message)
			}
		}
	}()

	message1, err := grpcClient.PostMessage(contextCRUD, &pb.PostMessageRequest{Text: "Prvo sporočilo v prvi temi", TopicId: topic1.Id, UserId: user.Id,})
	if err != nil {
		panic(err)
	}

	message2, err := grpcClient.PostMessage(contextCRUD, &pb.PostMessageRequest{Text: "Drugo sporočilo v prvi temi", TopicId: topic1.Id, UserId: user.Id,})
	if err != nil {
		panic(err)
	}

	messages, err := grpcClient.GetMessages(contextCRUD, &pb.GetMessagesRequest{TopicId: topic1.Id, FromMessageId: 0, Limit: 0,})
	if err != nil {
		panic(err)
	}
	fmt.Println("Messages in topic 1:")
	for _, message := range messages.Messages {
		fmt.Printf("- %v (ID: %v, Likes: %v)\n", message.Text, message.Id, message.Likes)
	}

	message3, err := grpcClient.PostMessage(contextCRUD, &pb.PostMessageRequest{Text: "Prvo sporočilo v drugi temi", TopicId: topic2.Id, UserId: user.Id,})
	if err != nil {
		panic(err)
	}

	_, err = grpcClient.UpdateMessage(contextCRUD, &pb.UpdateMessageRequest{TopicId: topic1.Id, UserId: user.Id, MessageId: message2.Id, Text: "Posodobljeno drugo sporočilo v prvi temi",})
	if err != nil {
		panic(err)
	}

	messages, err = grpcClient.GetMessages(contextCRUD, &pb.GetMessagesRequest{TopicId: topic1.Id, FromMessageId: 2, Limit: 0,})
	if err != nil {
		panic(err)
	}
	fmt.Println("Messages in topic 1 from message ID 2:")
	for _, message := range messages.Messages {
		fmt.Printf("- %v (ID: %v, Likes: %v)\n", message.Text, message.Id, message.Likes)
	}

	_, err = grpcClient.DeleteMessage(contextCRUD, &pb.DeleteMessageRequest{TopicId: topic1.Id, UserId: user.Id, MessageId: message1.Id,})
	if err != nil {
		panic(err)
	}

	messages, err = grpcClient.GetMessages(contextCRUD, &pb.GetMessagesRequest{TopicId: topic1.Id, FromMessageId: 0, Limit: 1,})
	if err != nil {
		panic(err)
	}
	fmt.Println("Messages in topic 1 with limit 1:")
	for _, message := range messages.Messages {
		fmt.Printf("- %v (ID: %v, Likes: %v)\n", message.Text, message.Id, message.Likes)
	}

	_, err = grpcClient.LikeMessage(contextCRUD, &pb.LikeMessageRequest{TopicId: topic2.Id, UserId: user.Id, MessageId: message3.Id,})
	if err != nil {
		panic(err)
	}
}