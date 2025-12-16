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

func main() {
	// preberemo argumente iz ukazne vrstice
	sPtr := flag.String("s", "", "server URL")
	pPtr := flag.Int("p", 9876, "port number")
	flag.Parse()

	// zaženemo strežnik ali odjemalca
	url := fmt.Sprintf("%v:%v", *sPtr, *pPtr)
	
	conn, err := grpc.NewClient(url, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()

	// vzpostavimo izvajalno okolje
	contextCRUD, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// vzpostavimo vmesnik gRPC
	grpcClient := razpravljalnica.NewMessageBoardClient(conn)

	user, err := grpcClient.CreateUser(contextCRUD, &razpravljalnica.CreateUserRequest{Username: "janez"})
	if err != nil {
		log.Fatalf("Could not create user: %v", err)
	}

	topic1, err := grpcClient.CreateTopic(contextCRUD, &razpravljalnica.CreateTopicRequest{Name: "Prva tema"})
	if err != nil {
		log.Fatalf("Could not create topic: %v", err)
	}

	topic2, err := grpcClient.CreateTopic(contextCRUD, &razpravljalnica.CreateTopicRequest{Name: "Druga tema"})
	if err != nil {
		log.Fatalf("Could not create topic: %v", err)
	}

	topics, err := grpcClient.ListTopics(contextCRUD, &emptypb.Empty{})
	if err != nil {
		log.Fatalf("Could not list topics: %v", err)
	}
	fmt.Println("Topics:")
	for _, topic := range topics.Topics {
		fmt.Printf("- %v (ID: %v)\n", topic.Name, topic.Id)
	}

	subNode, err := grpcClient.GetSubscriptionNode(contextCRUD, &razpravljalnica.SubscriptionNodeRequest{UserId: user.Id, TopicId: []int64{topic1.Id, topic2.Id},})
	if err != nil {
		log.Fatalf("Could not get subscription node: %v", err)
	}

	go func() {
		if stream, err := grpcClient.SubscribeTopic(contextCRUD, &razpravljalnica.SubscribeTopicRequest{TopicId: []int64{topic1.Id, topic2.Id}, UserId: user.Id, FromMessageId: 0, SubscribeToken: subNode.SubscribeToken}); err != nil {
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

	message1, err := grpcClient.PostMessage(contextCRUD, &razpravljalnica.PostMessageRequest{Text: "Prvo sporočilo v prvi temi", TopicId: topic1.Id, UserId: user.Id,})
	if err != nil {
		log.Fatalf("Could not post message: %v", err)
	}

	message2, err := grpcClient.PostMessage(contextCRUD, &razpravljalnica.PostMessageRequest{Text: "Drugo sporočilo v prvi temi", TopicId: topic1.Id, UserId: user.Id,})
	if err != nil {
		log.Fatalf("Could not post message: %v", err)
	}

	messages, err := grpcClient.GetMessages(contextCRUD, &razpravljalnica.GetMessagesRequest{TopicId: topic1.Id, FromMessageId: 0, Limit: 0,})
	if err != nil {
		log.Fatalf("Could not get messages: %v", err)
	}
	fmt.Println("Messages in topic 1:")
	for _, message := range messages.Messages {
		fmt.Printf("- %v (ID: %v, Likes: %v)\n", message.Text, message.Id, message.Likes)
	}

	message3, err := grpcClient.PostMessage(contextCRUD, &razpravljalnica.PostMessageRequest{Text: "Prvo sporočilo v drugi temi", TopicId: topic2.Id, UserId: user.Id,})
	if err != nil {
		log.Fatalf("Could not post message: %v", err)
	}

	message2_1, err := grpcClient.UpdateMessage(contextCRUD, &razpravljalnica.UpdateMessageRequest{TopicId: topic1.Id, UserId: user.Id, MessageId: message2.Id, Text: "Posodobljeno drugo sporočilo v prvi temi",})
	if err != nil {
		log.Fatalf("Could not update message: %v", err)
	}

	messages, err := grpcClient.GetMessages(contextCRUD, &razpravljalnica.GetMessagesRequest{TopicId: topic1.Id, FromMessageId: 2, Limit: 0,})
	if err != nil {
		log.Fatalf("Could not get messages: %v", err)
	}
	fmt.Println("Messages in topic 1 from message ID 2:")
	for _, message := range messages.Messages {
		fmt.Printf("- %v (ID: %v, Likes: %v)\n", message.Text, message.Id, message.Likes)
	}

	_, err := grpcClient.DeleteMessage(contextCRUD, &razpravljalnica.DeleteMessageRequest{TopicId: topic1.Id, UserId: user.Id, MessageId: message1.Id,})
	if err != nil {
		log.Fatalf("Could not delete message: %v", err)
	}

	messages, err := grpcClient.GetMessages(contextCRUD, &razpravljalnica.GetMessagesRequest{TopicId: topic1.Id, FromMessageId: 0, Limit: 1,})
	if err != nil {
		log.Fatalf("Could not get messages: %v", err)
	}
	fmt.Println("Messages in topic 1 with limit 1:")
	for _, message := range messages.Messages {
		fmt.Printf("- %v (ID: %v, Likes: %v)\n", message.Text, message.Id, message.Likes)
	}

	message3_like, err := grpcClient.LikeMessage(contextCRUD, &razpravljalnica.LikeMessageRequest{TopicId: topic2.Id, UserId: user.Id, MessageId: message3.Id,})
	if err != nil {
		log.Fatalf("Could not like message: %v", err)
	}
}