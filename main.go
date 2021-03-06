package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/joho/godotenv"

	//"github.com/google/uuid"
	"net"
	"strconv"

	kafka "github.com/segmentio/kafka-go"
)

func getKafkaReader(kafkaURL, topic, groupID string) *kafka.Reader {
	brokers := strings.Split(kafkaURL, ",")
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	var isCreateTopic bool
	flag.BoolVar(&isCreateTopic, "t", false, "Create topic flag")
	flag.Parse()
	// get kafka reader using environment variables.
	kafkaURL := os.Getenv("kafkaURL")
	topic := os.Getenv("topic")
	groupID := os.Getenv("groupID")
	fmt.Printf("Create topic flag: %v", isCreateTopic)
	if isCreateTopic {
		createTopic(kafkaURL, topic, 3)
	}
	reader := getKafkaReader(kafkaURL, topic, groupID)

	defer reader.Close()
	fmt.Println("start consuming ... !!")
	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Printf("consuming topic:%v partition:%v offset:%v key:%s value:%s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	}
}

func createTopic(kafkaURL, topic string, partition int) {

	conn, err := kafka.Dial("tcp", kafkaURL)
	if err != nil {
		panic(err.Error())
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		panic(err.Error())
	}
	var controllerConn *kafka.Conn
	controllerConn, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		panic(err.Error())
	}
	defer controllerConn.Close()

	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topic,
			NumPartitions:     partition,
			ReplicationFactor: 1,
		},
	}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		panic(err.Error())
	}
	fmt.Printf("Created topic: %v with %v partitions\n", topic, partition)
}
