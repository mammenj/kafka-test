package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"

	//"strings"
	//"github.com/google/uuid"
	//"time"
	kafka "github.com/segmentio/kafka-go"
)

func newKafkaWriter(kafkaURL, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:  kafka.TCP(kafkaURL),
		Topic: topic,
		//Balancer: &kafka.LeastBytes{},
		Balancer: kafka.Murmur2Balancer{},
	}
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	// read flag for order number
	var orderNumber string
	flag.StringVar(&orderNumber, "o", "", "order key/id")
	flag.Parse()

	if orderNumber == "" {
		log.Fatal("Please enter a order key --o key1")
	}

	// get kafka reader using environment variables.
	kafkaURL := os.Getenv("kafkaURL")
	topic := os.Getenv("topic")
	writer := newKafkaWriter(kafkaURL, topic)
	defer writer.Close()
	fmt.Println("start producing ... !!")
	for i := 0; i < 100; i++ {
		//key := fmt.Sprintf("orderid-%d",i)
		key := orderNumber
		seq := fmt.Sprintf("seq-%d", i)
		msg := kafka.Message{
			Key:   []byte(key),
			Value: []byte(fmt.Sprint(seq + " :message payload")),
		}
		err := writer.WriteMessages(context.Background(), msg)
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Printf("--> produced key: %v,  seq:%v \n", key, seq)
		}
		//time.Sleep(1 * time.Second)
	}

}
