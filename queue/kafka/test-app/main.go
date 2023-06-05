package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/braiphub/go-core/queue"
	"github.com/braiphub/go-core/queue/rabbitmq"
)

func main() {
	// test-topic
	const server = "localhost:9092"
	const consumerGroup = "task-name"
	const topic = "braip.service-name.topic"
	const topicDLQ = "braip.service-name.topic.dlq"
	const paralelPublishers = 3
	const parallelConsumers = 3

	fmt.Println("---------------------------------------------")
	fmt.Println("Press the Enter Key to stop anytime")
	fmt.Println("---------------------------------------------")

	q, err := rabbitmq.New(server, "my-app-name", consumerGroup)
	if err != nil {
		panic(err)
	}
	start := time.Now()

	// process received messages
	totalReceived := 0
	for i := 0; i < parallelConsumers; i++ {
		go func(q queue.QueueI) {
			q.Subscribe(context.Background(), topic, topicDLQ, func(m queue.Message) error {
				// insert your business logic here
				totalReceived++
				if totalReceived%2 == 0 {
					// println("message moved do dlx")
					return errors.New("unknown")
				}

				// println("consumed message")
				return nil
			})
		}(q)
	}

	totalPublished := 0
	for i := 0; i < paralelPublishers; i++ {
		go func() {
			for {
				err = q.Publish(
					context.Background(),
					topic,
					queue.Message{
						Metadata: map[string]string{
							"key1":  "val1",
							"index": strconv.Itoa(1),
						},
						Body: []byte("message payload"),
					},
				)
				if err != nil {
					panic(err)
				}
				totalPublished++
			}
		}()
	}

	fmt.Scanln()
	fmt.Printf("Published messages: %d\n", totalPublished)
	fmt.Printf("Processed messages: %d\n", totalReceived)
	fmt.Printf("Time elapsed (secs): %s\n", time.Since(start).String())
}
