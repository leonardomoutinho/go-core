package kafka

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/braiphub/go-core/queue"
	"gocloud.dev/pubsub"
	"gocloud.dev/pubsub/kafkapubsub"
)

type Kafka2 struct {
	brokers       []string
	clientID      string
	consumerGroup string
}

func New(brokers []string, clientID, consumerGroup string) (*Kafka2, error) {
	return &Kafka2{
		brokers:       brokers,
		clientID:      clientID,
		consumerGroup: consumerGroup,
	}, nil
}

func (kafka *Kafka2) Publish(ctx context.Context, topic string, msg queue.Message) error {
	config := kafkapubsub.MinimalConfig()
	config.ClientID = kafka.clientID

	// Construct a *pubsub.Topic.
	kafkaTopic, err := kafkapubsub.OpenTopic(kafka.brokers, config, topic, nil)
	if err != nil {
		return err
	}
	defer kafkaTopic.Shutdown(ctx)

	kafkaMsg := &pubsub.Message{
		Body: msg.Marshal(),
	}

	if err := kafkaTopic.Send(ctx, kafkaMsg); err != nil {
		return err
	}

	return nil
}

func (kafka *Kafka2) Subscribe(ctx context.Context, topic, retry string, f func(queue.Message) error) {
	config := kafkapubsub.MinimalConfig()
	config.ClientID = kafka.clientID

	// Construct a *pubsub.Subscription, joining the consumer group "kafka.consumerGroup"
	// and receiving messages from "topic".
	subscription, err := kafkapubsub.OpenSubscription(
		kafka.brokers, config, kafka.consumerGroup, []string{topic}, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer subscription.Shutdown(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Millisecond):
		}

		kafkaMsg, err := subscription.Receive(ctx)
		if err != nil {
			println("error receiving msg: " + err.Error())
			continue
		}

		var msg queue.Message
		if err := json.Unmarshal(kafkaMsg.Body, &msg); err != nil {
			// rmq.logger.Error("unmarshal message", err, log.Any("topic", topic), log.Any("body", string(d.Body)))
			if err := kafka.Publish(ctx, retry, msg); err != nil {
				// rmq.logger.Error("unmarshal message", err, log.Any("topic", topic), log.Any("body", string(d.Body)))
				continue
			}
		}

		if err := f(msg); err != nil {
			// rmq.logger.Error("nacked message", err, log.Any("topic", topic), log.Error(err))
			if err := kafka.Publish(ctx, retry, msg); err != nil {
				// rmq.logger.Error("unmarshal message", err, log.Any("topic", topic), log.Any("body", string(d.Body)))
				continue
			}
		}

		kafkaMsg.Ack()
	}

	// c, err := kafkaConfluent.NewConsumer(&kafkaConfluent.ConfigMap{
	// 	"bootstrap.servers":      kafka.server,
	// 	"group.id":               kafka.consumerGroup,
	// 	"auto.offset.reset":      "earliest",
	// 	"go.logs.channel.enable": true,
	// })
	// if err != nil {
	// 	return
	// }
	// defer c.Close()

	// c.SubscribeTopics([]string{topic}, nil)

	// for {
	// 	kafkaMsg, err := c.ReadMessage(time.Second)
	// 	if err != nil && err.(kafkaConfluent.Error).IsTimeout() {
	// 		// The client will automatically try to recover from all errors.
	// 		// Timeout is not considered an error because it is raised by
	// 		// ReadMessage in absence of messages.
	// 		continue
	// 	}
	// 	if err != nil {
	// 		fmt.Printf("Consumer error: %v (%v)\n", err, kafkaMsg)
	// 		continue
	// 	}

	// 	fmt.Printf("Message on %s: %s\n", kafkaMsg.TopicPartition, string(kafkaMsg.Value))
	// 	var msg queue.Message

	// 	if err := json.Unmarshal(kafkaMsg.Value, &msg); err != nil {
	// 		// rmq.logger.Error("unmarshal message", err, log.Any("topic", topic), log.Any("body", string(d.Body)))

	// 		if err := kafka.Publish(ctx, retry, msg); err != nil {
	// 			// rmq.logger.Error("unmarshal message", err, log.Any("topic", topic), log.Any("body", string(d.Body)))
	// 			continue
	// 		}
	// 	}

	// 	if err := f(msg); err != nil {
	// 		// rmq.logger.Error("nacked message", err, log.Any("topic", topic), log.Error(err))

	// 		if err := kafka.Publish(ctx, retry, msg); err != nil {
	// 			// rmq.logger.Error("unmarshal message", err, log.Any("topic", topic), log.Any("body", string(d.Body)))
	// 			continue
	// 		}
	// 	}
	// 	// rmq.logger.Debug("consumed", log.Any("topic", topic))

	// 	c.CommitMessage(kafkaMsg)
	// }

}
