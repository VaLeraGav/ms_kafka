package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rs/zerolog"
)

const (
	flushTimeoutMs = 5 * 1000 // ms
)

var errUnknownType = errors.New("unknown event type")

type Producer struct {
	log      *zerolog.Logger
	producer *kafka.Producer
}

func NewProducer(log *zerolog.Logger, kafkaAddress string) (*Producer, error) {
	conf := &kafka.ConfigMap{
		"bootstrap.servers":            kafkaAddress,
		"acks":                         "all",
		"queue.buffering.max.messages": 5000000, // параметр определяет максимальное количество сообщений, которые могут находиться в очереди для отправки на брокер.
	}

	p, err := kafka.NewProducer(conf)
	if err != nil {
		return nil, fmt.Errorf("error with new producer: %w", err)
	}

	log.Log().Str("address", kafkaAddress).Msg("kafka servers start")

	return &Producer{
		log:      log,
		producer: p,
	}, nil
}

func (p *Producer) SendBatchToTopicAsync(topicName string, messages [][]byte) error {
	var wg sync.WaitGroup
	errChan := make(chan error, len(messages))

	for _, msgValue := range messages {
		wg.Add(1)

		go func(value []byte) {
			defer wg.Done()
			if err := p.SendMsgToTopic(topicName, value); err != nil {
				p.log.Error().Err(err).Msg("failed to produce message asynchronously")
				errChan <- err
			}
		}(msgValue)
	}

	wg.Wait()

	close(errChan)

	for err := range errChan {
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *Producer) SendMsgToTopic(topicName string, msgValue []byte) error {
	msg := p.produceMessage(topicName, msgValue)
	deliveryChan := make(chan kafka.Event)
	defer close(deliveryChan)

	if err := p.producer.Produce(msg, deliveryChan); err != nil {
		return err
	}

	e := <-deliveryChan
	switch ev := e.(type) {
	case *kafka.Message:
		if ev.TopicPartition.Error != nil {
			return fmt.Errorf(
				"failed to deliver message to topic %s, partition %d, offset %v: %v",
				*ev.TopicPartition.Topic,
				ev.TopicPartition.Partition,
				ev.TopicPartition.Offset,
				ev.TopicPartition.Error,
			)
		}
		return nil
	case kafka.Error:
		return ev
	default:
		return errUnknownType
	}
}

func (p *Producer) produceMessage(topicName string, msgValue []byte) *kafka.Message {
	return &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topicName,
			Partition: kafka.PartitionAny,
		},
		Value: msgValue,
	}
}

func (p *Producer) Shutdown(ctx context.Context) error {
	remaining := p.producer.Flush(flushTimeoutMs)
	if remaining > 0 {
		p.log.Warn().Int("messages_remaining", remaining).Msg("Some messages were not delivered before shutdown")
	}
	p.producer.Close()
	return nil
}
