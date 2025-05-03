package kafka

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
	sessionTimeout = 7000            // ms
	readTimeoutMs  = 100             // ms
	maxBatchDelay  = 2 * time.Second // задержка перед обработкой неполного батча
)

type Handler interface {
	HandleMessage(message []byte, topic kafka.TopicPartition, cn int) error
	HandleBatch(messages []kafka.Message, cn int) error
}

type Consumer struct {
	log            *zerolog.Logger
	consumer       *kafka.Consumer
	handler        Handler
	consumerNumber int
	topic          string

	stopOnce sync.Once
	stopChan chan struct{}
}

func NewConsumer(log *zerolog.Logger, handler Handler, kafkaAddress string, topic string, consumerGroup string, consumerNumber int) (*Consumer, error) {
	cfg := &kafka.ConfigMap{
		"bootstrap.servers":        kafkaAddress,
		"group.id":                 consumerGroup,
		"session.timeout.ms":       sessionTimeout,
		"enable.auto.offset.store": false,
		"enable.auto.commit":       true,
		"auto.commit.interval.ms":  5000,
		"auto.offset.reset":        "latest",
	}

	c, err := kafka.NewConsumer(cfg)
	if err != nil {
		return nil, err
	}

	if err = c.Subscribe(topic, nil); err != nil {
		return nil, err
	}

	logger := log.With().
		Int("consumerNumber", consumerNumber).
		Logger()

	return &Consumer{
		log:            &logger,
		consumer:       c,
		handler:        handler,
		consumerNumber: consumerNumber,
		topic:          topic,
		stopChan:       make(chan struct{}),
	}, nil
}

func (c *Consumer) Read(ctx context.Context) {
	c.log.Info().
		Str("topics", c.topic).
		Msg("Kafka consumer started")

	for {
		select {
		case <-ctx.Done():
			c.log.Info().Msg("Context cancelled, stopping consumer")
			return
		case <-c.stopChan:
			c.log.Info().Msg("Stop signal received, exiting consumer loop")
			return
		default:
			kafkaMsg, err := c.consumer.ReadMessage(readTimeoutMs)
			if err != nil {
				if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.IsTimeout() {
					continue
				}
				c.log.Error().Err(err).Msg("Failed to read Kafka message")
				continue
			}

			if kafkaMsg == nil {
				continue
			}

			if err = c.handler.HandleMessage(kafkaMsg.Value, kafkaMsg.TopicPartition, c.consumerNumber); err != nil {
				c.log.Error().Err(err).Str("topic", *kafkaMsg.TopicPartition.Topic).
					Msg("Failed to handle Kafka message")
				continue
			}

			if _, err = c.consumer.StoreMessage(kafkaMsg); err != nil {
				c.log.Error().Err(err).Str("topic", *kafkaMsg.TopicPartition.Topic).
					Msg("Failed to store Kafka offset")
			}
		}
	}
}

func (c *Consumer) ReadBatchAsync(ctx context.Context, batchSize int, maxConcurrentBatches int) {
	c.log.Info().Str("topic", c.topic).Msg("Kafka consumer started")

	var (
		batch           []kafka.Message
		lastMessageTime time.Time
		semaphore       = make(chan struct{}, maxConcurrentBatches)
	)

	// TODO: возможно стоит отказаться от ctx в сторону обычного flag start
	for {
		time.Sleep(100 * time.Millisecond)

		if len(batch) > 0 && c.isLastBatch(lastMessageTime) {
			c.flushBatch(&batch, semaphore)
		}

		select {
		case <-ctx.Done():
			c.log.Info().Msg("Context cancelled, stopping consumer")
			c.flushBatch(&batch, semaphore)
			c.waitForInFlightBatches(semaphore)
			return

		case <-c.stopChan:
			c.log.Info().Msg("Stop signal received, exiting consumer loop")
			c.flushBatch(&batch, semaphore)
			c.waitForInFlightBatches(semaphore)
			return

		default:
			kafkaMsg, err := c.consumer.ReadMessage(readTimeoutMs)
			if err != nil {
				if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.IsTimeout() {
					continue
				}
				c.log.Error().Err(err).Msg("Failed to read Kafka message")
				continue
			}

			if kafkaMsg == nil {
				continue
			}

			batch = append(batch, *kafkaMsg)
			lastMessageTime = time.Now()

			if len(batch) >= batchSize {
				c.flushBatch(&batch, semaphore)
			}
		}
	}
}

// Проверка, являются ли оставшиеся сообщения последними по времени
func (c *Consumer) isLastBatch(lastMessageTime time.Time) bool {
	if time.Since(lastMessageTime) > maxBatchDelay {
		c.log.Info().Float64("elapsed", time.Since(lastMessageTime).Seconds()).Msg("Last batch detected based on time delay")
		return true
	}

	return false
}

// waitForInFlightBatches блокирует завершение, пока все горутины не освободят семафор
func (c *Consumer) waitForInFlightBatches(semaphore chan struct{}) {
	for i := 0; i < cap(semaphore); i++ {
		semaphore <- struct{}{}
	}
}

// flushBatch обрабатывает пакет сообщений
func (c *Consumer) flushBatch(batch *[]kafka.Message, semaphore chan struct{}) {
	if len(*batch) == 0 {
		return
	}

	// Копируем пакет, чтобы передать в горутину
	batchCopy := make([]kafka.Message, len(*batch))
	copy(batchCopy, *batch)

	*batch = nil
	c.processBatchAsync(batchCopy, semaphore)
}

// processBatchAsync асинхронно обрабатывает пакет сообщений
func (c *Consumer) processBatchAsync(batchToProcess []kafka.Message, semaphore chan struct{}) {
	semaphore <- struct{}{}
	go func() {
		defer func() { <-semaphore }()

		if err := c.handler.HandleBatch(batchToProcess, c.consumerNumber); err != nil {
			c.log.Error().Err(err).Msg("Failed to handle Kafka message batch")
			return
		}

		// Коммитим только после успешной обработки
		var offsets []kafka.TopicPartition
		for _, msg := range batchToProcess {
			tp := msg.TopicPartition
			tp.Offset += 1
			offsets = append(offsets, tp)
		}

		if _, err := c.consumer.CommitOffsets(offsets); err != nil {
			c.log.Error().Err(err).Msg("Failed to commit offsets")
		}
	}()
}

func (c *Consumer) Shutdown(ctx context.Context) error {
	c.stopOnce.Do(func() {
		close(c.stopChan)
	})

	c.log.Info().Msg("Shutting down Kafka consumer")

	if _, err := c.consumer.Commit(); err != nil {
		c.log.Error().Err(err).Msg("Failed to commit offsets during shutdown")
	}
	if err := c.consumer.Close(); err != nil {
		c.log.Error().Err(err).Msg("Failed to close Kafka consumer")
		return err
	}

	c.log.Info().Msg("Kafka consumer shutdown complete")
	return nil
}
