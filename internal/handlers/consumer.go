package handlers

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rs/zerolog"
)

type Handler struct {
	log *zerolog.Logger
}

func NewConsumerHandler(logger *zerolog.Logger) *Handler {
	return &Handler{
		log: logger,
	}
}

func (h *Handler) HandleMessage(message []byte, topic kafka.TopicPartition, cn int) error {
	h.log.Info().Msgf("Consumer #%d, Message from kafka with offset %d '%s' on partition %d", cn, topic.Offset, string(message), topic.Partition)
	return nil
}

func (h *Handler) HandleBatch(messages []kafka.Message, cn int) error {
	h.log.Info().Int("consumerNumber", cn).Int("batchSize", len(messages)).Msg("Handling batch of Kafka messages")

	// Проходим по всем сообщениям в батче и обрабатываем их
	for _, message := range messages {
		h.log.Info().Msgf("Consumer #%d, Message from Kafka with offset %d '%s' on partition %d",
			cn, message.TopicPartition.Offset, string(message.Value), message.TopicPartition.Partition)
	}

	return nil
}
