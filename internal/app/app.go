package app

import (
	"context"
	"time"

	"github.com/VaLeraGav/ms_kafka/internal/handlers"
	"github.com/VaLeraGav/ms_kafka/internal/kafka"
)

func RunProducer(configs *Config, projectPath string) {
	logPath := GetLogPath(projectPath)
	logger := ConfigureLogger(configs.Env, logPath)

	kafkaProducer, err := kafka.NewProducer(logger, configs.Kafka.Address)
	if err != nil {
		logger.Fatal().Err(err)
	}

	c := NewServer(configs, logger, kafkaProducer)
	httpServer := c.StartServer()

	GracefulShutdown(
		logger,
		10*time.Second,
		httpServer,
		kafkaProducer,
	)
}

func RunConsumer(topic, consumerGroup string, configs *Config, projectPath string) {
	logPath := GetLogPath(projectPath)
	logger := ConfigureLogger(configs.Env, logPath)

	h := handlers.NewConsumerHandler(logger)
	c1, err := kafka.NewConsumer(logger, h, configs.Kafka.Address, topic, consumerGroup, 1)
	if err != nil {
		logger.Fatal().Err(err)
	}

	c2, err := kafka.NewConsumer(logger, h, configs.Kafka.Address, topic, consumerGroup, 2)
	if err != nil {
		logger.Fatal().Err(err)
	}

	batchSize := 20
	maxConcurrentBatches := 2

	go func() {
		c1.ReadBatchAsync(context.Background(), batchSize, maxConcurrentBatches)
	}()
	go func() {
		c2.ReadBatchAsync(context.Background(), batchSize, maxConcurrentBatches)
	}()

	GracefulShutdown(
		logger,
		10*time.Second,
		c1,
		c2,
	)
}
