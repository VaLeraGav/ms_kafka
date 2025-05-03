# Микросервис Kafka

## Описание

Микросервис служит промежуточным звеном между пользователем и платформой Kafka.

## Стек разработки

- `zookeeper`
- `kafka`
- `kafka-ui`

## Установка

```bash
git clone https://github.com/VaLeraGav/ms_kafka.git
```

`http://localhost:9020/` - Запуск ui интерфеис kafka.

## Команды

```
make help
Usage:
  help           print this help message
  init-p         used to initialize the Go project, tidy, docker, migration, build and deploy
  fast-start-p   quick launch of ms_kafka_producer
  start-p        build start of ms_kafka_producer
  fast-start-c   quick launch of ms_kafka_consumer
  start-c        build start of ms_kafka_consumer
```

## Использование Producer

Примеры запросов к API, запись данных в Kafka:

> [!IMPORTANT]
> Перед тем как записывать в `topic`, нужно его создать и прописать настройки в `kafka-ui`

- POST `/kafka/topic/{topic_name}` - Запись в topic

request:

```json
[
    {
        "key": "value"
    }
]
```

response:

```json
{
    "status": "success",
    "message": "{topic_name}"
}
```

## Использование Consumer

> [!NOTE]
> В разработке

- проблема в `Local: No offset stored`
- `ReadBatchAsync` возникли проблемы в фиксации offset

```go
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
```