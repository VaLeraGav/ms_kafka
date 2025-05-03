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
Usage:
  help           print this help message
  init-p         used to initialize the Go project, tidy, docker, migration, build and deploy
  fast-start-p   quick launch of ms_kafka_producer
  start-p        build start of ms_kafka_producer
  fast-start-c   quick launch of ms_kafka_consumer
  start-c        build start of ms_kafka_consumer
```

## Использование

Примеры запросов к API и описание конечных точек:

> ![NOTE]: перед тем как записывать в `topic`, нужно его создать и прописать настройки в `kafka-ui`

- POST `/kafka/topic/{topic_name}` - Запись в topic
