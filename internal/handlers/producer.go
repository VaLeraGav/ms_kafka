package handlers

import (
	"errors"
	"io"
	"net/http"

	"github.com/VaLeraGav/ms_kafka/internal/kafka"
	"github.com/go-chi/chi"
	jsoniter "github.com/json-iterator/go"
	"github.com/rs/zerolog"
)

type ObjectList []map[string]interface{}

var (
	ErrTopicNameIsNotValid = errors.New("topic name is not valid")
	ErrBodyNotExist        = errors.New("body not exist")
	ErrReadingRequestBody  = errors.New("error reading the request body")
)

func Producer(log *zerolog.Logger, producer *kafka.Producer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		topicName := chi.URLParam(r, "topic_name")

		if len(topicName) < 3 {
			ErrorHandle(w, r, http.StatusBadRequest, ErrTopicNameIsNotValid)
			return
		}

		defer r.Body.Close()
		topicBody, err := io.ReadAll(r.Body)
		if err != nil {
			ErrorHandle(w, r, http.StatusInternalServerError, ErrReadingRequestBody)
			return
		}

		if len(topicBody) == 0 {
			ErrorHandle(w, r, http.StatusBadRequest, ErrBodyNotExist)
			return
		}

		// topicBody, err = os.ReadFile("/mnt/c/Users/gavrin.v/Desktop/Kafka/ЦеныССоглашением.json")
		// if err != nil {
		// 	log.Error().Err(err).Msg("failed to Marshal object")
		// 	return
		// }

		var objects ObjectList
		err = jsoniter.Unmarshal(topicBody, &objects)
		if err != nil {
			log.Error().Err(err).Msg("failed to Unmarshal object")
			ErrorHandle(w, r, http.StatusInternalServerError, errors.New("oшибка при обработке входных данных"))
			return
		}

		var messages [][]byte
		for _, object := range objects {
			infoBytes, err := jsoniter.Marshal(object)
			if err != nil {
				log.Error().Err(err).Interface("object", object).Msg("failed to Marshal object")
				break
			}
			messages = append(messages, infoBytes)
		}

		if err := producer.SendBatchToTopicAsync(topicName, messages); err != nil {
			ErrorHandle(w, r, http.StatusInternalServerError, err)
			return
		}

		SuccessStrHandle(w, r, http.StatusOK, topicName)
	}
}
