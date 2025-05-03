package app

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi"

	"github.com/VaLeraGav/ms_kafka/internal/handlers"
	"github.com/VaLeraGav/ms_kafka/internal/kafka"
	"github.com/VaLeraGav/ms_kafka/internal/middleware"
	"github.com/rs/zerolog"
)

type Server struct {
	config        *Config
	router        *chi.Mux
	log           *zerolog.Logger
	kafkaProducer *kafka.Producer
}

func NewServer(config *Config, logger *zerolog.Logger, kafkaProducer *kafka.Producer) *Server {
	s := &Server{
		router:        chi.NewRouter(),
		config:        config,
		log:           logger,
		kafkaProducer: kafkaProducer,
	}
	s.configureRouting()

	return s
}

func (s *Server) configureRouting() {
	s.router.Use(middleware.RequestID)
	s.router.Use(middleware.NewLogger(s.log))

	s.router.Post("/kafka/topic/{topic_name}", handlers.Producer(s.log, s.kafkaProducer))
}

func (s *Server) StartServer() *http.Server {
	s.log.Log().Str("address", s.config.HTTPServer.Address).Msg("starting server")

	server := &http.Server{
		Addr:    s.config.HTTPServer.Address,
		Handler: s.router,
	}

	go func() {
		if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			s.log.Fatal().Err(err).Msg("HTTP server error")
		}
		s.log.Info().Msg("stopped serving new connections")
	}()

	return server

}
