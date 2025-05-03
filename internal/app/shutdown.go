package app

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog"
)

type Shutdownable interface {
	Shutdown(ctx context.Context) error
}

func GracefulShutdown(logger *zerolog.Logger, timeout time.Duration, components ...Shutdownable) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	logger.Info().Msg("shutdown signal received")

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var wg sync.WaitGroup
	for _, comp := range components {
		wg.Add(1)
		go func(c Shutdownable) {
			defer wg.Done()
			if err := c.Shutdown(ctx); err != nil {
				logger.Error().Err(err).Msg("shutdown error")
			} else {
				logger.Info().Msg("shutdown success")
			}
		}(comp)
	}

	wg.Wait()
	logger.Info().Msg("graceful shutdown complete")
}
