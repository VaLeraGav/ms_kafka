package app

import (
	"os"
	"path/filepath"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const (
	envLocal = "local"
	envDev   = "dev"
	envProd  = "prod"
)

func GetLogPath(projectPath string) string {
	return filepath.Join(projectPath, "logs", "local.log")
}

func ConfigureLogger(env string, logPath string) *zerolog.Logger {
	var logger zerolog.Logger
	timeFormat := "2006-01-02 15:04:05+03:00"

	switch env {
	case envLocal:
		zerolog.SetGlobalLevel(zerolog.DebugLevel)

		logFile := openLogFile(logPath)

		consoleWriter := zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: timeFormat}
		fileWriter := zerolog.ConsoleWriter{Out: logFile, TimeFormat: timeFormat, NoColor: true}

		multiWriter := zerolog.MultiLevelWriter(fileWriter, consoleWriter)
		logger = zerolog.New(multiWriter)
	case envDev:
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
		logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	case envProd:
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
		logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, NoColor: true})
	default:
		log.Warn().Msg("Unknown environment, using the default level: Info")
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
		logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}

	logger = logger.With().Timestamp().Caller().Logger()

	return &logger
}

func openLogFile(logPath string) *os.File {
	dir := filepath.Dir(logPath)

	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Fatal().Err(err).Msg("Could not create log directory")
	}

	logFile, err := os.OpenFile(
		logPath,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0664,
	)
	if err != nil {
		log.Fatal().Err(err).Msg("Could not open log file")
	}

	return logFile
}
