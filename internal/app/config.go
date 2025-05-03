package app

import (
	"log"
	"os"
	"path/filepath"
	"strconv"

	"github.com/joho/godotenv"
)

type Config struct {
	Env        string `yaml:"env" env-default:"local"`
	HTTPServer `yaml:"http_server"`
	Kafka      `yaml:"kafka"`
}

type HTTPServer struct {
	Address string `yaml:"address" env-default:"localhost:8080"`
}

type Kafka struct {
	Address string `yaml:"address" env-default:"localhost:8080"`
}

func MustInitConfig(configPath string) *Config {
	if configPath == "" {
		log.Fatal("CONFIG_PATH is not set")
	}

	err := godotenv.Load(configPath)
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	return &Config{
		Env: MustGetEnv("ENV"),
		HTTPServer: HTTPServer{
			Address: MustGetEnv("HTTP_SERVER_ADDRESS"),
		},
		Kafka: Kafka{
			Address: MustGetEnv("KAFKA_ADDRESS"),
		},
	}
}

func PathDefault(workDir string) string {
	return filepath.Join(workDir, ".env")
}

func MustGetEnv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		log.Fatalf("no variable in env: %s", key)
	}
	return value
}

func MustGetEnvAsInt(name string) int {
	valueStr := MustGetEnv(name)
	if value, err := strconv.Atoi(valueStr); err == nil {
		return value
	}

	return -1
}
