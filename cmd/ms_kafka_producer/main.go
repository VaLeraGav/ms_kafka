package main

import (
	"flag"
	"os"

	"github.com/VaLeraGav/ms_kafka/internal/app"
)

func main() {
	projectPath := getProjectPath()

	var configPath string
	flag.StringVar(&configPath, "config", app.PathDefault(projectPath), "path to config file")
	flag.Parse()

	configs := app.MustInitConfig(configPath)

	app.RunProducer(configs, projectPath)
}

func getProjectPath() string {
	projectPath := os.Getenv("PROJECT_PATH")
	if projectPath != "" {
		return projectPath
	}

	currentDir, _ := os.Getwd()
	return currentDir
}
