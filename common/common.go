package common

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"time"
)

const DateTimeFormat = "2006-01-02 15:04:05"

type Config struct {
	Brokers []string
	Topic   string
}

type LogWriter struct {
}

// Read app config from Yaml file
func GetConfig() (*Config, error) {
	cfg := &Config{}
	file, err := ioutil.ReadFile("config.yml")
	if err != nil {
		LogError("Failed to read config", err)
	} else {
		//err = yaml.Unmarshal(file, cfg)
		err = yaml.UnmarshalStrict(file, cfg)
		if err != nil {
			LogError("Failed to parse config", err)
		}
	}

	return cfg, err
}

// Custom log writer
func (writer LogWriter) Write(bytes []byte) (int, error) {
	return fmt.Printf("[%s] %s", time.Now().Format(DateTimeFormat), string(bytes))
}

// Output message with error description
func LogError(message string, err error) {
	log.Printf("\n%s:\n%s\n", message, err)
}
