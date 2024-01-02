package clientconfig

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v2"
)

const (
	defaultClientConfigFile = "config/client.yaml"
	doPanic                 = true
)

// ClientConfig client
type ClientConfig struct {
	Version string `yaml:"version"`
	Server  struct {
		Host    string        `yaml:"host"`
		Port    int           `yaml:"port"`
		Timeout time.Duration `yaml:"timeout"`
	} `yaml:"server"`
	Wal struct {
		Datadir string `yaml:"datadir"`
	} `yaml:"wal"`
}

func check(err error, methodSign string) {
	msg := fmt.Sprintf("Failed while running method %s, Error %v", methodSign, err)
	if !doPanic {
		log.Print(msg)
		return
	}
	if err != nil {
		log.Fatalf(msg)
	}
}

func loadClientConfig() (cfg *ClientConfig) {
	configFile, err := os.ReadFile(defaultClientConfigFile)
	fmt.Printf("Loading ClientConfig from %s\n", defaultClientConfigFile)
	check(err, "loadClientConfig")
	err = yaml.Unmarshal(configFile, &cfg)
	check(err, "loadClientConfig")
	fmt.Printf("Loaded ClientConfig: %+v\n", cfg)
	return cfg
}

// Config function loads and returns the config based on `configName` parameter
func Config(configName string) (cfg interface{}) {
	switch strings.ToLower(configName) {
	case "client":
		cfg = loadClientConfig()
	default:
		log.Fatalf("Invalid config")
	}
	return cfg
}
