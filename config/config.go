package config

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v2"
)
// S3Config holds the configuration for S3 storage
type S3Config struct {
    Bucket    string `yaml:"bucket"`
    Region    string `yaml:"region"`
    AccessKey string `yaml:"accessKey"`
    SecretKey string `yaml:"secretKey"`
}

// ServerConfig server
type ServerConfig struct {
    Server struct {
        Host    string        `yaml:"host"`
        Port    int           `yaml:"port"`
        DB      string        `yaml:"dbname"`
        Timeout time.Duration `yaml:"timeout"`
    } `yaml:"server"`
    Wal struct {
        Datadir string   `yaml:"datadir"`
        UseS3   bool     `yaml:"useS3"`
        S3Config S3Config `yaml:"s3Config"`
    } `yaml:"wal"`
}
const (
	defaultClientConfigFile = "config/client.yaml"
	defaultServerConfigFile = "config/server.yaml"
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

func loadServerConfig() (cfg *ServerConfig) {
	configFile, err := os.ReadFile(defaultServerConfigFile)
	check(err, "loadServerConfig")
	err = yaml.Unmarshal(configFile, &cfg)
	check(err, "loadServerConfig")
	return cfg
}

func loadClientConfig() (cfg *ClientConfig) {
	configFile, err := os.ReadFile(defaultClientConfigFile)
	check(err, "loadClientConfig")
	err = yaml.Unmarshal(configFile, &cfg)
	check(err, "loadClientConfig")
	return cfg
}

// Config function loads and returns the config based on `configName` parameter
func Config(configName string) (cfg interface{}) {
	switch strings.ToLower(configName) {
	case "client":
		cfg = loadClientConfig()
	case "server":
		cfg = loadServerConfig()
	default:
		log.Fatalf("Invalid config")
	}
	return cfg
}
