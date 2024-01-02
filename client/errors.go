package client

import "errors"

var (
	// ErrConfigFileNotFound raised when invalid config file path
	ErrConfigFileNotFound = errors.New("error: Config file not found")
	// ErrConfigParseFailed when failed to parse config file
	ErrConfigParseFailed = errors.New("error: Failed to parse config file")
)
