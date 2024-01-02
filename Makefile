.PHONY: all build_client build_server proto clean copy_config

# Binary names
CLIENT_BIN_PATH := dist/pdbc
SERVER_BIN_PATH := dist/primod  # Corrected variable name

# Directories
CLIENT_MAIN_DIR := primocli/main.go
SERVER_MAIN_DIR := cmd/primod/main.go

# Default target
all: build_client build_server copy_config

# Compile protocol buffers
proto:
	bash ./compile_proto.sh

copy_config:
	mkdir -p dist/config
	mkdir -p dist/data
	cp clientconfig/client.yaml dist/config
	cp serverconfig/server.yaml dist/config

# Build client binary
build_client: proto
	go build -o $(CLIENT_BIN_PATH) $(CLIENT_MAIN_DIR)

# Build server binary
build_server: proto
	go build -o $(SERVER_BIN_PATH) $(SERVER_MAIN_DIR)

# Clean up
clean:
	rm -f $(CLIENT_BIN_PATH) $(SERVER_BIN_PATH)
	rm -rf dist
	rm primodb/primodproto/*.pb.go