#!/bin/sh

protoc --proto_path=primod/primodproto --go_out=primod/primodproto --go_opt=paths=source_relative --go-grpc_out=primod/primodproto --go-grpc_opt=paths=source_relative primod/primodproto/*.proto