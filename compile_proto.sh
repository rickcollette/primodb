#!/bin/sh

protoc --proto_path=primodb/primodproto --go_out=primodb/primodproto --go_opt=paths=source_relative --go-grpc_out=primodb/primodproto --go-grpc_opt=paths=source_relative primodb/primodproto/*.proto