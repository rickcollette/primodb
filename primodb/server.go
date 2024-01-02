package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/rickcollette/primodb/config"
	pb "github.com/rickcollette/primodb/primodb/primodproto"
	"google.golang.org/grpc"
)

const serverStartMsg = "MooDB server"

type server struct {
	db     *database
	config *config.ServerConfig
	pb.UnimplementedPrimoDBServer
}

func (s *server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	log.Printf("[Client: %s] GET: %s", req.ClientId, req.Key)
	value, err := s.db.Get(req.Key)
	respMsg := ""
	if err != nil {
		respMsg = err.Error()
	}
	return &pb.GetResponse{Value: value, RespMsg: respMsg, StatusCode: 200}, nil
}

func (s *server) Set(ctx context.Context, req *pb.SetRequest) (*pb.SetResponse, error) {
	log.Printf("[Client: %s] SET: %s", req.ClientId, req.Key)
	value, err := s.db.Set(req.Key, req.Value)
	respMsg := ""
	if err != nil {
		respMsg = err.Error()
	}
	return &pb.SetResponse{Message: value, RespMsg: respMsg, StatusCode: 201}, nil
}

func (s *server) Del(ctx context.Context, req *pb.DelRequest) (*pb.DelResponse, error) {
	log.Printf("[Client: %s] DEL: %s", req.ClientId, req.Key)
	value, err := s.db.Del(req.Key)
	respMsg := ""
	if err != nil {
		respMsg = err.Error()
	}
	return &pb.DelResponse{Message: value, RespMsg: respMsg, StatusCode: 204}, nil
}

func cleanup(db *database) {
	if db != nil && db.walObj != nil {
		db.walObj.Close()
	}
}

func Run() {
	cfg := config.Config("server").(*config.ServerConfig)
	db := NewDb(cfg.Server.DB, cfg.Wal.Datadir)
	defer cleanup(db)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		fmt.Println("\nShutting down server...")
		os.Exit(0)
	}()

	addr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterPrimoDBServer(s, &server{db: db, config: cfg})
	fmt.Printf("*************\n%s\n*************\n", serverStartMsg)
	log.Fatal(s.Serve(lis))
}
