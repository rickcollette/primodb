package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/rickcollette/primodb/serverconfig"
	pb "github.com/rickcollette/primodb/primodb/primodproto"
	"google.golang.org/grpc"
)

const serverStartMsg = "PrimoDB server started."

type server struct {
    db     *Server // Use *Server instead of *database
    config *serverconfig.ServerConfig
    pb.UnimplementedPrimoDBServer
}

func (s *server) Create(ctx context.Context, req *pb.CreateRequest) (*pb.CreateResponse, error) {
    log.Printf("[Client: %s] SET: %s in database: %s", req.ClientId, req.Key, req.Database)
    value, err := s.db.Create(req.Database, req.Key, req.Value) // Updated to include database
    respMsg := ""
    if err != nil {
        respMsg = err.Error()
    }
    return &pb.CreateResponse{Message: value, RespMsg: respMsg, StatusCode: 201}, nil
}

func (s *server) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
    log.Printf("[Client: %s] GET: %s in database: %s", req.ClientId, req.Key, req.Database)
    value, err := s.db.Read(req.Database, req.Key) // Updated to include database
    respMsg := ""
    if err != nil {
        respMsg = err.Error()
    }
    return &pb.ReadResponse{Value: value, RespMsg: respMsg, StatusCode: 200}, nil
}

func (s *server) Update(ctx context.Context, req *pb.UpdateRequest) (*pb.UpdateResponse, error) {
    log.Printf("[Client: %s] UPDATE: %s in database: %s", req.ClientId, req.Key, req.Database)
    value, err := s.db.Update(req.Database, req.Key, req.Value) // Updated to include database
    respMsg := ""
    if err != nil {
        respMsg = err.Error()
    }
    return &pb.UpdateResponse{Message: value, RespMsg: respMsg, StatusCode: 200}, nil
}

func (s *server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
    log.Printf("[Client: %s] DEL: %s in database: %s", req.ClientId, req.Key, req.Database)
    value, err := s.db.Delete(req.Database, req.Key) // Updated to include database
    respMsg := ""
    if err != nil {
        respMsg = err.Error()
    }
    return &pb.DeleteResponse{Message: value, RespMsg: respMsg, StatusCode: 204}, nil
}

func cleanup(db *Server) { // Change parameter type to *Server
    if db != nil && db.walObj != nil {
        db.walObj.Close()
    }
}

func Run() {
    cfg := serverconfig.Config("server").(*serverconfig.ServerConfig)

    var db *Server // Change type to *Server
    if cfg.Wal.UseS3 {
        db = NewServer(cfg.Wal.Datadir, true, cfg.Wal.S3Config) // Use NewServer instead of NewDb
    } else {
        db = NewServer(cfg.Wal.Datadir, false, serverconfig.S3Config{}) // Use NewServer instead of NewDb
    }
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
