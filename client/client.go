package client

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/rickcollette/primodb/clientconfig"

	pb "github.com/rickcollette/primodb/primodb/primodproto"
	"google.golang.org/grpc"
)

const doPanic = true

func check(err error, methodSign string) {
	if !doPanic {
		return
	}
	if err != nil {
		log.Fatalf("CLIENT: method %s, Error %v", methodSign, err)
	}
}

type PrimoDBClient struct {
	config   *clientconfig.ClientConfig
	client   pb.PrimoDBClient
	conn     *grpc.ClientConn
	ClientID string
	Timeout  time.Duration // Add timeout field to specify request timeout
}

// ServerAddress returns the address of mdb server.
func (c *PrimoDBClient) ServerAddress() string {
	serverConfig := c.config.Server
	return fmt.Sprintf("%s:%d", serverConfig.Host, serverConfig.Port)
}

func (c *PrimoDBClient) setupClient() {
	// Set up a connection to the server.
	conn, err := grpc.Dial(c.ServerAddress(), grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	c.conn = conn
	c.client = pb.NewPrimoDBClient(conn) // Fix: Create a gRPC client using the connection and the generated client code.
	// TODO look for alternative, handle error
	// Generate UUID
	id := uuid.New()
	c.ClientID = id.String()
}

// Get the value from server for a given key
func (c *PrimoDBClient) Read(key string) (string, error) {
	ctx, cancel := context.WithTimeout(
		context.Background(), c.config.Server.Timeout*time.Second)
	defer cancel()
	r, err := c.client.Read(ctx, &pb.ReadRequest{Key: key, ClientId: c.ClientID})
	check(err, "Read")
	return r.Value, err
}

// Set grpc client
func (c *PrimoDBClient) Create(key, value string) (string, error) {
	ctx, cancel := context.WithTimeout(
		context.Background(), c.config.Server.Timeout*time.Second)
	defer cancel()
	r, err := c.client.Create(ctx, &pb.CreateRequest{Key: key, Value: value, ClientId: c.ClientID})
	check(err, "Create")
	return r.Message, err
}

func (c *PrimoDBClient) Update(key, value string) (string, error) {
	ctx, cancel := context.WithTimeout(
		context.Background(), c.config.Server.Timeout*time.Second)
	defer cancel()
	r, err := c.client.Update(ctx, &pb.UpdateRequest{Key: key, Value: value, ClientId: c.ClientID})
	check(err, "Update")
	return r.Message, err
}
// Del grpc client
func (c *PrimoDBClient) Delete(key string) (string, error) {
	ctx, cancel := context.WithTimeout(
		context.Background(), c.config.Server.Timeout*time.Second)
	defer cancel()
	r, err := c.client.Delete(ctx, &pb.DeleteRequest{Key: key, ClientId: c.ClientID})
	check(err, "Delete")
	return r.Message, err
}

// GetID returns the client id
func (c *PrimoDBClient) GetID() string {
	if c.config != nil {
		return c.ClientID
	}
	return "Unknown Client ID" // Default value if config is nil
}

func (c *PrimoDBClient) Version() (string, error) {
	if c == nil || c.config == nil {
		return "Unknown Version", errors.New("client or config is nil")
	}
	return c.config.Version, nil
}
func NewClient(host string, port int, dbname string, timeout time.Duration, clientConfig *clientconfig.ClientConfig, username, password string) (*PrimoDBClient, error) {
    fmt.Printf("Debug - ClientConfig in NewClient: %+v\n", clientConfig)

    address := fmt.Sprintf("%s:%d", host, port)
    conn, err := grpc.Dial(address, grpc.WithInsecure())
    if err != nil {
        return nil, fmt.Errorf("did not connect: %v", err)
    }

    // Initialize the client
    client := PrimoDBClient{
        client:   pb.NewPrimoDBClient(conn),
        conn:     conn,
        ClientID: uuid.New().String(),
        Timeout:  timeout,
        config:   clientConfig,
    }

    // Authenticate the user
    ctx, cancel := context.WithTimeout(context.Background(), client.Timeout)
    defer cancel()

    authResp, err := client.client.Authenticate(ctx, &pb.AuthRequest{
        Username: username,
        Password: password,
    })
    if err != nil || !authResp.GetAuthenticated() {
        return nil, fmt.Errorf("authentication failed: %v", err)
    }

    // Store the token in client for future requests
    client.Token = authResp.GetToken()

    version, err := client.Version()
    if err != nil {
        fmt.Println("Error getting version:", err)
    } else {
        fmt.Printf("Client version: %s\n", version)
    }

    return &client, nil
}
