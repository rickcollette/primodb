package server

import (
	"context"
	"errors"

	pb "github.com/rickcollette/primodb/primodb/primodproto"
	"golang.org/x/crypto/bcrypt"
)


func (s *server) StoreUserCredentials(ctx context.Context, username, password string) error {
    hashedPassword, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
    if err != nil {
        return err // or handle error appropriately
    }

    // Access the 'users' database and store the hashed password
    db := s.db.dbStore.GetDatabase("users") // Access 'users' database
    _, err = db.Create("user:"+username, string(hashedPassword))
    return err
}

func (s *server) Authenticate(ctx context.Context, req *pb.AuthRequest) (*pb.AuthResponse, error) {
    // Retrieve hashed password from the 'users' database
    hashedPassword, err := s.db.dbStore.GetDatabase("users").Read("user:" + req.Username)
    if err != nil {
        // Handle error (e.g., user not found)
        return &pb.AuthResponse{Authenticated: false}, err
    }

    // Compare the provided password with the stored hashed password
    err = bcrypt.CompareHashAndPassword([]byte(hashedPassword), []byte(req.Password))
    if err != nil {
        // Passwords do not match
        return &pb.AuthResponse{Authenticated: false}, errors.New("authentication failed")
    }

    // Authentication successful
    return &pb.AuthResponse{
        Authenticated: true,
        Token:         "some-auth-token", // Generate a secure token
    }, nil
}
