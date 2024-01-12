package server

import (
	"context"
	"errors"
	"time"

	"github.com/rickcollette/primodb/memtable"
	pb "github.com/rickcollette/primodb/primodb/primodproto"

	jwt "github.com/dgrijalva/jwt-go"
	"golang.org/x/crypto/bcrypt"
)
var SecretKey = []byte("your_secret_key")

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
        if err == memtable.ErrKeyNotFound {
            // Return a generic error message rather than a specific 'user not found' to avoid user enumeration attacks
            return &pb.AuthResponse{Authenticated: false}, errors.New("authentication failed")
        }
        return &pb.AuthResponse{Authenticated: false}, err
    }

    // Compare the provided password with the stored hashed password
    err = bcrypt.CompareHashAndPassword([]byte(hashedPassword), []byte(req.Password))
    if err != nil {
        // Passwords do not match
        return &pb.AuthResponse{Authenticated: false}, errors.New("authentication failed")
    }

    // Authentication successful, generate a secure token
    token, err := generateSecureToken()
    if err != nil {
        // Handle token generation error
        return &pb.AuthResponse{Authenticated: false}, errors.New("failed to generate token")
    }

    // Return successful authentication response with token
    return &pb.AuthResponse{
        Authenticated: true,
        Token:         token,
    }, nil
}

func generateSecureToken() (string, error) {
    // Create a new token object, specifying signing method and the claims
    token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
        "exp": time.Now().Add(time.Hour * 24).Unix(), // Token expiration set to 24 hours
        // You can add more claims if needed
    })

    // Sign and get the complete encoded token as a string
    tokenString, err := token.SignedString(SecretKey)
    if err != nil {
        return "", err
    }

    return tokenString, nil
}
