package server

import (
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/rickcollette/primodb/serverconfig"
	"github.com/rickcollette/primodb/memtable"
	"github.com/rickcollette/primodb/primodb/primodproto"
	"github.com/rickcollette/primodb/wal"
	"google.golang.org/protobuf/proto"
)

type Mode string

const (
	ActiveMode   Mode = "ACTIVE"
	RecoveryMode Mode = "RECOVERY"
)

type Server struct {
	dbStore      *memtable.DatabaseStore
	mu           sync.Mutex
	mode         Mode
	rWalObj      *wal.Wal
	walObj       *wal.Wal
	useS3        bool
	s3Config     serverconfig.S3Config
	s3Uploader   *s3manager.Uploader
	s3Downloader *s3manager.Downloader
	s3Session    *session.Session
}

func NewServer(walDir string, useS3 bool, s3Config serverconfig.S3Config) *Server {
	server := &Server{
		dbStore:  memtable.NewDatabaseStore(),
		useS3:    useS3,
		s3Config: s3Config,
	}

	server.mu.Lock()
	defer server.mu.Unlock()

	log.Println("Starting Server initialization")

	// WAL setup
	var err error
	if useS3 {
		// Initialize AWS session and S3 uploader/downloader
		server.s3Config = s3Config
		server.s3Session, err = session.NewSession(&aws.Config{
			Region:      aws.String(s3Config.Region),
			Credentials: credentials.NewStaticCredentials(s3Config.AccessKey, s3Config.SecretKey, ""),
		})
		if err != nil {
			log.Fatalf("Failed to create AWS session: %s", err)
		}
		server.s3Uploader = s3manager.NewUploader(server.s3Session)
		server.s3Downloader = s3manager.NewDownloader(server.s3Session)
	}

	server.walObj, err = wal.New(walDir, useS3, s3Config, server.s3Session)
	if err != nil {
		log.Fatalf("Failed to initialize WAL: %s", err)
	}

	// Database recovery
	server.setMode(RecoveryMode)
	if err := server.recoverFromWAL(walDir); err != nil {
		log.Fatalf("Recovery failed: %s", err)
	}
	server.setMode(ActiveMode)

	log.Println("Server initialization finished")
	return server
}
func (s *Server) setMode(mode Mode) {
	s.mode = mode
}

func (s *Server) recoverFromWAL(walDir string) error {
	var err error
	// Open the existing WAL for recovery
	s.rWalObj, err = wal.Open(walDir)
	if err != nil {
		if err == wal.ErrWalNotFound {
			return nil // No WAL found, nothing to recover
		}
		return err
	}
	defer s.rWalObj.Close()

	if s.useS3 {
		s.walObj, err = wal.New(walDir, true, s.s3Config, s.s3Session)
	} else {
		s.walObj, err = wal.New(walDir, false, serverconfig.S3Config{}, nil)
	}
	if err != nil {
		return err
	}

	for record := range s.rWalObj.Read() {
		recordData := &primodproto.Record{}
		if err := proto.Unmarshal(record.Data, recordData); err != nil {
			return err
		}

		dbName, key := parseDbNameAndKey(recordData.Key)
		db := s.dbStore.GetDatabase(dbName)
		var err error
		switch recordData.Cmd {
		case "CREATE":
			_, err = db.Create(key, recordData.GetValue())
		case "DELETE":
			_, err = db.Delete(key)
		case "UPDATE":
			_, err = db.Update(key, recordData.GetValue())
		default:
			return fmt.Errorf("invalid command during recovery: %s", recordData.Cmd)
		}

		if err != nil {
			return err
		}
	}
	return nil
}
func (s *Server) logRecord(cmd, key, value string) error {
    record, err := proto.Marshal(&primodproto.Record{Cmd: cmd, Key: key, Value: value})
    if err != nil {
        return err
    }
    return s.walObj.Write(record)
}

func parseDbNameAndKey(combinedKey string) (string, string) {
    parts := strings.SplitN(combinedKey, ":", 2)
    if len(parts) != 2 {
        return "", combinedKey
    }
    return parts[0], parts[1]
}
func (s *Server) Create(databaseName, key, value string) (string, error) {
	db := s.dbStore.GetDatabase(databaseName) // Access the specific database

	// Log the operation
	if err := s.logRecord("CREATE", key, value); err != nil {
		return "", err
	}

	// Call Create method from memtable package
	return db.Create(key, value)
} // Get retrieves a value for a key from a specific database.
func (s *Server) Read(databaseName, key string) (string, error) {
	db := s.dbStore.GetDatabase(databaseName) // Access the specific database
	return db.Read(key)
}

func (s *Server) Update(databaseName, key, value string) (string, error) {
	db := s.dbStore.GetDatabase(databaseName) // Access the specific database

	// Log the operation
	if err := s.logRecord("UPDATE", key, ""); err != nil {
		return "", err
	}

	return db.Update(key, value)
}

// Del deletes a key-value pair from a specific database.
func (s *Server) Delete(databaseName, key string) (string, error) {
	db := s.dbStore.GetDatabase(databaseName) // Access the specific database

	// Log the operation
	if err := s.logRecord("DELETE", key, ""); err != nil {
		return "", err
	}

	return db.Delete(key)
}

