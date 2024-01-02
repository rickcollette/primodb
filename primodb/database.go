package server

import (
	"fmt"
	"log"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/rickcollette/primodb/config"
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

// database wraps the `memtable` methods and manages the WAL.
type database struct {
	db      *memtable.KVStore
	name    string
	mode    Mode
	mu      sync.Mutex
	rWalObj *wal.Wal
	walObj  *wal.Wal
	useS3    bool
	s3Config config.S3Config
	s3Uploader   *s3manager.Uploader
    s3Downloader *s3manager.Downloader
    s3Session    *session.Session
}

func (d *database) logRecord(cmd, key, value string) error {
	record, err := proto.Marshal(&primodproto.Record{Cmd: cmd, Key: key, Value: value})
	if err != nil {
		return err
	}
	return d.walObj.Write(record)
}

func (d *database) Get(key string) (string, error) {
	return d.db.Get(key)
}

func (d *database) Set(key, value string) (string, error) {
	if err := d.logRecord("SET", key, value); err != nil {
		return "", err // Don't use log.Fatal in library code
	}
	return d.db.Create(key, value)
}

func (d *database) Del(key string) (string, error) {
	if err := d.logRecord("DEL", key, ""); err != nil {
		return "", err // Don't use log.Fatal in library code
	}
	return d.db.Delete(key)
}

func (d *database) setMode(mode Mode) {
	d.mode = mode
}

func NewDb(name, walDir string, useS3 bool, s3Config config.S3Config) *database {
	db := &database{
		db:       memtable.NewDB(),
		name:     name,
		useS3:    useS3,
		s3Config: s3Config,
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	log.Println("Starting DB initialization")

	// WAL setup
	var err error
	if useS3 {
		// Initialize AWS session and S3 uploader/downloader
		db.s3Session, err = session.NewSession(&aws.Config{
			Region:      aws.String(s3Config.Region),
			Credentials: credentials.NewStaticCredentials(s3Config.AccessKey, s3Config.SecretKey, ""),
		})
		if err != nil {
			log.Fatalf("Failed to create AWS session: %s", err)
		}
		db.s3Uploader = s3manager.NewUploader(db.s3Session)
		db.s3Downloader = s3manager.NewDownloader(db.s3Session)
	}
	db.walObj, err = wal.New(walDir, useS3, s3Config, db.s3Session)
	if err != nil {
		log.Fatalf("Failed to initialize WAL: %s", err)
	}

	// Database recovery
	db.setMode(RecoveryMode)
	if err := db.recoverFromWAL(walDir); err != nil {
		log.Fatalf("Recovery failed: %s", err)
	}
	db.setMode(ActiveMode)

	log.Println("DB initialization finished")
	return db
}

func (d *database) recoverFromWAL(walDir string) error {
	var err error
    // Open the existing WAL for recovery
    d.rWalObj, err = wal.Open(walDir)
    if err != nil {
        if err == wal.ErrWalNotFound {
            return nil // No WAL found, nothing to recover
        }
        return err
    }
    defer d.rWalObj.Close()

    if d.useS3 {
        d.walObj, err = wal.New(walDir, true, d.s3Config, d.s3Session)
    } else {
        d.walObj, err = wal.New(walDir, false, config.S3Config{}, nil)
    }
    if err != nil {
        return err
    }

    // Process the records from the recovery WAL object
    for record := range d.rWalObj.Read() {
        recordData := &primodproto.Record{}
        if err := proto.Unmarshal(record.Data, recordData); err != nil {
            return err
        }
        switch recordData.Cmd {
        case "SET":
            _, err = d.Set(recordData.GetKey(), recordData.GetValue())
        case "DEL":
            _, err = d.Del(recordData.GetKey())
        default:
            return fmt.Errorf("invalid command during recovery: %s", recordData.Cmd)
        }
        if err != nil {
            return err
        }
    }
    return nil
}
