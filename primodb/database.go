package server

import (
	"fmt"
	"log"
	"sync"

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

func NewDb(name, walDir string) *database {
	db := &database{
		db:   memtable.NewDB(),
		name: name,
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	log.Println("Starting DB recovery")

	db.setMode(RecoveryMode)
	defer db.setMode(ActiveMode)

	if err := db.recoverFromWAL(walDir); err != nil {
		log.Fatalf("Recovery: %s", err)
	}

	log.Println("DB recovery finished")
	return db
}

func (d *database) recoverFromWAL(walDir string) error {
	var err error
	d.rWalObj, err = wal.Open(walDir)
	if err != nil {
		if err == wal.ErrWalNotFound {
			return nil // No WAL found, nothing to recover
		}
		return err
	}
	defer d.rWalObj.Close()

	d.walObj, err = wal.New(walDir)
	if err != nil {
		return err
	}

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
