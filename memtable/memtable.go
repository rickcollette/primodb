// Package memtable contains implementation of key-value store
package memtable

import (
	"errors"
	"sync"
	"time"
)

// Error variables defined at the package level
var (
	ErrKeyNotFound          = errors.New("error: Key not found")
	ErrInvalidCommand       = errors.New("error: Invalid command")
	ErrInvalidNoOfArguments = errors.New("error: Invalid number of arguments passed")
	ErrKeyValueMissing      = errors.New("error: Key or value not passed")
)

// KVRow individual row in db
type KVRow struct {
	Key       string
	Value     string
	createdAt int64
}

// KVStore DB memory map
type KVStore struct {
	data map[string]KVRow
	mux  sync.Mutex
}


func (s *KVStore) Create(key, value string) (string, error) {
	s.mux.Lock()
	defer s.mux.Unlock()
	s.data[key] = KVRow{key, value, time.Now().Unix()}
	return "Inserted 1", nil
}

func (s *KVStore) Read(key string) (string, error) {
	if s.data[key] != (KVRow{}) {
		return s.data[key].Value, nil
	}
	return "", ErrKeyNotFound
}

func (s *KVStore) Update(key, value string) (string, error) {
	s.mux.Lock()
	defer s.mux.Unlock()
	if found, _ := s.Read(key); len(found) == 0 {
		return "Updated 0", ErrKeyNotFound
	}
	s.data[key] = KVRow{key, value, time.Now().Unix()}
	return "Updated 1", nil
}

func (s *KVStore) Delete(key string) (string, error) {
	s.mux.Lock()
	defer s.mux.Unlock()
	if found, _ := s.Read(key); len(found) == 0 {
		return "Deleted 0", ErrKeyNotFound
	}
	delete(s.data, key)
	return "Deleted 1", nil
}


// Singleton KVStore instance
var once sync.Once
var store *KVStore

// NewDB returns a singleton KVStore instance
func NewDB() *KVStore {
	once.Do(func() {
		store = &KVStore{data: make(map[string]KVRow)}
	})
	return store
}

// DatabaseStore represents an in-memory key-value store for multiple databases.
type DatabaseStore struct {
	databases map[string]*KVStore
	mux       sync.Mutex
}

// NewDatabaseStore creates a new instance of DatabaseStore.
func NewDatabaseStore() *DatabaseStore {
	return &DatabaseStore{
		databases: make(map[string]*KVStore),
	}
}

// GetDatabase returns an in-memory key-value store by name.
func (s *DatabaseStore) GetDatabase(name string) *KVStore {
	s.mux.Lock()
	defer s.mux.Unlock()

	db, exists := s.databases[name]
	if !exists {
		db = &KVStore{data: make(map[string]KVRow)}
		s.databases[name] = db
	}

	return db
}

// DeleteDatabase deletes an in-memory database by name.
func (s *DatabaseStore) DeleteDatabase(name string) {
	s.mux.Lock()
	defer s.mux.Unlock()

	delete(s.databases, name)
}
