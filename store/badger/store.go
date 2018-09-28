package bolt

import (
	"log"

	modb "github.com/chilts/modb"
	badger "github.com/dgraph-io/badger"
)

type store struct{ db *badger.DB }

func Open(dirname string) (modb.ClientService, error) {
	opts := badger.DefaultOptions
	opts.Dir = dirname
	opts.ValueDir = dirname
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	// Your code here…
	return &store{db}, nil
	return nil, nil
}

func (s *store) Keys() ([]string, error) {
	keys := make([]string, 0)
	return keys, nil
}

func (s *store) Set(name, json string) error {
	return nil
}

func (s *store) Inc(name, field string) error {
	return nil
}

func (s *store) Get(key string) (string, error) {
	return "", nil
}

func (s *store) Close() error {
	return s.db.Close()
}
