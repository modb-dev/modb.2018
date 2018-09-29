package badger

import (
	"log"

	"github.com/chilts/sid"
	badger "github.com/dgraph-io/badger"
	modb "github.com/modb-io/modb"
	"github.com/tidwall/sjson"
)

type store struct{ db *badger.DB }

func Open(dirname string) (modb.ServerService, error) {
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

func (s *store) Keys(tablename string) ([]string, error) {
	keys := make([]string, 0)

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			keys = append(keys, string(it.Item().Key()))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return keys, nil
}

func (s *store) Set(pathSpec, json string) error {
	key := pathSpec + ":" + sid.Id()
	val := "set:" + json

	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), []byte(val))
	})
}

func (s *store) Inc(pathSpec, field string) error {
	json, err := sjson.Set("{}", field, 1)
	if err != nil {
		return err
	}

	key := pathSpec + ":" + sid.Id()
	val := "inc:" + json

	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), []byte(val))
	})
}

func (s *store) Get(pathSpec string) (string, error) {
	var v string

	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(pathSpec))
		if err != nil {
			return err
		}

		val, err := item.Value()
		if err != nil {
			return err
		}

		v = string(val)
		return nil
	})
	if err != nil {
		return "", err
	}

	return v, nil
}

func (s *store) Close() error {
	return s.db.Close()
}
