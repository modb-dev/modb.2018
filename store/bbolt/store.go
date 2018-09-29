package bbolt

import (
	"fmt"
	"strings"

	"github.com/chilts/sid"
	modb "github.com/modb-io/modb"
	"github.com/tidwall/sjson"
	bbolt "go.etcd.io/bbolt"
)

type store struct{ db *bbolt.DB }

func Open(dirname string) (modb.ServerService, error) {
	var err error

	db, err := bbolt.Open(dirname, 0666, nil)
	if err != nil {
		return nil, err
	}

	return &store{db}, nil
}

// Returns all of the keys in the current `tablename`.
func (s *store) Keys(tablename string) ([]string, error) {
	keys := make([]string, 0)

	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(tablename))
		if b == nil {
			return nil
		}

		c := b.Cursor()

		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			keys = append(keys, string(k))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return keys, nil
}

// Sets the item to the json data provided.
func (s *store) Set(path, json string) error {
	parts := strings.SplitN(path, "/", 2)
	tablename := parts[0]
	key := parts[1] + ":" + sid.Id()
	val := "set:" + json

	return s.db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(tablename))
		if err != nil {
			return err
		}
		return b.Put([]byte(key), []byte(val))
	})
}

// Increments a field of this item by 1.
func (s *store) Inc(path, field string) error {
	json, err := sjson.Set("{}", field, 1)
	if err != nil {
		return err
	}

	// path is 'tablename/itemname'
	parts := strings.SplitN(path, "/", 2)
	tablename := parts[0]
	key := parts[1] + ":" + sid.Id()
	val := "inc:" + json

	return s.db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(tablename))
		if err != nil {
			return err
		}
		return b.Put([]byte(key), []byte(val))
	})
}

// Always returns valid JSON for the key, even if the key doesn't exist. ie. an empty key would be returned as '{}'.
func (s *store) Get(path string) (string, error) {
	var v string
	fmt.Printf("Get() : v=[%s]\n", v)

	// path is 'tablename/itemname'
	parts := strings.SplitN(path, "/", 2)
	tablename := parts[0]
	key := parts[1]

	fmt.Printf("Get() : tablename=[%s], key=%s\n", tablename, key)

	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(tablename))
		if b == nil {
			return nil
		}
		v = string(b.Get([]byte(key)))
		return nil
	})

	return v, err
}

func (s *store) Close() error {
	return s.db.Close()
}
