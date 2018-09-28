package bolt

import (
	"fmt"

	modb "github.com/chilts/modb"
	"github.com/chilts/sid"
	"github.com/tidwall/sjson"
	bbolt "go.etcd.io/bbolt"
)

var logBucketName = []byte("log")
var itemBucketName = []byte("item")

type store struct{ db *bbolt.DB }

func Open(dirname string) (modb.ClientService, error) {
	var err error

	db, err := bbolt.Open(dirname, 0666, nil)
	if err != nil {
		return nil, err
	}

	// db.tx.Bucket(logBucketName)

	err = db.Update(func(tx *bbolt.Tx) error {
		var err error

		_, err = tx.CreateBucketIfNotExists(logBucketName)
		if err != nil {
			return err
		}

		_, err = tx.CreateBucketIfNotExists(itemBucketName)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return &store{db}, nil
}

// Returns all of the keys in the current `itemBucketName`.
func (s *store) Keys() ([]string, error) {
	keys := make([]string, 0)

	err := s.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(itemBucketName)
		cursor := bucket.Cursor()

		for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
			keys = append(keys, string(k))
		}
		return nil
	})
	return keys, err
}

// Sets the item to the json data provided.
func (s *store) Set(name, json string) error {
	key := sid.Id() + ":" + name
	val := "set:" + json

	fmt.Printf("key=%s\n", key)
	fmt.Printf("val=%s\n", val)

	return s.db.Update(func(tx *bbolt.Tx) error {
		log := tx.Bucket(logBucketName)
		return log.Put([]byte(key), []byte(val))
	})
}

// Increments a field of this item by 1.
func (s *store) Inc(name, field string) error {
	json, err := sjson.Set("{}", field, 1)
	if err != nil {
		return err
	}

	key := sid.Id() + ":" + name
	val := "inc:" + json

	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(logBucketName)
		return b.Put([]byte(key), []byte(val))
	})
}

// Always returns valid JSON for the key, even if the key doesn't exist. ie. an empty key would be returned as '{}'.
func (s *store) Get(key string) (string, error) {
	var v string
	err := s.db.View(func(tx *bbolt.Tx) error {
		v = string(tx.Bucket(itemBucketName).Get([]byte(key)))
		return nil
	})
	return v, err
}

func (s *store) Close() error {
	return s.db.Close()
}
