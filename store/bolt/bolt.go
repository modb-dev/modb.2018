package bolt

import (
	"fmt"

	modb "github.com/chilts/modb"
	"github.com/chilts/sid"
	"github.com/tidwall/sjson"
	bolt "go.etcd.io/bbolt"
)

var logBucketName = []byte("log")
var itemBucketName = []byte("item")

func Open(dirname string) (modb.ClientService, error) {
	var err error

	db, err := bolt.Open(dirname, 0666, nil)
	if err != nil {
		return nil, err
	}

	// db.tx.Bucket(logBucketName)

	err = db.Update(func(tx *bolt.Tx) error {
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

	return &blt{db}, nil
}

type blt struct{ db *bolt.DB }

// Returns all of the keys in the current `itemBucketName`.
func (b *blt) Keys() ([]string, error) {
	keys := make([]string, 0)

	err := b.db.View(func(tx *bolt.Tx) error {
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
func (b *blt) Set(name, json string) error {
	key := sid.Id() + ":" + name
	val := "set:" + json

	fmt.Printf("key=%s\n", key)
	fmt.Printf("val=%s\n", val)

	return b.db.Update(func(tx *bolt.Tx) error {
		log := tx.Bucket(logBucketName)
		return log.Put([]byte(key), []byte(val))
	})
}

// Increments a field of this item by 1.
func (b *blt) Inc(name, field string) error {
	json, err := sjson.Set("{}", field, 1)
	if err != nil {
		return err
	}

	key := sid.Id() + ":" + name
	val := "inc:" + json

	return b.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(logBucketName)
		return b.Put([]byte(key), []byte(val))
	})
}

// Always returns valid JSON for the key, even if the key doesn't exist. ie. an empty key would be returned as '{}'.
func (b *blt) Get(key string) (string, error) {
	var v string
	err := b.db.View(func(tx *bolt.Tx) error {
		v = string(tx.Bucket(itemBucketName).Get([]byte(key)))
		return nil
	})
	return v, err
}

func (b *blt) Close() error {
	return b.db.Close()
}
