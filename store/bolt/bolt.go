package bolt

import (
	modb "gitlab.com/chilts/modb"
	bolt "go.etcd.io/bbolt"
)

var logBucketName = []byte("item")

func Open(dirname string) (modb.ClientService, error) {
	db, err := bolt.Open(dirname, 0666, nil)
	if err != nil {
		return nil, err
	}

	return &blt{db}, nil
}

type blt struct{ db *bolt.DB }

func (b *blt) Set(i modb.Item) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(logBucketName)
		return b.Put([]byte(i.ID), []byte(i.Data))
	})
}

func (b *blt) Close() error {
	return b.db.Close()
}
