package modb

import (
	"math/rand"
	"time"

	"github.com/chilts/sid"
)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

func NewItem(key, action, data string) Item {
	// ToDo: validation

	id := sid.Id()

	return Item{
		ID:     id,
		Key:    key,
		Action: action,
		Data:   data,
	}
}

type Item struct {
	ID     string
	Key    string
	Action string
	Data   string // TODO: should be interface{}
}

func (i *Item) Time() time.Time {
	// split the ID into two
	return time.Now()
}

// ClientService is the interface that all client stores must implement.
type ClientService interface {
	Set(key, json string) error
	Inc(key, field string) error
	Get(key string) (string, error)
	Keys() ([]string, error)
	Close() error
}

// NodeService is the interface that all node stores must implement.
type NodeService interface {
	Close() error
}
