package modb

import (
	"fmt"
	"math/rand"
	"time"
)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

func NewItem(key, action, data string) Item {
	// ToDo: validation

	// Create an ID such as "14ee8f778fc450bc-4d65822107fcfd52"
	t := time.Now()
	r := rand.Uint64()
	id := fmt.Sprintf("%x", t.UnixNano()) + "-" + fmt.Sprintf("%x", r)

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
	Set(Item) error
	Close() error
}

// NodeService is the interface that all node stores must implement.
type NodeService interface {
	Close() error
}
