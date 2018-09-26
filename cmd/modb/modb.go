package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/redcon"
	bolt "go.etcd.io/bbolt"
)

func main() {
	// store
	var store string
	flag.StringVar(&store, "store", "store", "specify path to use for datastore")

	// clientHost
	var clientHost string
	flag.StringVar(&clientHost, "client-host", "", "host to listen on for clients")

	// clientPort
	var clientPort string
	flag.StringVar(&clientPort, "client-port", "6380", "port to listen on for clients")

	flag.Parse()

	// clientAddr
	clientAddr := clientHost + ":" + clientPort

	// flags
	flag.Usage = func() {
		fmt.Printf("Usage of %s:\n", os.Args[0])
		fmt.Printf("\n\n")
		flag.PrintDefaults()
	}

	log.Printf("MoDB node starting:\n")
	log.Printf("\n")
	log.Printf("store          : %s\n", store)
	log.Printf("client-host    : %s\n", clientHost)
	log.Printf("client-port    : %s\n", clientPort)
	log.Printf("client-address : %s\n", clientAddr)
	log.Printf("\n")

	var err error

	// opening datastore
	db, err := bolt.Open(store, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		log.Fatal("Error opening BoltDB: ", err)
	}
	defer db.Close()

	var mu sync.RWMutex
	var items = make(map[string][]byte)

	log.Printf("Starting server at %s", clientAddr)

	// the main (client) server
	err = redcon.ListenAndServe(clientAddr,
		func(conn redcon.Conn, cmd redcon.Command) {
			log.Printf("cmd: %s\n", cmd.Args)

			switch strings.ToLower(string(cmd.Args[0])) {
			default:
				conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
			case "ping":
				conn.WriteString("PONG")
			case "quit":
				conn.WriteString("OK")
				conn.Close()
			case "set":
				if len(cmd.Args) != 3 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				mu.Lock()
				items[string(cmd.Args[1])] = cmd.Args[2]
				mu.Unlock()
				conn.WriteString("OK")
			case "get":
				if len(cmd.Args) != 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				mu.RLock()
				val, ok := items[string(cmd.Args[1])]
				mu.RUnlock()
				if !ok {
					conn.WriteNull()
				} else {
					conn.WriteBulk(val)
				}
			case "del":
				if len(cmd.Args) != 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				mu.Lock()
				_, ok := items[string(cmd.Args[1])]
				delete(items, string(cmd.Args[1]))
				mu.Unlock()
				if !ok {
					conn.WriteInt(0)
				} else {
					conn.WriteInt(1)
				}
			}
		},
		func(conn redcon.Conn) bool {
			// use this function to accept or deny the connection.
			log.Printf("accept: %s", conn.RemoteAddr())
			return true
		},
		func(conn redcon.Conn, err error) {
			// this is called when the connection has been closed
			log.Printf("closed: %s, err: %v", conn.RemoteAddr(), err)
		},
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print("Finished")
}
