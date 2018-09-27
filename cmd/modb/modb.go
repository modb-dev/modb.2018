package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/chilts/sid"
	"github.com/tidwall/redcon"

	// https://github.com/golang/go/issues/26645#issuecomment-408572701
	store "gitlab.com/chilts/modb/store/bolt"
)

func main() {
	// path/store
	var storePath string
	flag.StringVar(&storePath, "store", "store", "specify path to use for datastore")

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
	log.Printf("store          : %s\n", storePath)
	log.Printf("client-host    : %s\n", clientHost)
	log.Printf("client-port    : %s\n", clientPort)
	log.Printf("client-address : %s\n", clientAddr)
	log.Printf("\n")

	var err error

	// create ClientService
	db, err := store.Open(storePath)
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
			case "id":
				conn.WriteString(sid.Id())
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
			case "keys":
				mu.RLock()
				conn.WriteArray(len(items))
				for k := range items {
					// keys[i] = string(k)
					conn.WriteBulkString(k)
				}
				mu.RUnlock()
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
