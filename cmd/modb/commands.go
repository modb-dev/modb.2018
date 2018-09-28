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
	badger "github.com/modb-io/modb/store/badger"
	bbolt "github.com/modb-io/modb/store/bbolt"
)

func CmdHelp() error {
	fmt.Println("MoDB server, client and utilities.")
	fmt.Println("")
	fmt.Println("Usage:")
	fmt.Println("")
	fmt.Println("  modb [command]")
	fmt.Println("")
	fmt.Println("Available Commands:")
	fmt.Println("")
	fmt.Println("  start       start a server")
	fmt.Println("")
	fmt.Println("  dump        dump a database")
	fmt.Println("")
	fmt.Println("  help        Help about any command")
	fmt.Println("")
	fmt.Println("Global Flags:")
	fmt.Println("")
	fmt.Println("  -h, --help")
	fmt.Println("        help for modb")
	fmt.Println("")
	fmt.Println("      --verbosity level")
	fmt.Println("        log level")
	fmt.Println("")
	fmt.Println("Use 'modb [command] --help' for more information about a command.")
	return nil
}

func CmdStartHelp() error {
	fmt.Println("Start a MoDB node.")
	fmt.Println("")
	fmt.Println("Usage:")
	fmt.Println("")
	fmt.Println("  modb start [flags]")
	fmt.Println("")
	fmt.Println("Examples:")
	fmt.Println("")
	fmt.Println("  modb start --development")
	fmt.Println("  modb start --production ")
	fmt.Println("")
	fmt.Println("Flags:")
	fmt.Println("")
	fmt.Println("  -h, --help")
	fmt.Println("        help for modb")
	fmt.Println("")
	fmt.Println("      --verbosity level")
	fmt.Println("        log level")
	fmt.Println("")
	fmt.Println("All global flags also apply. See 'modb --help'.")
	return nil
}

func CmdStart() error {
	flagSet := flag.NewFlagSet("", flag.ContinueOnError)

	// path/store
	var storePath string
	flagSet.StringVar(&storePath, "store", "mo.db", "specify path to use for datastore")

	// clientHost
	var clientHost string
	flagSet.StringVar(&clientHost, "client-host", "", "host to listen on for clients")

	// clientPort
	var clientPort string
	flagSet.StringVar(&clientPort, "client-port", "6380", "port to listen on for clients")

	// help
	var help bool
	flagSet.BoolVar(&help, "help", false, "help for MoDB")

	flagSet.Parse(os.Args[2:])

	if help == true {
		return CmdStartHelp()
	}

	// clientAddr
	clientAddr := clientHost + ":" + clientPort

	// --- Print Status ---
	log.Printf("MoDB node starting:\n")
	log.Printf("\n")
	log.Printf("store          : %s\n", storePath)
	log.Printf("client-host    : %s\n", clientHost)
	log.Printf("client-port    : %s\n", clientPort)
	log.Printf("client-address : %s\n", clientAddr)
	log.Printf("\n")

	// create ClientService
	db, err := bbolt.Open(storePath)
	if err != nil {
		fmt.Printf("Error opening path: %s\n", err.Error())
		return err
	}
	defer db.Close()

	// create ClientService
	db1, err := badger.Open("/tmp/badger")
	if err != nil {
		fmt.Printf("Error opening path: %s\n", err.Error())
		return err
	}
	defer db1.Close()

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
				// mu.Lock()
				// items[string(cmd.Args[1])] = cmd.Args[2]
				// mu.Unlock()
				// conn.WriteString("OK")
				name := string(cmd.Args[1])
				json := string(cmd.Args[2])
				// ToDo: validate both name and json.
				err := db.Set(name, json)
				if err != nil {
					conn.WriteError("FATAL Internal Error : " + err.Error())
					return
				}
				conn.WriteString("OK")
			case "keys":
				keys, err := db.Keys()
				if err != nil {
					conn.WriteError("FATAL Internal Error : " + err.Error())
					return
				}
				conn.WriteArray(len(keys))
				for _, val := range keys {
					conn.WriteBulkString(val)
				}
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
	return err
}
