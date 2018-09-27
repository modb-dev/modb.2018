package main

import (
	"log"
	"os"
)

func main() {
	var err error

	if len(os.Args) == 1 {
		_ = CmdHelp()
		return
	}

	command := os.Args[1]

	switch command {
	case "--help":
		err = CmdHelp()
	case "help":
		err = CmdHelp()
	case "start":
		err = CmdStart()
	}

	if err != nil {
		log.Fatal("Error running command:", err)
	}
}
