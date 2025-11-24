package main

import (
	"fmt"
	"os"

	"github.com/tanerijun/message-broker/broker"
	"github.com/tanerijun/message-broker/server"
)

func main() {
	if len(os.Args) != 4 {
		fmt.Println("Usage: broker <port> <mode> <peer_address>")
		fmt.Println("Example (primary): broker 8080 primary localhost:8081")
		fmt.Println("Example (backup):  broker 8081 backup localhost:8080")
		os.Exit(1)
	}

	port := os.Args[1]
	modeStr := os.Args[2]
	peerAddr := os.Args[3]

	addr := "0.0.0.0:" + port

	var mode broker.BrokerMode
	if modeStr == "primary" {
		mode = broker.PrimaryMode
	} else if modeStr == "backup" {
		mode = broker.BackupMode
	} else {
		fmt.Println("Mode must be 'primary' or 'backup'")
		os.Exit(1)
	}

	if err := server.StartServer(addr, mode, peerAddr); err != nil {
		fmt.Println("Server error:", err)
		os.Exit(1)
	}
}
