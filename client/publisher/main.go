package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

func main() {
	if len(os.Args) != 4 {
		fmt.Println("Usage: go run main.go <server_address> <topic> <message>")
		fmt.Println("Example: go run main.go localhost:8080 topicA 'Hello World'")
		os.Exit(1)
	}

	serverAddr := os.Args[1]
	topic := os.Args[2]
	message := os.Args[3]

	// Connect to server
	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		fmt.Println("Error connecting to server:", err)
		os.Exit(1)
	}
	defer conn.Close()

	scanner := bufio.NewScanner(conn)

	// Read CONNACK
	if scanner.Scan() {
		fmt.Println("Server:", scanner.Text())
	}

	// Send PUBLISH packet
	packet := fmt.Sprintf("PUBLISH|%s|%s\n", topic, message)
	_, err = conn.Write([]byte(packet))
	if err != nil {
		fmt.Println("Error sending message:", err)
		os.Exit(1)
	}

	// Read PUBACK
	if scanner.Scan() {
		fmt.Println("Server:", scanner.Text())
	}

	fmt.Println("Message published successfully. Disconnecting...")
}
