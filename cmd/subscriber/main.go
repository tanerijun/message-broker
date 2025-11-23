package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Println("Usage: go run main.go <server_address> <topic>")
		fmt.Println("Example: go run main.go localhost:8080 topicA")
		fmt.Println("Press CTRL-C to exit")
		os.Exit(1)
	}

	serverAddr := os.Args[1]
	topic := os.Args[2]

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

	// Send SUBSCRIBE packet
	packet := fmt.Sprintf("SUBSCRIBE|%s\n", topic)
	_, err = conn.Write([]byte(packet))
	if err != nil {
		fmt.Println("Error subscribing:", err)
		os.Exit(1)
	}

	// Read SUBACK
	if scanner.Scan() {
		fmt.Println("Server:", scanner.Text())
	}

	fmt.Printf("Subscribed to topic '%s'. Waiting for messages...\n", topic)
	fmt.Println("Press CTRL-C to exit")

	// Setup signal handler for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("\nReceived interrupt signal. Disconnecting...")
		conn.Close()
		os.Exit(0)
	}()

	// Keep receiving messages
	for scanner.Scan() {
		message := scanner.Text()
		fmt.Println("Received:", message)
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Connection error:", err)
	}
}
