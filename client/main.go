package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Usage: go run client.go <server_address>")
		fmt.Println("Example: go run client.go localhost:8080")
		fmt.Println("Example: go run client.go 192.168.1.100:8080")
		os.Exit(1)
	}

	serverAddr := os.Args[1]

	// Connect to server
	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		fmt.Println("Error connecting to server:", err)
		return
	}
	defer conn.Close()
	fmt.Printf("Connected to server at %s\n", serverAddr)

	// Channel to signal when to print prompt
	promptChan := make(chan bool)

	// Goroutine to read server responses
	go func() {
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			// Print server response on its own line
			fmt.Printf("\r\033[K") // Clear current line
			fmt.Println("Server:", scanner.Text())
			// Signal to print prompt again
			promptChan <- true
		}
	}()

	// Read user input and send to server
	scanner := bufio.NewScanner(os.Stdin)

	// Wait for welcome message before first prompt
	<-promptChan

	fmt.Print("Enter message: ")
	for scanner.Scan() {
		message := strings.TrimSpace(scanner.Text())
		if message != "" {
			conn.Write([]byte(message + "\n"))
			if strings.ToLower(message) == "goodbye" {
				// Wait for goodbye response
				<-promptChan
				return
			}
			// Wait for server response before showing next prompt
			<-promptChan
		}
		fmt.Print("Enter message: ")
	}
}
