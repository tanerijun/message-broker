package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
)

// Event is the interface for all events the reactor processes
type Event interface {
	GetConn() net.Conn
}

// NewConnectionEvent represents a new client connection
type NewConnectionEvent struct {
	conn net.Conn
}

func (e NewConnectionEvent) GetConn() net.Conn {
	return e.conn
}

// MessageEvent represents a message from an existing client
type MessageEvent struct {
	conn    net.Conn
	message string
}

func (e MessageEvent) GetConn() net.Conn {
	return e.conn
}

func (e MessageEvent) GetMessage() string {
	return e.message
}

// StartServer starts the echo server on the given address
func StartServer(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer listener.Close()
	fmt.Println("Server listening on", addr)

	return runServer(listener)
}

// runServer runs the server with the given listener (useful for testing)
func runServer(listener net.Listener) error {
	// Channel for passing events from proxy to application logic
	eventChan := make(chan Event)

	// Goroutine 1: Application Logic (Reactor)
	go applicationLogic(eventChan)

	// Goroutine 2: Proxy (Accept connections and read messages)
	proxy(listener, eventChan)

	return nil
}

// proxy accepts new connections and reads messages from all clients
func proxy(listener net.Listener, eventChan chan<- Event) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		fmt.Println("New connection from:", conn.RemoteAddr())

		// Send new connection event
		eventChan <- NewConnectionEvent{conn: conn}

		// Start reading from this connection
		go readFromConn(conn, eventChan)
	}
}

// readFromConn reads messages from a single connection and sends to event channel
func readFromConn(conn net.Conn, eventChan chan<- Event) {
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		message := strings.TrimSpace(scanner.Text())
		if message != "" {
			eventChan <- MessageEvent{conn: conn, message: message}
		}
	}
}

// applicationLogic processes all events (new connections and messages)
func applicationLogic(eventChan <-chan Event) {
	for event := range eventChan {
		switch e := event.(type) {
		case NewConnectionEvent:
			handleNewConnection(e)
		case MessageEvent:
			handleMessage(e)
		}
	}
}

func handleNewConnection(event NewConnectionEvent) {
	fmt.Println("Application: New client connected")
	event.GetConn().Write([]byte("Welcome to echo server! Type 'goodbye' to disconnect.\n"))
}

func handleMessage(event MessageEvent) {
	conn := event.GetConn()
	message := event.GetMessage()

	fmt.Printf("Application: Received '%s' from %s\n", message, conn.RemoteAddr())

	if strings.ToLower(message) == "goodbye" {
		conn.Write([]byte("Goodbye!\n"))
		conn.Close()
		fmt.Println("Application: Client disconnected")
	} else {
		// Echo the message back
		response := message + "\n"
		conn.Write([]byte(response))
	}
}
