package server

import (
	"bufio"
	"fmt"
	"net"
	"strings"

	"github.com/tanerijun/message-broker/broker"
	"github.com/tanerijun/message-broker/protocol"
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

// DisconnectEvent represents a client disconnection
type DisconnectEvent struct {
	conn net.Conn
}

func (e DisconnectEvent) GetConn() net.Conn {
	return e.conn
}

// StartServer starts the message broker on the given address
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
	eventChan := make(chan Event, 100)

	brk := broker.NewBroker()

	// Goroutine 1: Application Logic (Reactor)
	go applicationLogic(eventChan, brk)

	// Goroutine 2: Proxy (Accept connections and read messages)
	proxy(listener, eventChan)

	return nil
}

// proxy accepts new connections and reads messages from all clients
func proxy(listener net.Listener, eventChan chan<- Event) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			// Check if it's because the listener was closed (normal shutdown)
			if _, ok := err.(net.Error); ok {
				// Listener closed, exit gracefully
				return
			}
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
	// Connection closed
	eventChan <- DisconnectEvent{conn: conn}
}

// applicationLogic processes all events (new connections and messages)
func applicationLogic(eventChan <-chan Event, brk *broker.Broker) {
	for event := range eventChan {
		switch e := event.(type) {
		case NewConnectionEvent:
			handleNewConnection(e)
		case MessageEvent:
			handleMessage(e, brk)
		case DisconnectEvent:
			handleDisconnect(e, brk)
		}
	}
}

func handleNewConnection(event NewConnectionEvent) {
	fmt.Println("Application: New client connected:", event.GetConn().RemoteAddr())
	event.GetConn().Write([]byte("CONNACK|Connected to Message Broker\n"))
}

func handleMessage(event MessageEvent, brk *broker.Broker) {
	conn := event.GetConn()
	message := event.GetMessage()

	fmt.Printf("Application: Received '%s' from %s\n", message, conn.RemoteAddr())

	// Parse the packet
	packet, err := protocol.ParsePacket(message)
	if err != nil {
		fmt.Printf("Application: Invalid packet from %s: %v\n", conn.RemoteAddr(), err)
		fmt.Fprintf(conn, "ERROR|%v\n", err)
		return
	}

	// Handle the packet
	brk.HandlePacket(packet, conn)

	// If it's a PUBLISH packet, the publisher can disconnect
	if packet.Type == protocol.PUBLISH {
		fmt.Printf("Application: Publisher %s sent message and will disconnect\n", conn.RemoteAddr())
		conn.Close()
	}
}

func handleDisconnect(event DisconnectEvent, brk *broker.Broker) {
	conn := event.GetConn()
	fmt.Printf("Application: Client disconnected: %s\n", conn.RemoteAddr())
	brk.RemoveConnection(conn)
	conn.Close()
}
