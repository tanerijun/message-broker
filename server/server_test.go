package main

import (
	"bufio"
	"net"
	"strings"
	"testing"
)

func TestEchoServer(t *testing.T) {
	// Start server in background on random port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("Failed to start listener:", err)
	}
	defer listener.Close()

	serverAddr := listener.Addr().String()

	// Start server in background
	go runServer(listener)

	t.Run("SingleClientEcho", func(t *testing.T) {
		conn, err := net.Dial("tcp", serverAddr)
		if err != nil {
			t.Fatal("Failed to connect:", err)
		}
		defer conn.Close()

		scanner := bufio.NewScanner(conn)

		// Read welcome message
		if !scanner.Scan() {
			t.Fatal("Failed to read welcome message")
		}

		// Send test message
		conn.Write([]byte("hello\n"))

		// Read echo response
		if !scanner.Scan() {
			t.Fatal("Failed to read echo response")
		}
		response := strings.TrimSpace(scanner.Text())
		if response != "hello" {
			t.Errorf("Expected 'hello', got '%s'", response)
		}

		// Send goodbye
		conn.Write([]byte("goodbye\n"))

		// Read goodbye response
		if !scanner.Scan() {
			t.Fatal("Failed to read goodbye response")
		}
	})

	t.Run("MultipleClients", func(t *testing.T) {
		// Connect two clients
		conn1, err := net.Dial("tcp", serverAddr)
		if err != nil {
			t.Fatal("Client 1 failed to connect:", err)
		}
		defer conn1.Close()

		conn2, err := net.Dial("tcp", serverAddr)
		if err != nil {
			t.Fatal("Client 2 failed to connect:", err)
		}
		defer conn2.Close()

		scanner1 := bufio.NewScanner(conn1)
		scanner2 := bufio.NewScanner(conn2)

		// Read welcome messages
		scanner1.Scan()
		scanner2.Scan()

		// Client 1 sends message
		conn1.Write([]byte("client1\n"))
		if !scanner1.Scan() {
			t.Fatal("Client 1 failed to read response")
		}
		if strings.TrimSpace(scanner1.Text()) != "client1" {
			t.Error("Client 1 did not receive correct echo")
		}

		// Client 2 sends message
		conn2.Write([]byte("client2\n"))
		if !scanner2.Scan() {
			t.Fatal("Client 2 failed to read response")
		}
		if strings.TrimSpace(scanner2.Text()) != "client2" {
			t.Error("Client 2 did not receive correct echo")
		}

		// Disconnect both
		conn1.Write([]byte("goodbye\n"))
		conn2.Write([]byte("goodbye\n"))
	})
}
