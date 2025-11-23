package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// Helper to read a line with timeout using connection deadline
func readLineWithTimeout(conn net.Conn, timeout time.Duration) (string, error) {
	conn.SetReadDeadline(time.Now().Add(timeout))
	defer conn.SetReadDeadline(time.Time{}) // Clear deadline

	scanner := bufio.NewScanner(conn)
	if scanner.Scan() {
		return scanner.Text(), nil
	}

	if err := scanner.Err(); err != nil {
		return "", err
	}
	return "", fmt.Errorf("connection closed")
}

func TestMessageBroker(t *testing.T) {
	// Start server in background on random port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("Failed to start listener:", err)
	}

	serverAddr := listener.Addr().String()

	// Start server in background
	go runServer(listener)

	time.Sleep(10 * time.Millisecond)

	t.Cleanup(func() {
		listener.Close()
	})

	t.Run("SingleSubscriberReceivesMessage", func(t *testing.T) {
		sub := mustConnect(t, serverAddr)
		defer sub.Close()

		mustRead(t, sub) // CONNACK
		mustWrite(t, sub, "SUBSCRIBE|topicA\n")
		mustRead(t, sub) // SUBACK

		pub := mustConnect(t, serverAddr)
		defer pub.Close()

		mustRead(t, pub) // CONNACK
		mustWrite(t, pub, "PUBLISH|topicA|Hello\n")
		mustRead(t, pub) // PUBACK

		msg := mustRead(t, sub)
		if !strings.Contains(msg, "Hello") {
			t.Errorf("Expected 'Hello', got: %s", msg)
		}
	})

	t.Run("MultipleSubscribersSameTopic", func(t *testing.T) {
		// Two subscribers to topicA
		sub1 := mustConnect(t, serverAddr)
		defer sub1.Close()
		mustRead(t, sub1) // CONNACK
		mustWrite(t, sub1, "SUBSCRIBE|topicA\n")
		mustRead(t, sub1) // SUBACK

		sub2 := mustConnect(t, serverAddr)
		defer sub2.Close()
		mustRead(t, sub2) // CONNACK
		mustWrite(t, sub2, "SUBSCRIBE|topicA\n")
		mustRead(t, sub2) // SUBACK

		// Publish
		pub := mustConnect(t, serverAddr)
		defer pub.Close()
		mustRead(t, pub) // CONNACK
		mustWrite(t, pub, "PUBLISH|topicA|Broadcast\n")
		mustRead(t, pub) // PUBACK

		// Both should receive
		var wg sync.WaitGroup
		wg.Add(2)

		checkReceived := func(conn net.Conn, name string) {
			defer wg.Done()
			msg := mustRead(t, conn)
			if !strings.Contains(msg, "Broadcast") {
				t.Errorf("%s expected 'Broadcast', got: %s", name, msg)
			}
		}

		go checkReceived(sub1, "Sub1")
		go checkReceived(sub2, "Sub2")

		wg.Wait()
	})

	t.Run("DifferentTopicsAreIsolated", func(t *testing.T) {
		// Sub1 to topicA, Sub2 to topicB
		sub1 := mustConnect(t, serverAddr)
		defer sub1.Close()
		mustRead(t, sub1) // CONNACK
		mustWrite(t, sub1, "SUBSCRIBE|topicA\n")
		mustRead(t, sub1) // SUBACK

		sub2 := mustConnect(t, serverAddr)
		defer sub2.Close()
		mustRead(t, sub2) // CONNACK
		mustWrite(t, sub2, "SUBSCRIBE|topicB\n")
		mustRead(t, sub2) // SUBACK

		// Publish to topicA
		pub1 := mustConnect(t, serverAddr)
		defer pub1.Close()
		mustRead(t, pub1) // CONNACK
		mustWrite(t, pub1, "PUBLISH|topicA|OnlyA\n")
		mustRead(t, pub1) // PUBACK

		// Only sub1 should receive
		msg := mustRead(t, sub1)
		if !strings.Contains(msg, "OnlyA") {
			t.Errorf("Sub1 expected 'OnlyA', got: %s", msg)
		}

		// Publish to topicB
		pub2 := mustConnect(t, serverAddr)
		defer pub2.Close()
		mustRead(t, pub2) // CONNACK
		mustWrite(t, pub2, "PUBLISH|topicB|OnlyB\n")
		mustRead(t, pub2) // PUBACK

		// Only sub2 should receive
		msg = mustRead(t, sub2)
		if !strings.Contains(msg, "OnlyB") {
			t.Errorf("Sub2 expected 'OnlyB', got: %s", msg)
		}
	})

	t.Run("SubscriberJoinsLate", func(t *testing.T) {
		// First subscriber
		sub1 := mustConnect(t, serverAddr)
		defer sub1.Close()
		mustRead(t, sub1) // CONNACK
		mustWrite(t, sub1, "SUBSCRIBE|topicX\n")
		mustRead(t, sub1) // SUBACK

		// First publish
		pub1 := mustConnect(t, serverAddr)
		defer pub1.Close()
		mustRead(t, pub1) // CONNACK
		mustWrite(t, pub1, "PUBLISH|topicX|First\n")
		mustRead(t, pub1) // PUBACK

		msg := mustRead(t, sub1)
		if !strings.Contains(msg, "First") {
			t.Errorf("Expected 'First', got: %s", msg)
		}

		// Second subscriber joins
		sub2 := mustConnect(t, serverAddr)
		defer sub2.Close()
		mustRead(t, sub2) // CONNACK
		mustWrite(t, sub2, "SUBSCRIBE|topicX\n")
		mustRead(t, sub2) // SUBACK

		// Second publish - both should receive
		pub2 := mustConnect(t, serverAddr)
		defer pub2.Close()
		mustRead(t, pub2) // CONNACK
		mustWrite(t, pub2, "PUBLISH|topicX|Second\n")
		mustRead(t, pub2) // PUBACK

		var wg sync.WaitGroup
		wg.Add(2)

		checkMsg := func(conn net.Conn, name string) {
			defer wg.Done()
			msg := mustRead(t, conn)
			if !strings.Contains(msg, "Second") {
				t.Errorf("%s expected 'Second', got: %s", name, msg)
			}
		}

		go checkMsg(sub1, "Sub1")
		go checkMsg(sub2, "Sub2")

		wg.Wait()
	})
}

func mustConnect(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal("Failed to connect:", err)
	}
	return conn
}

func mustRead(t *testing.T, conn net.Conn) string {
	t.Helper()
	line, err := readLineWithTimeout(conn, 200*time.Millisecond)
	if err != nil {
		t.Fatal("Failed to read:", err)
	}
	return line
}

func mustWrite(t *testing.T, conn net.Conn, data string) {
	t.Helper()
	_, err := conn.Write([]byte(data))
	if err != nil {
		t.Fatal("Failed to write:", err)
	}
}
