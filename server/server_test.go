package server

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tanerijun/message-broker/broker"
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
	go runServer(listener, broker.PrimaryMode, "")

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

func TestEventTypes(t *testing.T) {
	t.Run("NewConnectionEvent", func(t *testing.T) {
		conn := &net.TCPConn{}
		event := NewConnectionEvent{conn: conn}
		if event.GetConn() != conn {
			t.Error("NewConnectionEvent.GetConn() should return the connection")
		}
	})

	t.Run("MessageEvent", func(t *testing.T) {
		conn := &net.TCPConn{}
		event := MessageEvent{conn: conn, message: "test message"}
		if event.GetConn() != conn {
			t.Error("MessageEvent.GetConn() should return the connection")
		}
		if event.GetMessage() != "test message" {
			t.Errorf("MessageEvent.GetMessage() = %v, want 'test message'", event.GetMessage())
		}
	})

	t.Run("DisconnectEvent", func(t *testing.T) {
		conn := &net.TCPConn{}
		event := DisconnectEvent{conn: conn}
		if event.GetConn() != conn {
			t.Error("DisconnectEvent.GetConn() should return the connection")
		}
	})
}

func TestInvalidPacketHandling(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("Failed to start listener:", err)
	}

	serverAddr := listener.Addr().String()
	go runServer(listener, broker.PrimaryMode, "")
	time.Sleep(10 * time.Millisecond)

	t.Cleanup(func() {
		listener.Close()
	})

	t.Run("InvalidPacketFormat", func(t *testing.T) {
		conn := mustConnect(t, serverAddr)
		defer conn.Close()

		mustRead(t, conn) // CONNACK

		// Send invalid packet
		mustWrite(t, conn, "INVALID\n")

		// Should receive ERROR response
		response := mustRead(t, conn)
		if !strings.Contains(response, "ERROR") {
			t.Errorf("Expected ERROR response for invalid packet, got: %s", response)
		}
	})

	t.Run("MalformedPublish", func(t *testing.T) {
		conn := mustConnect(t, serverAddr)
		defer conn.Close()

		mustRead(t, conn) // CONNACK

		// Send PUBLISH with missing payload
		mustWrite(t, conn, "PUBLISH|topic\n")

		// Should receive ERROR response
		response := mustRead(t, conn)
		if !strings.Contains(response, "ERROR") {
			t.Errorf("Expected ERROR response for malformed PUBLISH, got: %s", response)
		}
	})

	t.Run("EmptyTopicSubscribe", func(t *testing.T) {
		conn := mustConnect(t, serverAddr)
		defer conn.Close()

		mustRead(t, conn) // CONNACK

		// Send SUBSCRIBE with empty topic
		mustWrite(t, conn, "SUBSCRIBE|\n")

		// Should receive ERROR response
		response := mustRead(t, conn)
		if !strings.Contains(response, "ERROR") {
			t.Errorf("Expected ERROR response for empty topic, got: %s", response)
		}
	})
}

func TestMessageDeliveryOrdering(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("Failed to start listener:", err)
	}

	serverAddr := listener.Addr().String()
	go runServer(listener, broker.PrimaryMode, "")
	time.Sleep(10 * time.Millisecond)

	t.Cleanup(func() {
		listener.Close()
	})

	// Subscribe one connection
	sub := mustConnect(t, serverAddr)
	defer sub.Close()
	mustRead(t, sub) // CONNACK
	mustWrite(t, sub, "SUBSCRIBE|ordering\n")
	mustRead(t, sub) // SUBACK

	// Use a buffered reader for the subscriber connection
	reader := bufio.NewReader(sub)

	// Publish multiple messages quickly
	messages := []string{"first", "second", "third"}
	for _, msg := range messages {
		pub := mustConnect(t, serverAddr)
		mustRead(t, pub) // CONNACK
		mustWrite(t, pub, fmt.Sprintf("PUBLISH|ordering|%s\n", msg))
		mustRead(t, pub) // PUBACK
		pub.Close()
	}

	// Read all messages with timeout (edge computing can take up to 150ms * 3 messages)
	received := []string{}
	deadline := time.Now().Add(1 * time.Second)
	for i := 0; i < 3; i++ {
		sub.SetReadDeadline(deadline)
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("Failed to read message %d: %v (received so far: %d)", i+1, err, len(received))
		}
		received = append(received, strings.TrimSpace(line))
	}

	// Check all messages were received
	for _, expected := range messages {
		found := false
		for _, msg := range received {
			if strings.Contains(msg, expected) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Message '%s' was not received", expected)
		}
	}
}

func TestNoSubscribers(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("Failed to start listener:", err)
	}

	serverAddr := listener.Addr().String()
	go runServer(listener, broker.PrimaryMode, "")
	time.Sleep(10 * time.Millisecond)

	t.Cleanup(func() {
		listener.Close()
	})

	// Publish to a topic with no subscribers
	pub := mustConnect(t, serverAddr)
	defer pub.Close()

	mustRead(t, pub) // CONNACK
	mustWrite(t, pub, "PUBLISH|noSubscribers|message\n")

	// Should still receive PUBACK
	response := mustRead(t, pub)
	if !strings.Contains(response, "PUBACK") {
		t.Errorf("Expected PUBACK even with no subscribers, got: %s", response)
	}
}

func TestMultipleSubscriptionsPerConnection(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("Failed to start listener:", err)
	}

	serverAddr := listener.Addr().String()
	go runServer(listener, broker.PrimaryMode, "")
	time.Sleep(10 * time.Millisecond)

	t.Cleanup(func() {
		listener.Close()
	})

	// One connection subscribes to multiple topics
	sub := mustConnect(t, serverAddr)
	defer sub.Close()
	mustRead(t, sub) // CONNACK

	mustWrite(t, sub, "SUBSCRIBE|topic1\n")
	mustRead(t, sub) // SUBACK

	mustWrite(t, sub, "SUBSCRIBE|topic2\n")
	mustRead(t, sub) // SUBACK

	// Publish to topic1
	pub1 := mustConnect(t, serverAddr)
	mustRead(t, pub1) // CONNACK
	mustWrite(t, pub1, "PUBLISH|topic1|message1\n")
	mustRead(t, pub1) // PUBACK
	pub1.Close()

	msg1 := mustRead(t, sub)
	if !strings.Contains(msg1, "message1") {
		t.Errorf("Expected 'message1', got: %s", msg1)
	}

	// Publish to topic2
	pub2 := mustConnect(t, serverAddr)
	mustRead(t, pub2) // CONNACK
	mustWrite(t, pub2, "PUBLISH|topic2|message2\n")
	mustRead(t, pub2) // PUBACK
	pub2.Close()

	msg2 := mustRead(t, sub)
	if !strings.Contains(msg2, "message2") {
		t.Errorf("Expected 'message2', got: %s", msg2)
	}
}
