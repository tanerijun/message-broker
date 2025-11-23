package broker

import (
	"net"
	"strings"
	"testing"

	"github.com/tanerijun/message-broker/protocol"
)

// mockConn is a mock connection for testing
type mockConn struct {
	net.Conn
	writeBuffer []string
	closed      bool
}

func newMockConn() *mockConn {
	return &mockConn{
		writeBuffer: []string{},
	}
}

func (m *mockConn) Write(b []byte) (n int, err error) {
	m.writeBuffer = append(m.writeBuffer, string(b))
	return len(b), nil
}

func (m *mockConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 12345}
}

func (m *mockConn) Close() error {
	m.closed = true
	return nil
}

func TestBrokerSubscribe(t *testing.T) {
	b := NewBroker()
	conn := newMockConn()

	b.Subscribe("testTopic", conn)

	if count := b.GetSubscriberCount("testTopic"); count != 1 {
		t.Errorf("Expected 1 subscriber, got %d", count)
	}

	// Subscribe another connection to the same topic
	conn2 := newMockConn()
	b.Subscribe("testTopic", conn2)

	if count := b.GetSubscriberCount("testTopic"); count != 2 {
		t.Errorf("Expected 2 subscribers, got %d", count)
	}
}

func TestBrokerPublish(t *testing.T) {
	b := NewBroker()

	// Subscribe two connections to topicA
	conn1 := newMockConn()
	conn2 := newMockConn()
	b.Subscribe("topicA", conn1)
	b.Subscribe("topicA", conn2)

	// Subscribe one connection to topicB
	conn3 := newMockConn()
	b.Subscribe("topicB", conn3)

	// Publish to topicA
	publisher := newMockConn()
	b.Publish("topicA", "Hello", publisher)

	// Check that conn1 and conn2 received the message
	if len(conn1.writeBuffer) != 1 {
		t.Errorf("conn1 should have received 1 message, got %d", len(conn1.writeBuffer))
	}
	if len(conn2.writeBuffer) != 1 {
		t.Errorf("conn2 should have received 1 message, got %d", len(conn2.writeBuffer))
	}
	if len(conn3.writeBuffer) != 0 {
		t.Errorf("conn3 should not have received any messages, got %d", len(conn3.writeBuffer))
	}

	// Verify message content
	if !strings.Contains(conn1.writeBuffer[0], "Hello") {
		t.Errorf("Message should contain 'Hello', got %s", conn1.writeBuffer[0])
	}
}

func TestBrokerRemoveConnection(t *testing.T) {
	b := NewBroker()
	conn := newMockConn()

	b.Subscribe("topic1", conn)
	b.Subscribe("topic2", conn)

	if count := b.GetSubscriberCount("topic1"); count != 1 {
		t.Errorf("Expected 1 subscriber for topic1, got %d", count)
	}

	b.RemoveConnection(conn)

	if count := b.GetSubscriberCount("topic1"); count != 0 {
		t.Errorf("Expected 0 subscribers for topic1 after removal, got %d", count)
	}
	if count := b.GetSubscriberCount("topic2"); count != 0 {
		t.Errorf("Expected 0 subscribers for topic2 after removal, got %d", count)
	}
}

func TestHandlePacket(t *testing.T) {
	b := NewBroker()
	conn := newMockConn()

	// Test SUBSCRIBE packet
	subPacket := &protocol.Packet{
		Type:  protocol.SUBSCRIBE,
		Topic: "testTopic",
	}
	b.HandlePacket(subPacket, conn)

	if count := b.GetSubscriberCount("testTopic"); count != 1 {
		t.Errorf("Expected 1 subscriber, got %d", count)
	}
	if len(conn.writeBuffer) != 1 || !strings.Contains(conn.writeBuffer[0], "SUBACK") {
		t.Errorf("Expected SUBACK response")
	}

	// Test PUBLISH packet
	pubConn := newMockConn()
	pubPacket := &protocol.Packet{
		Type:    protocol.PUBLISH,
		Topic:   "testTopic",
		Payload: "Test Message",
	}
	b.HandlePacket(pubPacket, pubConn)

	if len(conn.writeBuffer) != 2 {
		t.Errorf("Subscriber should have received 2 messages (SUBACK + MESSAGE), got %d", len(conn.writeBuffer))
	}
	if !strings.Contains(conn.writeBuffer[1], "Test Message") {
		t.Errorf("Expected to receive 'Test Message', got %s", conn.writeBuffer[1])
	}
}
