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
	b := NewBroker(PrimaryMode, "")
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
	b := NewBroker(PrimaryMode, "")

	// Subscribe two connections to topicA
	conn1 := newMockConn()
	conn2 := newMockConn()
	b.Subscribe("topicA", conn1)
	b.Subscribe("topicA", conn2)

	// Subscribe one connection to topicB
	conn3 := newMockConn()
	b.Subscribe("topicB", conn3)

	// Publish to topicA
	b.Publish("topicA", "Hello", "msg-test-1")

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
	b := NewBroker(PrimaryMode, "")
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
	b := NewBroker(PrimaryMode, "")
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
		Type:      protocol.PUBLISH,
		Topic:     "testTopic",
		Payload:   "Test Message",
		MessageID: "msg-test-1",
	}
	b.HandlePacket(pubPacket, pubConn)

	if len(conn.writeBuffer) != 2 {
		t.Errorf("Subscriber should have received 2 messages (SUBACK + MESSAGE), got %d", len(conn.writeBuffer))
	}
	if !strings.Contains(conn.writeBuffer[1], "Test Message") {
		t.Errorf("Expected to receive 'Test Message', got %s", conn.writeBuffer[1])
	}
}

func TestBrokerModeSwitch(t *testing.T) {
	b := NewBroker(BackupMode, "")

	if mode := b.GetMode(); mode != BackupMode {
		t.Errorf("Expected BackupMode, got %v", mode)
	}

	b.SetMode(PrimaryMode)

	if mode := b.GetMode(); mode != PrimaryMode {
		t.Errorf("Expected PrimaryMode after switch, got %v", mode)
	}
}

func TestBrokerBackupModeQueuesMessages(t *testing.T) {
	b := NewBroker(BackupMode, "")

	// Subscribe a connection
	conn := newMockConn()
	b.Subscribe("testTopic", conn)

	// Publish in backup mode should queue message, not deliver
	b.Publish("testTopic", "Hello Backup", "msg-backup-1")

	// Subscriber should NOT receive message immediately (backup mode)
	if len(conn.writeBuffer) != 0 {
		t.Errorf("Backup mode should not deliver immediately, got %d messages", len(conn.writeBuffer))
	}

	// Check that message is stored in replica store
	b.mu.RLock()
	if _, exists := b.replicaStore["msg-backup-1"]; !exists {
		t.Error("Message should be queued in replica store")
	}
	b.mu.RUnlock()
}

func TestHandleReplication(t *testing.T) {
	b := NewBroker(BackupMode, "")

	b.handleReplication("msg-123", "topicA", "payload", 1)

	b.mu.RLock()
	msg, exists := b.replicaStore["msg-123"]
	b.mu.RUnlock()

	if !exists {
		t.Fatal("Message should be stored in replica store")
	}

	if msg.MessageID != "msg-123" {
		t.Errorf("MessageID = %v, want msg-123", msg.MessageID)
	}
	if msg.Topic != "topicA" {
		t.Errorf("Topic = %v, want topicA", msg.Topic)
	}
	if msg.Payload != "payload" {
		t.Errorf("Payload = %v, want payload", msg.Payload)
	}
	if msg.SeqNum != 1 {
		t.Errorf("SeqNum = %v, want 1", msg.SeqNum)
	}
	if msg.State != StateReplicated {
		t.Errorf("State = %v, want StateReplicated", msg.State)
	}
}

func TestMarkDelivered(t *testing.T) {
	b := NewBroker(BackupMode, "")

	// First replicate a message
	b.handleReplication("msg-200", "topicX", "data", 5)

	// Verify it's in replica store
	b.mu.RLock()
	if _, exists := b.replicaStore["msg-200"]; !exists {
		t.Fatal("Message should be in replica store before marking delivered")
	}
	b.mu.RUnlock()

	// Mark as delivered
	b.markDelivered("msg-200")

	// Should be pruned from replica store
	b.mu.RLock()
	if _, exists := b.replicaStore["msg-200"]; exists {
		t.Error("Message should be pruned after marking delivered")
	}
	// Should be in delivered map
	if !b.deliveredMsgs["msg-200"] {
		t.Error("Message should be marked as delivered")
	}
	b.mu.RUnlock()
}

func TestMarkDeliveredDuplicateHandling(t *testing.T) {
	b := NewBroker(BackupMode, "")

	// Replicate message
	b.handleReplication("msg-dup", "topic", "data", 1)

	// Mark as delivered
	b.markDelivered("msg-dup")

	// Try to replicate again (duplicate)
	b.handleReplication("msg-dup", "topic", "data", 1)

	// Should not be in replica store (already delivered)
	b.mu.RLock()
	count := len(b.replicaStore)
	b.mu.RUnlock()

	if count != 0 {
		t.Errorf("Duplicate message should not be stored, got %d messages", count)
	}
}

func TestPromoteToPrimary(t *testing.T) {
	b := NewBroker(BackupMode, "")

	// Subscribe connections to topics
	conn1 := newMockConn()
	conn2 := newMockConn()
	b.Subscribe("topicA", conn1)
	b.Subscribe("topicB", conn2)

	// Simulate replicated messages
	b.handleReplication("msg-1", "topicA", "first", 1)
	b.handleReplication("msg-2", "topicB", "second", 2)
	b.handleReplication("msg-3", "topicA", "third", 3)

	// Verify they're in replica store
	b.mu.RLock()
	if len(b.replicaStore) != 3 {
		t.Fatalf("Expected 3 messages in replica store, got %d", len(b.replicaStore))
	}
	b.mu.RUnlock()

	// Promote to primary
	b.PromoteToPrimary()

	// Check mode changed
	if mode := b.GetMode(); mode != PrimaryMode {
		t.Errorf("Expected PrimaryMode after promotion, got %v", mode)
	}

	// All messages should be delivered
	if len(conn1.writeBuffer) < 2 {
		t.Errorf("conn1 should receive 2 messages (topicA), got %d", len(conn1.writeBuffer))
	}
	if len(conn2.writeBuffer) < 1 {
		t.Errorf("conn2 should receive 1 message (topicB), got %d", len(conn2.writeBuffer))
	}

	// Messages should be delivered in order (seq 1, 2, 3)
	if !strings.Contains(conn1.writeBuffer[0], "first") {
		t.Errorf("First message to conn1 should contain 'first'")
	}
	if !strings.Contains(conn2.writeBuffer[0], "second") {
		t.Errorf("Message to conn2 should contain 'second'")
	}
	if !strings.Contains(conn1.writeBuffer[1], "third") {
		t.Errorf("Second message to conn1 should contain 'third'")
	}

	// Replica store should be empty (pruned)
	b.mu.RLock()
	if len(b.replicaStore) != 0 {
		t.Errorf("Replica store should be empty after promotion, got %d", len(b.replicaStore))
	}
	b.mu.RUnlock()
}

func TestHandlePacketReplication(t *testing.T) {
	b := NewBroker(BackupMode, "")
	conn := newMockConn()

	// Test REPLICATE packet
	replicatePacket := &protocol.Packet{
		Type:      protocol.REPLICATE,
		MessageID: "msg-rep-1",
		Topic:     "events",
		Payload:   "data",
		SeqNum:    10,
	}
	b.HandlePacket(replicatePacket, conn)

	b.mu.RLock()
	msg, exists := b.replicaStore["msg-rep-1"]
	b.mu.RUnlock()

	if !exists {
		t.Fatal("REPLICATE packet should store message")
	}
	if msg.SeqNum != 10 {
		t.Errorf("SeqNum = %v, want 10", msg.SeqNum)
	}
}

func TestHandlePacketMarkDelivered(t *testing.T) {
	b := NewBroker(BackupMode, "")
	conn := newMockConn()

	// First replicate a message
	b.handleReplication("msg-md-1", "topic", "payload", 5)

	// Test MARK_DELIVERED packet
	markPacket := &protocol.Packet{
		Type:      protocol.MARK_DELIVERED,
		MessageID: "msg-md-1",
		SeqNum:    5,
	}
	b.HandlePacket(markPacket, conn)

	b.mu.RLock()
	if _, exists := b.replicaStore["msg-md-1"]; exists {
		t.Error("Message should be pruned after MARK_DELIVERED")
	}
	if !b.deliveredMsgs["msg-md-1"] {
		t.Error("Message should be in deliveredMsgs")
	}
	b.mu.RUnlock()
}

func TestHandlePacketHeartbeat(t *testing.T) {
	b := NewBroker(PrimaryMode, "")
	conn := newMockConn()

	heartbeatPacket := &protocol.Packet{
		Type: protocol.HEARTBEAT,
	}
	b.HandlePacket(heartbeatPacket, conn)

	if len(conn.writeBuffer) != 1 {
		t.Fatalf("Expected 1 response, got %d", len(conn.writeBuffer))
	}

	response := conn.writeBuffer[0]
	if !strings.Contains(response, "HEARTBEAT_ACK") {
		t.Errorf("Expected HEARTBEAT_ACK, got %s", response)
	}
}

func TestGetNextSeqNum(t *testing.T) {
	b := NewBroker(PrimaryMode, "")

	seq1 := b.getNextSeqNum()
	seq2 := b.getNextSeqNum()
	seq3 := b.getNextSeqNum()

	if seq1 != 1 {
		t.Errorf("First seq = %d, want 1", seq1)
	}
	if seq2 != 2 {
		t.Errorf("Second seq = %d, want 2", seq2)
	}
	if seq3 != 3 {
		t.Errorf("Third seq = %d, want 3", seq3)
	}
}

func TestRemoveFailedConnections(t *testing.T) {
	b := NewBroker(PrimaryMode, "")

	conn1 := newMockConn()
	conn2 := newMockConn()
	conn3 := newMockConn()

	b.Subscribe("topic", conn1)
	b.Subscribe("topic", conn2)
	b.Subscribe("topic", conn3)

	if count := b.GetSubscriberCount("topic"); count != 3 {
		t.Fatalf("Expected 3 subscribers, got %d", count)
	}

	// Remove conn2 as failed
	failedConns := []net.Conn{conn2}
	b.removeFailedConnections("topic", failedConns)

	if count := b.GetSubscriberCount("topic"); count != 2 {
		t.Errorf("Expected 2 subscribers after removal, got %d", count)
	}

	// Verify conn1 and conn3 are still there
	b.mu.RLock()
	subs := b.subscriptions["topic"]
	found1 := false
	found2 := false
	found3 := false
	for _, sub := range subs {
		if sub == conn1 {
			found1 = true
		}
		if sub == conn2 {
			found2 = true
		}
		if sub == conn3 {
			found3 = true
		}
	}
	b.mu.RUnlock()

	if !found1 {
		t.Error("conn1 should still be subscribed")
	}
	if found2 {
		t.Error("conn2 should be removed")
	}
	if !found3 {
		t.Error("conn3 should still be subscribed")
	}
}

func TestModeString(t *testing.T) {
	if modeString(PrimaryMode) != "PRIMARY" {
		t.Errorf("PrimaryMode string = %v, want PRIMARY", modeString(PrimaryMode))
	}
	if modeString(BackupMode) != "BACKUP" {
		t.Errorf("BackupMode string = %v, want BACKUP", modeString(BackupMode))
	}
}
