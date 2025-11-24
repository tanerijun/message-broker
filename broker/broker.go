package broker

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/tanerijun/message-broker/protocol"
)

type BrokerMode int

const (
	PrimaryMode BrokerMode = iota
	BackupMode
)

type MessageState int

const (
	StateReplicated MessageState = iota // Stored but not yet delivered
	StateDelivered                      // Delivered by primary, can be pruned
)

type ReplicatedMessage struct {
	MessageID string
	Topic     string
	Payload   string
	Timestamp time.Time
	State     MessageState
	SeqNum    int
}

type Broker struct {
	mu               sync.RWMutex
	subscriptions    map[string][]net.Conn         // topic -> list of subscriber connections
	mode             BrokerMode                    // Primary or Backup
	backupAddr       string                        // Address of backup broker
	backupConn       net.Conn                      // Persistent connection to backup
	primaryAddr      string                        // Address of primary (for backup)
	replicaStore     map[string]*ReplicatedMessage // MessageID -> Message
	deliveredMsgs    map[string]bool               // Deduplication tracking
	nextSeqNum       int                           // Sequence number generator
	lastDeliveredSeq map[string]int                // topic -> last delivered seq
	isPrimaryAlive   bool                          // Health status
}

func NewBroker(mode BrokerMode, peerAddr string) *Broker {
	b := &Broker{
		subscriptions:    make(map[string][]net.Conn),
		replicaStore:     make(map[string]*ReplicatedMessage),
		deliveredMsgs:    make(map[string]bool),
		mode:             mode,
		isPrimaryAlive:   true,
		nextSeqNum:       1,
		lastDeliveredSeq: make(map[string]int),
	}

	if mode == PrimaryMode {
		b.backupAddr = peerAddr
	} else {
		b.primaryAddr = peerAddr
	}

	return b
}

func (b *Broker) SetMode(mode BrokerMode) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.mode = mode
	fmt.Printf("Broker: Switched to %s mode\n", modeString(mode))
}

func (b *Broker) GetMode() BrokerMode {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.mode
}

func (b *Broker) Subscribe(topic string, conn net.Conn) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.subscriptions[topic] = append(b.subscriptions[topic], conn)
	fmt.Printf("Broker: Client %s subscribed to topic '%s' (%d total subscribers)\n",
		conn.RemoteAddr(), topic, len(b.subscriptions[topic]))
}

func (b *Broker) Publish(topic string, payload string, messageID string) {
	b.mu.RLock()
	mode := b.mode
	b.mu.RUnlock()

	if mode == BackupMode {
		// Backup mode: Accept PUBLISH and queue for processing after promotion
		// This handles the case where publisher fails over before backup promotes
		fmt.Printf("Broker (Backup): Received direct PUBLISH for topic '%s', queueing as replica\n", topic)

		// Generate sequence number and store as replicated message
		seqNum := b.getNextSeqNum()
		b.mu.Lock()
		if !b.deliveredMsgs[messageID] {
			b.replicaStore[messageID] = &ReplicatedMessage{
				MessageID: messageID,
				Topic:     topic,
				Payload:   payload,
				Timestamp: time.Now(),
				State:     StateReplicated,
				SeqNum:    seqNum,
			}
			fmt.Printf("Broker (Backup): Queued PUBLISH as replica %s (seq=%d, total: %d undelivered)\n",
				messageID, seqNum, len(b.replicaStore))
		}
		b.mu.Unlock()
		return
	}

	// PRIMARY PATH
	fmt.Printf("Broker (Primary): Processing message %s for topic '%s'\n", messageID, topic)

	// 1. Replicate to backup FIRST
	seqNum := b.getNextSeqNum()
	if err := b.replicateToBackup(messageID, topic, payload, seqNum); err != nil {
		fmt.Printf("Broker (Primary): Replication failed: %v\n", err)
		// Continue anyway - in production you might want retry logic
	} else {
		fmt.Printf("Broker (Primary): Message %s replicated with seqNum %d\n", messageID, seqNum)
	}

	// 2. Edge computing simulation (50-150ms busy loop)
	edgeComputingTime := 50 + rand.Intn(101)
	fmt.Printf("Broker (Primary): Edge computing for %dms\n", edgeComputingTime)
	// Busy loop to simulate edge computing
	start := time.Now()
	for time.Since(start) < time.Duration(edgeComputingTime)*time.Millisecond {
		// Busy wait
	}

	// 3. Deliver to subscribers
	delivered := b.deliverToSubscribers(topic, payload, seqNum)

	if !delivered {
		fmt.Printf("Broker (Primary): No subscribers or delivery failed for message %s\n", messageID)
		// Even if no subscribers, we should mark it as delivered to prune from backup
	}

	// 4. Mark delivered in backup
	if err := b.markDeliveredInBackup(messageID, seqNum); err != nil {
		fmt.Printf("Broker (Primary): Failed to mark delivered in backup: %v\n", err)
		// Backup will re-deliver on failover if this fails
	} else {
		fmt.Printf("Broker (Primary): Message %s marked as delivered in backup\n", messageID)
	}
}

func (b *Broker) getNextSeqNum() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	seq := b.nextSeqNum
	b.nextSeqNum++
	return seq
}

func (b *Broker) deliverToSubscribers(topic, payload string, seqNum int) bool {
	b.mu.RLock()
	subscribers := b.subscriptions[topic]
	b.mu.RUnlock()

	if len(subscribers) == 0 {
		return false
	}

	fmt.Printf("Broker: Delivering seq=%d to topic '%s' (%d subscribers)\n",
		seqNum, topic, len(subscribers))

	message := fmt.Sprintf("MESSAGE|%s|%s\n", topic, payload)
	failedConns := []net.Conn{}
	successCount := 0

	for _, conn := range subscribers {
		if _, err := conn.Write([]byte(message)); err != nil {
			fmt.Printf("Broker: Failed to send to %s: %v\n", conn.RemoteAddr(), err)
			failedConns = append(failedConns, conn)
		} else {
			successCount++
		}
	}

	if len(failedConns) > 0 {
		b.removeFailedConnections(topic, failedConns)
	}

	// Update last delivered sequence for this topic
	b.mu.Lock()
	b.lastDeliveredSeq[topic] = seqNum
	b.mu.Unlock()

	return successCount > 0
}

func (b *Broker) replicateToBackup(messageID, topic, payload string, seqNum int) error {
	if b.backupAddr == "" {
		return fmt.Errorf("no backup configured")
	}

	b.mu.Lock()
	if b.backupConn == nil {
		conn, err := net.Dial("tcp", b.backupAddr)
		if err != nil {
			b.mu.Unlock()
			return fmt.Errorf("failed to connect to backup: %w", err)
		}
		b.backupConn = conn

		// Read CONNACK from backup
		scanner := make([]byte, 1024)
		conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		_, err = conn.Read(scanner)
		conn.SetReadDeadline(time.Time{}) // Clear deadline
		if err != nil {
			b.backupConn = nil
			b.mu.Unlock()
			return fmt.Errorf("failed to read CONNACK from backup: %w", err)
		}
	}
	conn := b.backupConn
	b.mu.Unlock()

	packet := &protocol.Packet{
		Type:      protocol.REPLICATE,
		MessageID: messageID,
		Topic:     topic,
		Payload:   payload,
		SeqNum:    seqNum,
	}

	_, err := fmt.Fprintf(conn, "%s\n", packet.String())
	if err != nil {
		// Connection failed, clear it so we reconnect next time
		b.mu.Lock()
		b.backupConn = nil
		b.mu.Unlock()
	}
	return err
}

func (b *Broker) markDeliveredInBackup(messageID string, seqNum int) error {
	b.mu.RLock()
	conn := b.backupConn
	b.mu.RUnlock()

	if conn == nil {
		return fmt.Errorf("no backup connection")
	}

	packet := &protocol.Packet{
		Type:      protocol.MARK_DELIVERED,
		MessageID: messageID,
		SeqNum:    seqNum,
	}

	_, err := fmt.Fprintf(conn, "%s\n", packet.String())
	if err != nil {
		b.mu.Lock()
		b.backupConn = nil
		b.mu.Unlock()
	}
	return err
}

func (b *Broker) handleReplication(messageID, topic, payload string, seqNum int) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.deliveredMsgs[messageID] {
		fmt.Printf("Broker (Backup): Message %s already delivered, ignoring duplicate\n", messageID)
		return
	}

	b.replicaStore[messageID] = &ReplicatedMessage{
		MessageID: messageID,
		Topic:     topic,
		Payload:   payload,
		Timestamp: time.Now(),
		State:     StateReplicated,
		SeqNum:    seqNum,
	}

	fmt.Printf("Broker (Backup): Stored replica %s (total: %d undelivered)\n",
		messageID, len(b.replicaStore))
}

func (b *Broker) markDelivered(messageID string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if msg, exists := b.replicaStore[messageID]; exists {
		msg.State = StateDelivered
		b.deliveredMsgs[messageID] = true

		// PRUNE: Delete delivered messages
		delete(b.replicaStore, messageID)
		fmt.Printf("Broker (Backup): Message %s marked delivered and PRUNED (remaining: %d)\n",
			messageID, len(b.replicaStore))
	}
}

func (b *Broker) PromoteToPrimary() {
	b.mu.Lock()
	defer b.mu.Unlock()

	fmt.Println("=== Broker: PROMOTING BACKUP TO PRIMARY ===")
	b.mode = PrimaryMode

	// Get all undelivered messages
	type msgWithSeq struct {
		msg *ReplicatedMessage
		seq int
	}

	undelivered := []msgWithSeq{}
	for _, msg := range b.replicaStore {
		if msg.State == StateReplicated {
			undelivered = append(undelivered, msgWithSeq{msg: msg, seq: msg.SeqNum})
		}
	}

	// Maintain order
	sort.Slice(undelivered, func(i, j int) bool {
		return undelivered[i].seq < undelivered[j].seq
	})

	fmt.Printf("Broker: Processing %d undelivered messages in order\n", len(undelivered))

	for _, item := range undelivered {
		msg := item.msg
		fmt.Printf("Broker: Delivering stored message %s (seq=%d) to topic %s\n",
			msg.MessageID, msg.SeqNum, msg.Topic)

		// Deliver without replication (we're now primary, no backup)
		b.mu.Unlock()
		delivered := b.deliverToSubscribers(msg.Topic, msg.Payload, msg.SeqNum)
		b.mu.Lock()

		if delivered {
			msg.State = StateDelivered
			delete(b.replicaStore, msg.MessageID)
			b.deliveredMsgs[msg.MessageID] = true
		}
	}

	fmt.Printf("Broker: Promotion complete. Remaining replicas: %d\n", len(b.replicaStore))
}

func (b *Broker) StartHealthCheck() {
	if b.mode != BackupMode {
		return
	}

	ticker := time.NewTicker(1 * time.Second)
	consecutiveFailures := 0
	const failureThreshold = 3 // Require 3 consecutive failures

	go func() {
		for range ticker.C {
			alive := b.checkPrimaryHealth()

			if !alive {
				consecutiveFailures++
				fmt.Printf("Broker (Backup): Primary health check failed (%d/%d)\n",
					consecutiveFailures, failureThreshold)
			} else {
				consecutiveFailures = 0
			}

			if consecutiveFailures >= failureThreshold {
				b.mu.Lock()
				b.isPrimaryAlive = false
				b.mu.Unlock()
				fmt.Println("Broker (Backup): Primary confirmed dead! Taking over...")
				b.PromoteToPrimary()
				ticker.Stop()
				return
			}
		}
	}()
}

func (b *Broker) checkPrimaryHealth() bool {
	conn, err := net.DialTimeout("tcp", b.primaryAddr, 500*time.Millisecond)
	if err != nil {
		return false
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(500 * time.Millisecond))

	// Read CONNACK first
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		return false
	}
	connack := strings.TrimSpace(string(buf[:n]))
	if !strings.HasPrefix(connack, "CONNACK") {
		return false
	}

	// Send HEARTBEAT
	_, err = fmt.Fprintf(conn, "HEARTBEAT\n")
	if err != nil {
		return false
	}

	// Read HEARTBEAT_ACK
	n, err = conn.Read(buf)
	if err != nil {
		return false
	}

	response := strings.TrimSpace(string(buf[:n]))
	return response == "HEARTBEAT_ACK"
}

func (b *Broker) StartCleanupRoutine() {
	ticker := time.NewTicker(30 * time.Second)
	go func() {
		for range ticker.C {
			b.mu.Lock()

			// Prevent unbounded growth of deliveredMsgs
			if len(b.deliveredMsgs) > 10000 {
				log.Println("Broker: Cleaning up deduplication map")
				b.deliveredMsgs = make(map[string]bool)
			}

			b.mu.Unlock()
		}
	}()
}

// removeFailedConnections removes connections that failed to receive messages
func (b *Broker) removeFailedConnections(topic string, failedConns []net.Conn) {
	b.mu.Lock()
	defer b.mu.Unlock()

	subscribers := b.subscriptions[topic]
	newSubscribers := []net.Conn{}

	for _, conn := range subscribers {
		failed := slices.Contains(failedConns, conn)
		if !failed {
			newSubscribers = append(newSubscribers, conn)
		}
	}

	b.subscriptions[topic] = newSubscribers
	fmt.Printf("Broker: Removed %d failed connections from topic '%s'\n", len(failedConns), topic)
}

// RemoveConnection removes a connection from all topics
func (b *Broker) RemoveConnection(conn net.Conn) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for topic, subscribers := range b.subscriptions {
		newSubscribers := []net.Conn{}
		for _, sub := range subscribers {
			if sub != conn {
				newSubscribers = append(newSubscribers, sub)
			}
		}
		b.subscriptions[topic] = newSubscribers
	}
	fmt.Printf("Broker: Removed connection %s from all topics\n", conn.RemoteAddr())
}

// GetSubscriberCount returns the number of subscribers for a topic
func (b *Broker) GetSubscriberCount(topic string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subscriptions[topic])
}

// HandlePacket processes a packet from a client
func (b *Broker) HandlePacket(packet *protocol.Packet, conn net.Conn) {
	switch packet.Type {
	case protocol.SUBSCRIBE:
		b.Subscribe(packet.Topic, conn)
		conn.Write([]byte("SUBACK|Subscribed successfully\n"))

	case protocol.PUBLISH:
		// Generate messageID if not provided by client
		messageID := packet.MessageID
		if messageID == "" {
			messageID = fmt.Sprintf("msg-%d-%d", time.Now().UnixNano(), rand.Intn(10000))
		}
		b.Publish(packet.Topic, packet.Payload, messageID)
		conn.Write([]byte("PUBACK|Published successfully\n"))

	case protocol.REPLICATE:
		b.handleReplication(packet.MessageID, packet.Topic, packet.Payload, packet.SeqNum)

	case protocol.MARK_DELIVERED:
		b.markDelivered(packet.MessageID)

	case protocol.HEARTBEAT:
		conn.Write([]byte("HEARTBEAT_ACK\n"))
	}
}

func modeString(mode BrokerMode) string {
	if mode == PrimaryMode {
		return "PRIMARY"
	}
	return "BACKUP"
}
