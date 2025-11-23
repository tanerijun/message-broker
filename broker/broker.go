package broker

import (
	"fmt"
	"net"
	"slices"
	"sync"

	"github.com/tanerijun/message-broker/protocol"
)

type Broker struct {
	mu            sync.RWMutex
	subscriptions map[string][]net.Conn // topic -> list of subscriber connections
}

func NewBroker() *Broker {
	return &Broker{
		subscriptions: make(map[string][]net.Conn),
	}
}

func (b *Broker) Subscribe(topic string, conn net.Conn) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.subscriptions[topic] = append(b.subscriptions[topic], conn)
	fmt.Printf("Broker: Client %s subscribed to topic '%s' (%d total subscribers)\n", conn.RemoteAddr(), topic, len(b.subscriptions[topic]))
}

func (b *Broker) Publish(topic string, payload string, publisherConn net.Conn) {
	b.mu.RLock()
	subscribers := b.subscriptions[topic]
	b.mu.RUnlock()

	fmt.Printf("Broker: Publishing to topic '%s' (%d subscribers)\n", topic, len(subscribers))

	// Send message to all subscribers
	failedConns := []net.Conn{}
	for _, conn := range subscribers {
		message := fmt.Sprintf("MESSAGE|%s|%s\n", topic, payload)
		_, err := conn.Write([]byte(message))
		if err != nil {
			fmt.Printf("Broker: Failed to send to %s: %v\n", conn.RemoteAddr(), err)
			failedConns = append(failedConns, conn)
		}
	}

	// Clean up failed connections
	if len(failedConns) > 0 {
		b.removeFailedConnections(topic, failedConns)
	}
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
		b.Publish(packet.Topic, packet.Payload, conn)
		conn.Write([]byte("PUBACK|Published successfully"))
	}
}
