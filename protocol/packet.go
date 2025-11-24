package protocol

import (
	"fmt"
	"strconv"
	"strings"
)

// PacketType represents the type of control packet
type PacketType byte

const (
	PUBLISH PacketType = iota
	SUBSCRIBE
	REPLICATE      // Primary -> Backup replication
	MARK_DELIVERED // Primary tells backup message was delivered
	HEARTBEAT      // Backup checks if primary is alive
	HEARTBEAT_ACK  // Primary responds to heartbeat
)

// Packet represents a control packet
type Packet struct {
	Type      PacketType
	Topic     string
	Payload   string
	MessageID string // Unique identifier for each message
	SeqNum    int    // Sequence number for ordering
}

// String returns a string representation of the packet
func (p *Packet) String() string {
	switch p.Type {
	case PUBLISH:
		return fmt.Sprintf("PUBLISH|%s|%s", p.Topic, p.Payload)
	case SUBSCRIBE:
		return fmt.Sprintf("SUBSCRIBE|%s", p.Topic)
	case REPLICATE:
		return fmt.Sprintf("REPLICATE|%s|%d|%s|%s", p.MessageID, p.SeqNum, p.Topic, p.Payload)
	case MARK_DELIVERED:
		return fmt.Sprintf("MARK_DELIVERED|%s|%d", p.MessageID, p.SeqNum)
	case HEARTBEAT:
		return "HEARTBEAT"
	case HEARTBEAT_ACK:
		return "HEARTBEAT_ACK"
	default:
		return "UNKNOWN"
	}
}

// ParsePacket parses a raw message into a Packet
// Format examples:
// - "PUBLISH|topic|payload"
// - "SUBSCRIBE|topic"
// - "REPLICATE|messageID|seqNum|topic|payload"
// - "MARK_DELIVERED|messageID|seqNum"
// - "HEARTBEAT"
// - "HEARTBEAT_ACK"
func ParsePacket(raw string) (*Packet, error) {
	// Split into minimum required parts for the most complex packet
	parts := strings.SplitN(raw, "|", 5)
	if len(parts) < 1 {
		return nil, fmt.Errorf("invalid packet format: empty packet")
	}

	packet := &Packet{}

	// Parse packet type
	switch strings.ToUpper(parts[0]) {
	case "PUBLISH":
		// For PUBLISH, we need to split into exactly 3 parts to preserve pipes in payload
		publishParts := strings.SplitN(raw, "|", 3)
		if len(publishParts) != 3 {
			return nil, fmt.Errorf("PUBLISH packet requires 3 parts: PUBLISH|topic|payload")
		}
		packet.Type = PUBLISH
		packet.Topic = publishParts[1]
		packet.Payload = publishParts[2]

	case "SUBSCRIBE":
		if len(parts) < 2 {
			return nil, fmt.Errorf("SUBSCRIBE packet requires at least 2 parts: SUBSCRIBE|topic")
		}
		packet.Type = SUBSCRIBE
		packet.Topic = parts[1]

	case "REPLICATE":
		if len(parts) != 5 {
			return nil, fmt.Errorf("REPLICATE packet requires 5 parts: REPLICATE|messageID|seqNum|topic|payload")
		}
		packet.Type = REPLICATE
		packet.MessageID = parts[1]
		seqNum, err := strconv.Atoi(parts[2])
		if err != nil {
			return nil, fmt.Errorf("invalid sequence number in REPLICATE: %w", err)
		}
		packet.SeqNum = seqNum
		packet.Topic = parts[3]
		packet.Payload = parts[4]

	case "MARK_DELIVERED":
		if len(parts) != 3 {
			return nil, fmt.Errorf("MARK_DELIVERED packet requires 3 parts: MARK_DELIVERED|messageID|seqNum")
		}
		packet.Type = MARK_DELIVERED
		packet.MessageID = parts[1]
		seqNum, err := strconv.Atoi(parts[2])
		if err != nil {
			return nil, fmt.Errorf("invalid sequence number in MARK_DELIVERED: %w", err)
		}
		packet.SeqNum = seqNum

	case "HEARTBEAT":
		packet.Type = HEARTBEAT

	case "HEARTBEAT_ACK":
		packet.Type = HEARTBEAT_ACK

	default:
		return nil, fmt.Errorf("unknown packet type: %s", parts[0])
	}

	// Validate topic is not empty for packets that require it
	if (packet.Type == PUBLISH || packet.Type == SUBSCRIBE || packet.Type == REPLICATE) && packet.Topic == "" {
		return nil, fmt.Errorf("topic cannot be empty")
	}

	return packet, nil
}
