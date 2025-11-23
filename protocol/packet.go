package protocol

import (
	"fmt"
	"strings"
)

// PacketType represents the type of control packet
type PacketType byte

const (
	PUBLISH PacketType = iota
	SUBSCRIBE
)

// Packet represents a control packet
type Packet struct {
	Type    PacketType
	Topic   string
	Payload string
}

// String returns a string representation of the packet
func (p *Packet) String() string {
	if p.Type == PUBLISH {
		return fmt.Sprintf("PUBLISH|%s|%s", p.Topic, p.Payload)
	}
	return fmt.Sprintf("SUBSCRIBE|%s", p.Topic)
}

// ParsePacket parses a raw message into a Packet
// Format: "PUBLISH|topic|payload" or "SUBSCRIBE|topic"
func ParsePacket(raw string) (*Packet, error) {
	parts := strings.SplitN(raw, "|", 3)
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid packet format: expected at least 2 parts")
	}

	packet := &Packet{}

	// Parse packet type
	switch strings.ToUpper(parts[0]) {
	case "PUBLISH":
		packet.Type = PUBLISH
		if len(parts) != 3 {
			return nil, fmt.Errorf("PUBLISH packet requires 3 parts: PUBLISH|topic|payload")
		}
		packet.Payload = parts[2]
	case "SUBSCRIBE":
		packet.Type = SUBSCRIBE
		if len(parts) < 2 {
			return nil, fmt.Errorf("SUBSCRIBE packet requires at least 2 parts: SUBSCRIBE|topic")
		}
	default:
		return nil, fmt.Errorf("unknown packet type: %s", parts[0])
	}

	packet.Topic = parts[1]
	if packet.Topic == "" {
		return nil, fmt.Errorf("topic cannot be empty")
	}

	return packet, nil
}
