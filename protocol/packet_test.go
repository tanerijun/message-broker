package protocol

import "testing"

func TestParsePacket(t *testing.T) {
	tests := []struct {
		name        string
		raw         string
		wantType    PacketType
		wantTopic   string
		wantPayload string
		wantErr     bool
	}{
		{
			name:        "Valid PUBLISH packet",
			raw:         "PUBLISH|topicA|Hello World",
			wantType:    PUBLISH,
			wantTopic:   "topicA",
			wantPayload: "Hello World",
			wantErr:     false,
		},
		{
			name:        "Valid SUBSCRIBE packet",
			raw:         "SUBSCRIBE|topicB",
			wantType:    SUBSCRIBE,
			wantTopic:   "topicB",
			wantPayload: "",
			wantErr:     false,
		},
		{
			name:        "PUBLISH with pipe in payload",
			raw:         "PUBLISH|topic|message|with|pipes",
			wantType:    PUBLISH,
			wantTopic:   "topic",
			wantPayload: "message|with|pipes",
			wantErr:     false,
		},
		{
			name:    "Invalid format - too few parts",
			raw:     "PUBLISH",
			wantErr: true,
		},
		{
			name:    "Invalid PUBLISH - no payload",
			raw:     "PUBLISH|topic",
			wantErr: true,
		},
		{
			name:    "Unknown packet type",
			raw:     "UNKNOWN|topic|payload",
			wantErr: true,
		},
		{
			name:    "Empty topic",
			raw:     "SUBSCRIBE|",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			packet, err := ParsePacket(tt.raw)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParsePacket() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if packet.Type != tt.wantType {
					t.Errorf("Type = %v, want %v", packet.Type, tt.wantType)
				}
				if packet.Topic != tt.wantTopic {
					t.Errorf("Topic = %v, want %v", packet.Topic, tt.wantTopic)
				}
				if packet.Payload != tt.wantPayload {
					t.Errorf("Payload = %v, want %v", packet.Payload, tt.wantPayload)
				}
			}
		})
	}
}

func TestPacketString(t *testing.T) {
	tests := []struct {
		name   string
		packet Packet
		want   string
	}{
		{
			name: "PUBLISH packet",
			packet: Packet{
				Type:    PUBLISH,
				Topic:   "test",
				Payload: "hello",
			},
			want: "PUBLISH|test|hello",
		},
		{
			name: "SUBSCRIBE packet",
			packet: Packet{
				Type:  SUBSCRIBE,
				Topic: "news",
			},
			want: "SUBSCRIBE|news",
		},
		{
			name: "REPLICATE packet",
			packet: Packet{
				Type:      REPLICATE,
				MessageID: "msg-123",
				SeqNum:    42,
				Topic:     "events",
				Payload:   "data",
			},
			want: "REPLICATE|msg-123|42|events|data",
		},
		{
			name: "MARK_DELIVERED packet",
			packet: Packet{
				Type:      MARK_DELIVERED,
				MessageID: "msg-456",
				SeqNum:    100,
			},
			want: "MARK_DELIVERED|msg-456|100",
		},
		{
			name: "HEARTBEAT packet",
			packet: Packet{
				Type: HEARTBEAT,
			},
			want: "HEARTBEAT",
		},
		{
			name: "HEARTBEAT_ACK packet",
			packet: Packet{
				Type: HEARTBEAT_ACK,
			},
			want: "HEARTBEAT_ACK",
		},
		{
			name: "PUBLISH with pipes in payload",
			packet: Packet{
				Type:    PUBLISH,
				Topic:   "topic",
				Payload: "data|with|pipes",
			},
			want: "PUBLISH|topic|data|with|pipes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.packet.String()
			if got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParsePacketReplication(t *testing.T) {
	tests := []struct {
		name        string
		raw         string
		wantType    PacketType
		wantMsgID   string
		wantSeqNum  int
		wantTopic   string
		wantPayload string
		wantErr     bool
	}{
		{
			name:        "Valid REPLICATE packet",
			raw:         "REPLICATE|msg-123|42|topicA|payload",
			wantType:    REPLICATE,
			wantMsgID:   "msg-123",
			wantSeqNum:  42,
			wantTopic:   "topicA",
			wantPayload: "payload",
			wantErr:     false,
		},
		{
			name:    "REPLICATE with too few parts",
			raw:     "REPLICATE|msg-123|42|topicA",
			wantErr: true,
		},
		{
			name:    "REPLICATE with invalid seqNum",
			raw:     "REPLICATE|msg-123|invalid|topicA|payload",
			wantErr: true,
		},
		{
			name:        "REPLICATE with empty topic",
			raw:         "REPLICATE|msg-123|42||payload",
			wantType:    REPLICATE,
			wantMsgID:   "msg-123",
			wantSeqNum:  42,
			wantTopic:   "",
			wantPayload: "payload",
			wantErr:     true,
		},
		{
			name:       "Valid MARK_DELIVERED packet",
			raw:        "MARK_DELIVERED|msg-456|100",
			wantType:   MARK_DELIVERED,
			wantMsgID:  "msg-456",
			wantSeqNum: 100,
			wantErr:    false,
		},
		{
			name:    "MARK_DELIVERED with too few parts",
			raw:     "MARK_DELIVERED|msg-456",
			wantErr: true,
		},
		{
			name:    "MARK_DELIVERED with invalid seqNum",
			raw:     "MARK_DELIVERED|msg-456|notanumber",
			wantErr: true,
		},
		{
			name:     "Valid HEARTBEAT packet",
			raw:      "HEARTBEAT",
			wantType: HEARTBEAT,
			wantErr:  false,
		},
		{
			name:     "Valid HEARTBEAT_ACK packet",
			raw:      "HEARTBEAT_ACK",
			wantType: HEARTBEAT_ACK,
			wantErr:  false,
		},
		{
			name:     "HEARTBEAT with lowercase",
			raw:      "heartbeat",
			wantType: HEARTBEAT,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			packet, err := ParsePacket(tt.raw)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParsePacket() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if packet.Type != tt.wantType {
					t.Errorf("Type = %v, want %v", packet.Type, tt.wantType)
				}
				if tt.wantMsgID != "" && packet.MessageID != tt.wantMsgID {
					t.Errorf("MessageID = %v, want %v", packet.MessageID, tt.wantMsgID)
				}
				if tt.wantSeqNum != 0 && packet.SeqNum != tt.wantSeqNum {
					t.Errorf("SeqNum = %v, want %v", packet.SeqNum, tt.wantSeqNum)
				}
				if tt.wantTopic != "" && packet.Topic != tt.wantTopic {
					t.Errorf("Topic = %v, want %v", packet.Topic, tt.wantTopic)
				}
				if tt.wantPayload != "" && packet.Payload != tt.wantPayload {
					t.Errorf("Payload = %v, want %v", packet.Payload, tt.wantPayload)
				}
			}
		})
	}
}

func TestParsePacketRoundTrip(t *testing.T) {
	tests := []Packet{
		{Type: PUBLISH, Topic: "test", Payload: "hello"},
		{Type: SUBSCRIBE, Topic: "news"},
		{Type: REPLICATE, MessageID: "msg-1", SeqNum: 10, Topic: "events", Payload: "data"},
		{Type: MARK_DELIVERED, MessageID: "msg-2", SeqNum: 20},
		{Type: HEARTBEAT},
		{Type: HEARTBEAT_ACK},
	}

	for _, original := range tests {
		t.Run(original.String(), func(t *testing.T) {
			str := original.String()
			parsed, err := ParsePacket(str)
			if err != nil {
				t.Fatalf("ParsePacket() error = %v", err)
			}

			if parsed.Type != original.Type {
				t.Errorf("Type mismatch: got %v, want %v", parsed.Type, original.Type)
			}
			if parsed.Topic != original.Topic {
				t.Errorf("Topic mismatch: got %v, want %v", parsed.Topic, original.Topic)
			}
			if parsed.Payload != original.Payload {
				t.Errorf("Payload mismatch: got %v, want %v", parsed.Payload, original.Payload)
			}
			if parsed.MessageID != original.MessageID {
				t.Errorf("MessageID mismatch: got %v, want %v", parsed.MessageID, original.MessageID)
			}
			if parsed.SeqNum != original.SeqNum {
				t.Errorf("SeqNum mismatch: got %v, want %v", parsed.SeqNum, original.SeqNum)
			}
		})
	}
}
