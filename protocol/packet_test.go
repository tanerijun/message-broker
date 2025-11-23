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
