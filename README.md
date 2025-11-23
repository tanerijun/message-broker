# Message Broker

A simple PUBSUB message broker in Go.

## Protocol

The broker uses a simple text-based protocol:

- **SUBSCRIBE**: `SUBSCRIBE|<topic>`
- **PUBLISH**: `PUBLISH|<topic>|<message>`

## Commands Reference

### Installing from GitHub

```bash
# Install all components
go install github.com/tanerijun/message-broker/cmd/broker@latest
go install github.com/tanerijun/message-broker/cmd/publisher@latest
go install github.com/tanerijun/message-broker/cmd/subscriber@latest

# Then run (binaries are in $GOPATH/bin or ~/go/bin)
broker 8080
subscriber localhost:8080 topicA
publisher localhost:8080 topicA "Hello"
```

### Running directly with go run (for development)

From the **root directory**:

```bash
# Terminal 1: Start broker
go run ./cmd/broker 8080

# Terminal 2: Subscribe to topicA
go run ./cmd/subscriber localhost:8080 topicA

# Terminal 3: Subscribe to topicB
go run ./cmd/subscriber localhost:8080 topicB

# Terminal 4: Publish to topicA
go run ./cmd/publisher localhost:8080 topicA "Hello"

# Terminal 5: Subscribe to topicA (third subscriber)
go run ./cmd/subscriber localhost:8080 topicA

# Terminal 4: Publish to topicA again
go run ./cmd/publisher localhost:8080 topicA "Bye"
```

### Building binaries locally

```bash
# Build all
go build -o bin/broker ./cmd/broker
go build -o bin/publisher ./cmd/publisher
go build -o bin/subscriber ./cmd/subscriber

# Run
./bin/broker 8080
./bin/subscriber localhost:8080 topicA
./bin/publisher localhost:8080 topicA "Hello"
```

### Testing

```bash
go test ./...              # All tests
go test -v ./...           # Verbose
go test -cover ./...       # With coverage
go test -race ./...        # Race detection
```
