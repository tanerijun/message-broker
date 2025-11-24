# Message Broker

A prototype of fault-tolerant PUBSUB message broker in Go with primary-backup replication. (based on the 'main' branch)

## How It Works

## Architecture

### Components

1. **Primary Broker**:
   - Receives messages from publishers
   - Replicates to backup before processing
   - Performs edge computing (50-150ms busy loop)
   - Delivers to subscribers
   - Notifies backup when delivery is complete

2. **Backup Broker**:
   - Stores replicated messages
   - Polls primary health every 1 second
   - Automatically promotes to primary on failure
   - Processes stored messages upon promotion

3. **Publisher**:
   - Publishes at T interval
   - Tracks last N messages
   - Detects primary failure via polling
   - Automatically fails over to backup
   - Resends recent messages on failover

4. **Subscriber**:
   - Connects to both primary and backup
   - Receives messages from active broker
   - Automatic transition during failover

### Message Flow

```
Normal Operation:
Publisher -> Primary -> [1. Replicate] -> Backup (stores)
                     -> [2. Edge Compute 50-150ms]
                     -> [3. Deliver] -> Subscribers
                     -> [4. Mark Delivered] -> Backup (prunes)

Failover:
Primary Dies -> Backup Detects (3 health checks)
             -> Backup Promotes to Primary
             -> Processes Stored Messages
             -> Publisher Switches to Backup
             -> Resends Last 5 Messages
```

### Protocol

The broker uses a simple text-based protocol with pipe-separated fields:

#### Client Packets

- **SUBSCRIBE**: `SUBSCRIBE|<topic>`
- **PUBLISH**: `PUBLISH|<topic>|<message>`

#### Replication Packets (Internal)

- **REPLICATE**: `REPLICATE|<messageID>|<seqNum>|<topic>|<payload>`
- **MARK_DELIVERED**: `MARK_DELIVERED|<messageID>|<seqNum>`
- **HEARTBEAT**: `HEARTBEAT`
- **HEARTBEAT_ACK**: `HEARTBEAT_ACK`

#### Response Packets

- **CONNACK**: `CONNACK|Connected to Message Broker`
- **SUBACK**: `SUBACK|Subscribed successfully`
- **PUBACK**: `PUBACK|Published successfully`
- **MESSAGE**: `MESSAGE|<topic>|<payload>`

### Design Decisions

#### Replication Strategy

FCFS (paper A: FRAME). This "replicate-first" approach minimizes the risk of message loss. The trade-off is slightly higher latency (~5ms), but is acceptable (in this case) given the 50-150ms edge computing delay.

#### Message Pruning

The backup only deletes messages after receiving explicit confirmation from the primary. (replicate → deliver → acknowledge)

### Implementation Details

#### Primary Broker Flow

```
1. Receive PUBLISH from client
2. Replicate to backup (REPLICATE packet)
3. Send PUBACK to client
4. Perform edge computing (50-150ms busy loop)
5. Deliver to all subscribers
6. Send MARK_DELIVERED to backup
```

#### Backup Broker Flow

```
Normal Operation:
1. Receive REPLICATE from primary
2. Store in replica map with state=REPLICATED
3. Receive MARK_DELIVERED from primary
4. Mark state=DELIVERED and delete from map

Failover:
1. Detect primary failure (3 consecutive timeouts)
2. Promote self to primary mode
3. Sort stored replicas by sequence number
4. Process each replica (edge compute + deliver)
5. Continue as new primary
```

#### Publisher Failover Logic

```
1. Send PUBLISH with timeout
2. If timeout:
   a. Switch target to backup broker
   b. Resend last N buffered messages
   c. Continue with new messages to backup
```

#### Subscriber Design

The subscriber maintains connections to both brokers SIMULTANEOUSLY. This approach eliminates connection establishment overhead during failover. Messages are labeled by source (PRIMARY/BACKUP) for logging purposes.

## Commands Reference

### Building Binaries

```bash
# Build all components
go build -o bin/broker ./cmd/broker
go build -o bin/publisher ./cmd/publisher
go build -o bin/subscriber ./cmd/subscriber
```

### Simple Mode (Single Broker)

For basic testing without fault tolerance:

```bash
# Terminal 1: Start broker (simple mode - no replication)
go run ./cmd/broker 8080 primary ""

# Terminal 2: Subscribe
go run ./cmd/subscriber localhost:8080 "" topicA

# Terminal 3: Publish
echo "Note: Simple publisher not implemented. Use fault-tolerant mode."
```

### Fault-Tolerant Mode

From the **root directory**:

```bash
# Terminal 1: Start primary broker
go run ./cmd/broker 8080 primary localhost:8081

# Terminal 2: Start backup broker
go run ./cmd/broker 8081 backup localhost:8080

# Terminal 3: Start subscriber (connects to both brokers)
go run ./cmd/subscriber localhost:8080 localhost:8081 topicC

# Terminal 4: Start publisher (publishes for 30 seconds)
go run ./cmd/publisher localhost:8080 localhost:8081 topicC 30

# Terminal 1: Kill primary (press Ctrl+C after ~15 seconds)
# Watch the backup automatically take over!
```

## Testing

```bash
go test ./...              # All tests
go test -v ./...           # Verbose
go test -cover ./...       # With coverage
go test -race ./...        # Race detection
```
