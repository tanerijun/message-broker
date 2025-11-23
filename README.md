# Message Broker

A simple PUBSUB message broker in Go.

## Protocol

The broker uses a simple text-based protocol:

- **SUBSCRIBE**: `SUBSCRIBE|<topic>`
- **PUBLISH**: `PUBLISH|<topic>|<message>`

## Running the Broker

```bash
cd server
go run main.go server.go 8080
```

## Running a Subscriber

```bash
cd client/subscriber
go run main.go localhost:8080 topicA
```

## Running a Publisher

```bash
cd client/publisher
go run main.go localhost:8080 topicA "Hello World"
```

## Testing

Run all tests:

```bash
go test ./...
```

Run tests with coverage:

```bash
go test -cover ./...
```

## Example Scenario

Terminal 1 (Broker):

```bash
cd server
go run main.go server.go 8080
```

Terminal 2 (Subscriber 1 - topicA):

```bash
cd client/subscriber
go run main.go localhost:8080 topicA
```

Terminal 3 (Subscriber 2 - topicB):

```bash
cd client/subscriber
go run main.go localhost:8080 topicB
```

Terminal 4 (Publisher - topicA):

```bash
cd client/publisher
go run main.go localhost:8080 topicA "Hello"
# Only Subscriber 1 will receive this
```

Terminal 5 (Subscriber 3 - topicA):

```bash
cd client/subscriber
go run main.go localhost:8080 topicA
```

Terminal 6 (Publisher - topicA):

```bash
cd client/publisher
go run main.go localhost:8080 topicA "Bye"
# Both Subscriber 1 and Subscriber 3 will receive this
```
