package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"

	"time"
)

func main() {
	if len(os.Args) != 5 {
		fmt.Println("Usage: publisher <primary_addr> <backup_addr> <topic> <duration_sec>")
		fmt.Println("Example: publisher localhost:8080 localhost:8081 topicC 30")
		os.Exit(1)
	}

	primaryAddr := os.Args[1]
	backupAddr := os.Args[2]
	topic := os.Args[3]
	duration, err := strconv.Atoi(os.Args[4])
	if err != nil {
		fmt.Println("Invalid duration:", err)
		os.Exit(1)
	}

	currentBroker := primaryAddr
	seqNum := 1
	failedOver := false
	consecutiveFailures := 0
	const failureThreshold = 3 // Fail over after 3 consecutive failures

	ticker := time.NewTicker(100 * time.Millisecond) // 10 Hz
	defer ticker.Stop()

	timeout := time.After(time.Duration(duration) * time.Second)

	fmt.Printf("Publisher: Starting to publish to %s for %d seconds\n", currentBroker, duration)

	for {
		select {
		case <-timeout:
			fmt.Println("Publisher: Duration complete, exiting")
			return

		case <-ticker.C:
			message := strconv.Itoa(seqNum)
			msgID := fmt.Sprintf("msg-%d-%d", time.Now().UnixNano(), seqNum)

			success := publishWithTimeout(currentBroker, topic, message, msgID, 200*time.Millisecond)

			if !success {
				consecutiveFailures++
				fmt.Printf("Publisher: Failed to send seq=%d (failures: %d/%d)\n", seqNum, consecutiveFailures, failureThreshold)

				if consecutiveFailures >= failureThreshold && !failedOver {
					fmt.Println("Publisher: Multiple failures detected! Failing over to backup...")
					failedOver = true
					currentBroker = backupAddr
					consecutiveFailures = 0

					// Don't resend buffer - backup will deliver from its replicas
					// Just retry current message after failover
					success = publishWithTimeout(currentBroker, topic, message, msgID, 200*time.Millisecond)
					if success {
						consecutiveFailures = 0
					}
				} else {
					// Don't advance seqNum - retry same message next tick
					continue
				}
			} else {
				consecutiveFailures = 0 // Reset on success
			}

			if success {
				fmt.Printf("Publisher: Sent seq=%d to %s\n", seqNum, currentBroker)
				seqNum++
			}
		}
	}
}

func publishWithTimeout(brokerAddr, topic, message, messageID string, timeout time.Duration) bool {
	conn, err := net.DialTimeout("tcp", brokerAddr, timeout)
	if err != nil {
		return false
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(timeout))
	scanner := bufio.NewScanner(conn)

	// Read CONNACK
	if !scanner.Scan() {
		return false
	}

	// Send PUBLISH with messageID
	packet := fmt.Sprintf("PUBLISH|%s|%s\n", topic, message)
	if _, err := conn.Write([]byte(packet)); err != nil {
		return false
	}

	// Wait for PUBACK
	if !scanner.Scan() {
		return false
	}

	return true
}
