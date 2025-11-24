package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

func main() {
	if len(os.Args) != 4 {
		fmt.Println("Usage: subscriber <primary_addr> <backup_addr> <topic>")
		fmt.Println("Example: subscriber localhost:8080 localhost:8081 topicC")
		os.Exit(1)
	}

	primaryAddr := os.Args[1]
	backupAddr := os.Args[2]
	topic := os.Args[3]

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	fmt.Printf("Subscriber: Starting, will connect to both PRIMARY (%s) and BACKUP (%s)\n", primaryAddr, backupAddr)

	// Connect to both brokers
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		subscribeToBroker("PRIMARY", primaryAddr, topic)
	}()

	go func() {
		defer wg.Done()
		subscribeToBroker("BACKUP", backupAddr, topic)
	}()

	<-sigChan
	fmt.Println("\nSubscriber: Exiting")
}

func subscribeToBroker(name, addr, topic string) {
	for {
		fmt.Printf("Subscriber: Connecting to %s at %s...\n", name, addr)

		conn, err := net.Dial("tcp", addr)
		if err != nil {
			fmt.Printf("Subscriber: Failed to connect to %s: %v\n", name, err)
			time.Sleep(2 * time.Second)
			continue
		}

		scanner := bufio.NewScanner(conn)

		// Read CONNACK
		if !scanner.Scan() {
			fmt.Printf("Subscriber: Failed to read CONNACK from %s\n", name)
			conn.Close()
			time.Sleep(2 * time.Second)
			continue
		}
		fmt.Printf("Subscriber: %s said: %s\n", name, scanner.Text())

		// Send SUBSCRIBE
		packet := fmt.Sprintf("SUBSCRIBE|%s\n", topic)
		if _, err := conn.Write([]byte(packet)); err != nil {
			fmt.Printf("Subscriber: Failed to send SUBSCRIBE to %s: %v\n", name, err)
			conn.Close()
			time.Sleep(2 * time.Second)
			continue
		}

		// Read SUBACK
		if !scanner.Scan() {
			fmt.Printf("Subscriber: Failed to read SUBACK from %s\n", name)
			conn.Close()
			time.Sleep(2 * time.Second)
			continue
		}
		fmt.Printf("Subscriber: %s said: %s\n", name, scanner.Text())

		fmt.Printf("Subscriber: Connected to %s, waiting for messages...\n", name)

		// Receive messages
		for scanner.Scan() {
			message := strings.TrimSpace(scanner.Text())
			if message != "" {
				fmt.Printf("[%s] %s\n", name, message)
			}
		}

		if err := scanner.Err(); err != nil {
			fmt.Printf("Subscriber: Connection error with %s: %v\n", name, err)
		}

		fmt.Printf("Subscriber: Disconnected from %s, reconnecting...\n", name)
		conn.Close()
		time.Sleep(2 * time.Second)
	}
}
