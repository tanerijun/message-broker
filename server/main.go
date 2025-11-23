package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Usage: go run main.go <port>")
		fmt.Println("Example: go run main.go 8080")
		os.Exit(1)
	}

	port := os.Args[1]
	addr := "0.0.0.0:" + port // bind to all interfaces

	if err := StartServer(addr); err != nil {
		fmt.Println("Server error:", err)
		os.Exit(1)
	}
}
