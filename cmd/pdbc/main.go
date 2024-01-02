package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/rickcollette/primodb/client"
)

func main() {
	// Initialize the PrimoDB client
	primoClient := client.NewClient()

	// Create a Scanner to read input from the command line
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Println("Connected to PrimoDB")

	for {
		fmt.Print("pdbc > ") // Command prompt
		scanner.Scan()
		input := scanner.Text()

		// Split the input into command and arguments
		parts := strings.Fields(input)
		if len(parts) == 0 {
			continue
		}

		command := parts[0]
		switch command {
		case "exit", "quit":
			fmt.Println("Exiting PrimoDB Client.")
			return
		case "get":
			if len(parts) != 2 {
				fmt.Println("Usage: get <key>")
				continue
			}
			key := parts[1]
			value, err := primoClient.Get(key)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				continue
			}
			fmt.Printf("Value: %s\n", value)
		case "set":
			if len(parts) != 3 {
				fmt.Println("Usage: set <key> <value>")
				continue
			}
			key, value := parts[1], parts[2]
			response, err := primoClient.Set(key, value)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				continue
			}
			fmt.Println(response)
		case "del":
			if len(parts) != 2 {
				fmt.Println("Usage: del <key>")
				continue
			}
			key := parts[1]
			response, err := primoClient.Del(key)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				continue
			}
			fmt.Println(response)
		default:
			fmt.Println("Unknown command:", command)
		}
	}
}
