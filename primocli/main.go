package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/rickcollette/primodb/client"
	"github.com/rickcollette/primodb/clientconfig"
)

const (
	QuitCommand    = ".quit"
	ExitCommand    = ".exit"
	QCommand       = ".q"
	HelpCommand    = ".help"
	VersionCommand = ".version"
)

type commands struct {
	READ    string
	CREATE    string
	UPDATE string
	DELETE string
	DEL    string
	ID     string
}


// CommandEnum enum of supported commands
var (
	dbClient *client.PrimoDBClient
	CommandEnum = commands{"READ", "CREATE", "UPDATE", "DELETE", "DEL", "ID"}
	// ErrKeyNotFound raise when no value found for a given key
	ErrKeyNotFound = errors.New("error: Key not found")
	// ErrInvalidCommand raised when command passed from CLI
	ErrInvalidCommand = errors.New("error: Invalid command")
	// ErrInvalidNoOfArguments raised when argument count more/less than required by the command
	ErrInvalidNoOfArguments = errors.New("error: Invalid number of arguments passed")
	// ErrKeyValueMissing key or value not passed for a command
	ErrKeyValueMissing = errors.New("error: Key or value not passed")
	host               string
	port               int
	dbname             string
	timeout            int
)
var CommandMap map[string]interface{}
// CommandMap map of command enum => command method


func processedCmd(input string) (string, string, string, error) {
	if input == "" {
		return "", "", "", ErrInvalidNoOfArguments
	}

	input = strings.TrimSpace(input)
	fields := strings.Fields(input)

	// Handle special commands first
	if len(fields) == 1 {
		cmd := strings.ToLower(fields[0])
		switch cmd {
		case QuitCommand, VersionCommand, ExitCommand, QCommand, HelpCommand:
			return cmd, "", "", nil
		}
	}

	// For other commands
	if len(fields) < 2 {
		return "", "", "", ErrKeyValueMissing
	}

	cmd := strings.ToUpper(fields[0])
	var key, value string
	var err error

	switch cmd {
	case CommandEnum.READ, CommandEnum.DELETE, CommandEnum.DEL:
		if len(fields) != 2 {
			err = ErrInvalidNoOfArguments
		} else {
			key = fields[1]
		}
	case CommandEnum.CREATE, CommandEnum.UPDATE:
		if len(fields) != 3 {
			err = ErrInvalidNoOfArguments
		} else {
			key, value = fields[1], fields[2]
		}
	default:
		err = ErrInvalidCommand
	}

	return cmd, key, value, err
}

// cli is the main CLI loop
func cli(host string, port int, dbname string, timeout int) {
	var result string
	log.SetFlags(0)
	reader := bufio.NewReader(os.Stdin)

	// Recover if server is down
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("Error raised while connecting to the server, Error(%s)\n", r)
		}
	}()

	for {
		fmt.Printf("primodb > ")
		input, err := reader.ReadString('\n')
		if err != nil {
			log.Fatal(err)
		}

		cmd, key, value, err := processedCmd(input)
		if err != nil {
			log.Println(err)
			continue
		}

		// Handle special commands
		switch cmd {
		case QuitCommand, ExitCommand, QCommand:
			fmt.Println("Exiting PrimoDB CLI.")
			os.Exit(0)
		case VersionCommand:
			if dbClient == nil {
				fmt.Println("Error: Database client is not initialized.")
			} else {
				version, err := dbClient.Version()
				if err != nil {
					fmt.Println("Error getting version:", err)
				} else {
					fmt.Println("PrimoDB Version:", version)
				}
			}
			continue		
		case HelpCommand:
			printHelp()
			continue
		}

		method, ok := CommandMap[cmd]
		if !ok {
			log.Println(ErrInvalidCommand)
			continue
		}

		// Execute the command
		if key == "" && value == "" {
			result, err = method.(func() string)(), nil
		} else if value == "" {
			result, err = method.(func(string) (string, error))(key)
		} else {
			result, err = method.(func(string, string) (string, error))(key, value)
		}

		if err != nil {
			log.Println(err)
			continue
		}
		fmt.Println(result)
	}
}
func printHelp() {
	fmt.Println("PrimoDB Commands:")
	fmt.Println("  READ <key>             - Retrieve the value for the given key.")
	fmt.Println("  CREATE <key> <value>     - Set the value for the given key.")
	fmt.Println("  UPDATE <key> <value>  - Update the value for the given key.")
	fmt.Println("  DELETE <key>          - Delete the value for the given key.")
	fmt.Println("  DEL <key>             - Alias for DELETE.")
	fmt.Println("  ID                    - Retrieve the client ID.")
	fmt.Println("  .version 			 - Display the version of PrimoDB.")
	fmt.Println("  .quit, .exit, .q      - Exit the CLI.")
	fmt.Println("  .help                 - Display this help message.")
}
func main() {
    flag.StringVar(&host, "host", "localhost", "host")
    flag.IntVar(&port, "port", 9969, "port")
    flag.StringVar(&dbname, "dbname", "primodb", "dbname")
    flag.IntVar(&timeout, "timeout", 5, "timeout")
    flag.Parse()

    // Load client configuration from the YAML file
    clientConfig, ok := clientconfig.Config("client").(*clientconfig.ClientConfig)
    if !ok || clientConfig == nil {
        log.Fatalf("Failed to load client configuration")
    }

    fmt.Printf("Loaded client configuration: %+v\n", clientConfig)

    // Initialize dbClient with the loaded configuration
    dbClient, err := client.NewClient(host, port, dbname, time.Duration(timeout)*time.Second, clientConfig)
    if err != nil {
        fmt.Println("Error initializing dbClient:", err)
        return
    }

    fmt.Println("Debug - dbClient initialized successfully")

    // Initialize CommandMap
    CommandMap = map[string]interface{}{
        CommandEnum.READ:    dbClient.Read,
        CommandEnum.CREATE:    dbClient.Create,
		CommandEnum.UPDATE: dbClient.Update,
        CommandEnum.DELETE: dbClient.Delete,
        CommandEnum.DEL:    dbClient.Delete,
        CommandEnum.ID:     dbClient.GetID,
    }

    cli(host, port, dbname, timeout)
}
