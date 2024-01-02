package primodbd

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/rickcollette/primodb/client"
	"github.com/rickcollette/primodb/clientconfig"
)

// PrimoDBConn holds a connection to the database
type PrimoDBConn struct {
    client *client.PrimoDBClient
}

type PrimoDBDriver struct{}

// Implement the driver.Conn interface
func (c *PrimoDBConn) Prepare(query string) (driver.Stmt, error) {
    // Implement query preparation
    // ...
	return nil, nil
}

func (c *PrimoDBConn) Close() error {
    // Implement connection close logic
    // ...
	return nil
}

func (c *PrimoDBConn) Begin() (driver.Tx, error) {
    // Implement if the database supports transactions
    // Return an error if not supported
    return nil, driver.ErrSkip
}
func (d *PrimoDBDriver) Open(dsn string) (driver.Conn, error) {
	parsedURL, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("invalid DSN: %v", err)
	}

	host := parsedURL.Hostname()
	port, err := strconv.Atoi(parsedURL.Port())
	if err != nil {
		return nil, fmt.Errorf("invalid port: %v", err)
	}

	// Create a ClientConfig instance
	clientConfig := &clientconfig.ClientConfig{
		Server: struct {
			Host    string        `yaml:"host"`
			Port    int           `yaml:"port"`
			Timeout time.Duration `yaml:"timeout"`
		}{
			Host:    host,
			Port:    port,
			Timeout: 5 * time.Second, // You can adjust the timeout as needed
		},
		// Set other fields of ClientConfig as needed
	}

	// Initialize PrimoDBClient with the new clientConfig
	primoDBClient, _ := client.NewClient(host, port, "primodb", 5*time.Second, clientConfig)

	return &PrimoDBConn{client: primoDBClient}, nil
}

// Implement other necessary methods such as Prepare, Close, Begin, etc.
func init() {
    sql.Register("primodb", &PrimoDBDriver{})
}
