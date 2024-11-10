package main

import (
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/lib/pq"
	_ "github.com/lib/pq"
)

type PostgresDBParams struct {
	dbName   string
	host     string
	port     string
	user     string
	password string
	sslmode  string
}

type PostgresTransactionLogger struct {
	events chan<- Event // Write-only channel for sending events
	errors <-chan error // Read-only channel for receiving errors
	db     *sql.DB      // The database access interface
}

func NewPostgresTransactionLogger(config PostgresDBParams) (TransactionLogger, error) {
	connStr := fmt.Sprintf("host=%s port = %s dbname=%s user=%s password=%s sslmode=%s",
		config.host, config.port, config.dbName, config.user, config.password, config.sslmode)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open db: %w", err)
	}

	// Retry mechanism to wait for the database to be ready
	for i := 0; i < 10; i++ {
		log.Println("pinging database")
		err = db.Ping()
		if err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to open db connection: %w", err)
	}

	logger := &PostgresTransactionLogger{db: db}
	exists, err := logger.verifyTableExists()
	if err != nil {
		return nil, fmt.Errorf("failed to verify table exists: %w", err)
	}
	if !exists {
		if err = logger.createTable(); err != nil {
			return nil, fmt.Errorf("failed to create table: %w", err)
		}
	}
	return logger, nil
}

func (l *PostgresTransactionLogger) WritePut(key, value string) {
	l.events <- Event{EventType: EventPut, Key: key, Value: value}
}

func (l *PostgresTransactionLogger) WriteDelete(key string) {
	l.events <- Event{EventType: EventDelete, Key: key}
}

func (l *PostgresTransactionLogger) Err() <-chan error {
	return l.errors
}

func (l *PostgresTransactionLogger) Run() *sync.WaitGroup {
	events := make(chan Event, 16)
	l.events = events
	errors := make(chan error, 1)
	l.errors = errors

	var once sync.Once
	var wg sync.WaitGroup

	go func() {
		query := `INSERT INTO transactions
							(event_type, key, value)
							VALUES ($1, $2, $3)`

		for e := range events { // Retrieve the next Event
			_, err := l.db.Exec( // Execute the INSERT query
				query,
				e.EventType, e.Key, e.Value)

			if err != nil {
				errors <- err
			}

			once.Do(func() {
				// At least one event has been written, let the caller know
				wg.Done()
			})
		}
	}()

	wg.Add(1)
	return &wg
}

func (l *PostgresTransactionLogger) ReadEvents() (<-chan Event, <-chan error) {
	outEvent := make(chan Event)    // An unbuffered events channel
	outError := make(chan error, 1) // A buffered errors channel

	go func() {
		defer close(outEvent) // Close the channels when the
		defer close(outError) // goroutine ends

		query := `SELECT sequence, event_type, key, value
							FROM transactions
							ORDER BY sequence`

		rows, err := l.db.Query(query) // Run query; get result set
		if err != nil {
			outError <- fmt.Errorf("sql query error: %w", err)
			return
		}
		defer rows.Close()

		e := Event{}

		for rows.Next() {
			err = rows.Scan(
				&e.Sequence, &e.EventType,
				&e.Key, &e.Value)

			if err != nil {
				outError <- fmt.Errorf("error reading row: %w", err)
				return
			}
			outEvent <- e // Send e to the channel
		}

		err = rows.Err()
		if err != nil {
			outError <- fmt.Errorf("transaction log read failure: %w", err)
		}
	}()

	return outEvent, outError
}

func (l *PostgresTransactionLogger) Shutdown() {
	l.db.Close()
}

func (l *PostgresTransactionLogger) verifyTableExists() (bool, error) {
	_, err := l.db.Query("SELECT 1 FROM transactions LIMIT 1")
	if err != nil {
		if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == "42P01" {
			// Table does not exist
			return false, nil
		}
		// Some other error
		return false, err
	}
	return true, nil
}

func (l *PostgresTransactionLogger) createTable() error {
	_, err := l.db.Exec(`CREATE TABLE transactions (
		sequence BIGSERIAL PRIMARY KEY,
		event_type SMALLINT NOT NULL,
		key TEXT NOT NULL,
		value TEXT, 
		create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	)`)
	return err
}
