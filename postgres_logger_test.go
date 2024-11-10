package main

import (
	"context"
	"log"
	"os"
	"testing"

	_ "github.com/lib/pq"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

var (
	pgContainer testcontainers.Container
	params      PostgresDBParams
)

func TestPostgresTransactionLogger(t *testing.T) {
	logger, err := NewPostgresTransactionLogger(params)
	if err != nil {
		t.Fatalf("failed to create logger: %v", err)
	}
	defer logger.Shutdown()

	t.Run("TestWritePut", func(t *testing.T) {
		wg := logger.Run()
		logger.WritePut("key1", "value1")

		// Wait for Run() to encode the event
		// otherwise, ReadEvents() is too fast and will not receive the event
		wg.Wait()

		events, errors := logger.ReadEvents()

		select {
		case err := <-errors:
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		case event := <-events:
			if event.Key != "key1" || event.Value != "value1" || event.EventType != EventPut {
				t.Fatalf("unexpected event: %+v", event)
			}
		}
	})

	t.Run("TestWriteDelete", func(t *testing.T) {
		wg := logger.Run()
		logger.WriteDelete("key2")

		// Wait for Run() to encode the event
		// otherwise, ReadEvents() is too fast and will not receive the event
		wg.Wait()

		events, errors := logger.ReadEvents()

		select {
		case err := <-errors:
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		case event := <-events:
			if event.Key == "key2" && event.EventType != EventDelete {
				t.Fatalf("unexpected event: %+v", event)
			}
		}
	})
}

func TestMain(m *testing.M) {
	ctx := context.Background()

	// Set up the PostgreSQL container
	var err error
	pgContainer, err := postgres.Run(ctx,
		"postgres:latest",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("testpassword"),
	)
	if err != nil {
		log.Fatalf("failed to start PostgreSQL container: %v", err)
	}

	host, err := pgContainer.Host(ctx)
	if err != nil {
		log.Fatalf("failed to get container host: %v", err)
	}

	port, err := pgContainer.MappedPort(ctx, "5432")
	if err != nil {
		log.Fatalf("failed to get container port %v: %v", port, err)
	}

	params = PostgresDBParams{
		host:     host,
		port:     port.Port(),
		dbName:   "testdb",
		user:     "postgres",
		password: "testpassword",
		sslmode:  "disable",
	}

	// Run the tests
	code := m.Run()

	// Clean up the PostgreSQL container
	if err := pgContainer.Terminate(ctx); err != nil {
		log.Fatalf("failed to terminate PostgreSQL container: %v", err)
	}

	os.Exit(code)
}
