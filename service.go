package main

import (
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/joho/godotenv"
)

type EventType byte

const (
	_                     = iota
	EventDelete EventType = iota
	EventPut
)

type Event struct {
	Sequence  uint64    `json:"sequence"`
	EventType EventType `json:"event_type"`
	Key       string    `json:"key"`
	Value     string    `json:"value"`
}

type TransactionLogger interface {
	WriteDelete(key string)
	WritePut(key, value string)
	Err() <-chan error
	ReadEvents() (<-chan Event, <-chan error)
	Run() *sync.WaitGroup
	Shutdown()
}

var logger TransactionLogger

type LogType int

const (
	FileLog LogType = iota
	DatabaseLog
)

func initializeTransactionLog(logType LogType) error {
	var err error

	// Load environment variables from .env file
	err = godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file")
	}

	switch logType {
	case FileLog:
		logger, err = NewFileTransactionLogger("transaction.log")
	case DatabaseLog:
		logger, err =
			NewPostgresTransactionLogger(PostgresDBParams{
				host:     os.Getenv("POSTGRES_HOST"),
				port:     os.Getenv("POSTGRES_PORT"),
				dbName:   os.Getenv("POSTGRES_DB"),
				user:     os.Getenv("POSTGRES_USER"),
				password: os.Getenv("POSTGRES_PASSWORD"),
				sslmode:  "disable",
			})

	}
	if err != nil {
		return fmt.Errorf("failed to create event logger: %w", err)
	}

	events, errors := logger.ReadEvents()

	e := Event{}
	ok := true

	for ok && err == nil {
		select {
		case err, ok = <-errors:
		case e, ok = <-events:
			switch e.EventType {
			case EventDelete:
				err = Delete(e.Key)
			case EventPut:
				err = Put(e.Key, e.Value)
			}
		}
	}

	logger.Run()

	return err
}

func gracefulShutdown() {
	logger.Shutdown()
}
