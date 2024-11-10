package main

import (
	"fmt"
	"sync"
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

func initializeTransactionLog(logType string) error {
	var err error

	switch logType {
	case "file":
		logger, err = NewFileTransactionLogger("transaction.log")
	case "databse":
		// TODO
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
