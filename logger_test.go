package main

import (
	"testing"
)

func TestFileTransactionLoggerWithBuffer(t *testing.T) {
	logger, err := NewFileTransactionLogger("")
	if err != nil {
		t.Fatalf("failed to create logger: %v", err)
	}
	wg := logger.Run()

	logger.WritePut("key1", "value123")
	logger.Shutdown()

	// Wait for Run() to encode the event
	// otherwise, ReadEvents() is too fast and will not receive the event
	wg.Wait()

	events, errors := logger.ReadEvents()

	for {
		select {
		case err := <-errors:
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		case event, ok := <-events:
			if !ok {
				return
			}
			if event.Key != "key1" || event.Value != "value123" || event.EventType != EventPut {
				t.Fatalf("unexpected event: %+v", event)
			}
		}
	}
}

func TestFileTransactionLoggerDeleteWithBuffer(t *testing.T) {
	logger, err := NewFileTransactionLogger("")
	if err != nil {
		t.Fatalf("failed to create logger: %v", err)
	}
	logger.Run()

	logger.WriteDelete("key1")
	logger.Shutdown()

	events, errors := logger.ReadEvents()

	for {
		select {
		case err := <-errors:
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		case event, ok := <-events:
			if !ok {
				return
			}
			if event.Key != "key1" || event.EventType != EventDelete {
				t.Fatalf("unexpected event: %+v", event)
			}
		}
	}
}
