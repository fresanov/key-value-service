package main

import (
	"testing"
)

func TestLogger(t *testing.T) {
	tests := []struct {
		name       string
		operations []func(logger TransactionLogger)
		expected   []Event
	}{
		{
			name: "Put",
			operations: []func(logger TransactionLogger){
				func(logger TransactionLogger) { logger.WritePut("key1", "value123") },
			},
			expected: []Event{
				{EventType: EventPut, Key: "key1", Value: "value123"},
			},
		},
		{
			name: "Delete",
			operations: []func(logger TransactionLogger){
				func(logger TransactionLogger) { logger.WriteDelete("key1") },
			},
			expected: []Event{
				{EventType: EventDelete, Key: "key1"},
			},
		},
		{
			name: "Put and Delete",
			operations: []func(logger TransactionLogger){
				func(logger TransactionLogger) { logger.WritePut("key1", "value123") },
				func(logger TransactionLogger) { logger.WriteDelete("key1") },
			},
			expected: []Event{
				{EventType: EventPut, Key: "key1", Value: "value123"},
				{EventType: EventDelete, Key: "key1"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, err := NewFileTransactionLogger("")
			if err != nil {
				t.Fatalf("failed to create logger: %v", err)
			}
			wg := logger.Run()

			for _, op := range tt.operations {
				op(logger)
			}
			logger.Shutdown()

			// Wait for Run() to encode the event
			// otherwise, ReadEvents() is too fast and will not receive the event
			wg.Wait()

			fileLogger, ok := logger.(*FileTransactionLogger)
			if !ok {
				t.Fatalf("logger is not a FileTransactionLogger")
			}
			// since we are using the API backwards for the test, calling Run() before ReadEvents()
			// we have to reset the sequence by number of events
			fileLogger.lastSequence -= uint64(len(tt.expected))

			events, errors := logger.ReadEvents()

			for i, expectedEvent := range tt.expected {
				select {
				case err := <-errors:
					if err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
				case event, ok := <-events:
					if !ok {
						t.Fatalf("expected event %d but got none", i)
					}
					if event.Key != expectedEvent.Key || event.Value != expectedEvent.Value || event.EventType != expectedEvent.EventType {
						t.Fatalf("unexpected event: got %+v, want %+v", event, expectedEvent)
					}
				}
			}
		})
	}
}
