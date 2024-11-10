package main

import (
	"testing"
)

func TestLoggerPut(t *testing.T) {
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

	fileLogger, ok := logger.(*FileTransactionLogger)
	if !ok {
		t.Fatalf("logger is not a FileTransactionLogger")
	}
	// since we are using the API backwards for the test, calling Run() before ReadEvents()
	// we have to reset the sequence by number of events
	fileLogger.lastSequence--

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

func TestLoggerDelete(t *testing.T) {
	logger, err := NewFileTransactionLogger("")
	if err != nil {
		t.Fatalf("failed to create logger: %v", err)
	}
	wg := logger.Run()

	logger.WriteDelete("key1")
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
	fileLogger.lastSequence--

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

func TestLoggerPuttingAndDeleting(t *testing.T) {
	logger, err := NewFileTransactionLogger("")
	if err != nil {
		t.Fatalf("failed to create logger: %v", err)
	}
	wg := logger.Run()

	logger.WritePut("key1", "value123")
	logger.WriteDelete("key1")
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
	fileLogger.lastSequence = fileLogger.lastSequence - 2

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
			t.Logf("event: %+v", event)
			if event.Key == "key1" && event.Value == "value123" && event.EventType != EventPut {
				t.Fatalf("unexpected event: %+v", event)
			}
			if event.Key == "key1" && event.Value == "" && event.EventType != EventDelete {
				t.Fatalf("unexpected event: %+v", event)
			}
		}
	}
}
