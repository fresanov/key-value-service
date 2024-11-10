package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
)

type CloseableBuffer struct {
	bytes.Buffer
}

func (cb *CloseableBuffer) Close() error {
	// No-op for in-memory buffer
	return nil
}

type FileTransactionLogger struct {
	events       chan<- Event       // Write-only channel for sending events
	errors       <-chan error       // Read-only channel for receiving errors
	lastSequence uint64             // The last used event sequence number
	file         io.ReadWriteCloser // The location of the transaction log
}

// NewFileTransactionLogger creates a new FileTransactionLogger
// that writes to the specified file. If the filename is empty,
// the logger will write to an in-memory buffer (intended for unit-testing).
func NewFileTransactionLogger(filename string) (TransactionLogger, error) {
	if filename == "" {
		return &FileTransactionLogger{file: new(CloseableBuffer)}, nil
	}
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return nil, fmt.Errorf("cannot open transaction log file: %w", err)
	}
	return &FileTransactionLogger{file: file}, nil
}

func (l *FileTransactionLogger) WritePut(key, value string) {
	l.events <- Event{EventType: EventPut, Key: key, Value: value}
}

func (l *FileTransactionLogger) WriteDelete(key string) {
	l.events <- Event{EventType: EventDelete, Key: key}
}

func (l *FileTransactionLogger) Err() <-chan error {
	return l.errors
}

func (l *FileTransactionLogger) Shutdown() {
	// TODO flush channels to file
	l.file.Close()
}

func (l *FileTransactionLogger) Run() *sync.WaitGroup {
	events := make(chan Event, 16) // Make an events channel
	l.events = events
	errors := make(chan error, 1) // Make an errors channel
	l.errors = errors
	var once sync.Once
	var wg sync.WaitGroup

	go func() {
		encoder := json.NewEncoder(l.file)
		for e := range events { // Retrieve the next Event
			l.lastSequence++ // Increment sequence number
			e.Sequence = l.lastSequence

			if err := encoder.Encode(e); err != nil {
				log.Printf("error encoding event: %v", err)
				errors <- err
				return
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

func (l *FileTransactionLogger) ReadEvents() (<-chan Event, <-chan error) {
	outEvent := make(chan Event)    // An unbuffered Event channel
	outError := make(chan error, 1) // A buffered error channel

	go func() {
		defer close(outEvent) // Close the channels when the
		defer close(outError) // goroutine ends

		decoder := json.NewDecoder(l.file)
		for {
			var e Event
			if err := decoder.Decode(&e); err != nil {
				if err == io.EOF {
					break
				}
				outError <- fmt.Errorf("input parse error: %w", err)
				return
			}

			// Sanity check! Are the sequence numbers in increasing order?
			if l.lastSequence >= e.Sequence {
				outError <- fmt.Errorf("transaction numbers out of sequence, sequence: %d, last sequence: %d", e.Sequence, l.lastSequence)
				return
			}
			l.lastSequence = e.Sequence // Update last used sequence #

			outEvent <- e // Send the event along
		}
	}()

	return outEvent, outError
}
