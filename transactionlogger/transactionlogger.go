package transactionlogger

import (
	"bufio"
	"fmt"
	"net/url"
	"os"
)

type EventType byte

const (
	_                     = iota
	EventDelete EventType = iota //=1
	EventPut                     //=2
)

type Event struct {
	Sequence  uint64
	EventType EventType
	Key       string
	Value     string
}

func (e Event) String() string {
	return fmt.Sprintf("%v %v %v %v", e.Sequence, e.EventType, e.Key, e.Value)
}

type TransactionLogger interface {
	WritePut(key, value string)
	WriteDelete(key string)
	Err() <-chan error
	ReadEvents() (<-chan Event, <-chan error)
	Run()
}

type FileTransactionLogger struct {
	events       chan<- Event
	errors       <-chan error
	lastSequence uint64
	file         *os.File
}

func (f *FileTransactionLogger) WritePut(key, value string) {
	f.events <- Event{EventType: EventPut, Key: key, Value: url.QueryEscape(value)}
}

func (f *FileTransactionLogger) WriteDelete(key string) {
	f.events <- Event{EventType: EventDelete, Key: key, Value: "_deleted_"}
}

func (f *FileTransactionLogger) Err() <-chan error {
	return f.errors
}

func (f *FileTransactionLogger) Run() {
	events := make(chan Event, 16)
	f.events = events

	errors := make(chan error, 1)
	f.errors = errors
	go func() {
		for e := range events {
			f.lastSequence++
			_, err := fmt.Fprintf(f.file, "%d\t%d\t%s\t%s\n", f.lastSequence, e.EventType, e.Key, e.Value)
			if err != nil {
				errors <- err
				return
			}
		}
		fmt.Println("Ending Run's goroutine")
	}()
}

func (f *FileTransactionLogger) ReadEvents() (<-chan Event, <-chan error) {
	scanner := bufio.NewScanner(f.file)
	outevents := make(chan Event)
	outerrors := make(chan error, 1)
	go func() {
		var e Event
		defer close(outevents)
		defer close(outerrors)
		for scanner.Scan() {
			line := scanner.Text()
			if _, err := fmt.Sscanf(line, "%d\t%d\t%s\t%s", &e.Sequence, &e.EventType, &e.Key, &e.Value); err != nil {
				outerrors <- fmt.Errorf("error while parsing file: %w", err)
				return
			}
			if f.lastSequence >= e.Sequence {
				outerrors <- fmt.Errorf("transcation numbers out of sequence")
				return
			}
			val, err := url.QueryUnescape(e.Value)
			if err != nil {
				outerrors <- fmt.Errorf("unable to unescape value %v: %w", e.Value, err)
				return
			}
			e.Value = val
			f.lastSequence = e.Sequence
			outevents <- e
		}

		if err := scanner.Err(); err != nil {
			outerrors <- fmt.Errorf("transcation log read failure: %w", err)
			return
		}
	}()
	return outevents, outerrors
}

func NewTransactionLogger(filename string) (TransactionLogger, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return nil, fmt.Errorf("cannot open transaction log file: %v", err)
	}
	return &FileTransactionLogger{file: file}, nil
}
