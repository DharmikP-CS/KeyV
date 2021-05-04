package store

import (
	"errors"
	"sync"
)

var store = struct {
	mu sync.RWMutex
	m  map[string]string
}{m: make(map[string]string)}

func Put(key string, value string) error {
	store.mu.Lock()
	store.m[key] = value
	store.mu.Unlock()

	return nil
}

var ErrNoSuchKey error = errors.New("no such key found")

func Get(key string) (string, error) {
	store.mu.RLock()
	val, ok := store.m[key]
	store.mu.RUnlock()
	if !ok {
		return "", ErrNoSuchKey
	}
	return val, nil
}

func Delete(key string) error {
	store.mu.Lock()
	delete(store.m, key)
	store.mu.Unlock()
	return nil
}
