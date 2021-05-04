package store

import (
	"errors"
	"testing"
)

func TestPut(t *testing.T) {
	const k = "test-put-key"
	const v = "test-put-val"
	defer func() {
		store.mu.Lock()
		delete(store.m, k)
		store.mu.Unlock()
	}()

	store.mu.RLock()
	_, isPresent := store.m[k]
	store.mu.RUnlock()

	if isPresent {
		t.Errorf("%v already exists", k)
	}
	err := Put(k, v)
	if err != nil {
		t.Error(err)
	}

	store.mu.RLock()
	val, isPresent := store.m[k]
	store.mu.RUnlock()

	if !isPresent {
		t.Errorf("Put %v failed", val)
	}
	if val != v {
		t.Errorf("wrong value put, %v should be %v", val, v)
	}
}

func TestGet(t *testing.T) {
	const k = "test-get-key"
	const v = "test-get-val"

	defer func() {
		store.mu.Lock()
		delete(store.m, k)
		store.mu.Unlock()
	}()

	_, err := Get(k)
	if err == nil {
		t.Errorf("%v key should not be present before insertion", k)
	}
	if !errors.Is(err, ErrNoSuchKey) {
		t.Error("unexpected error ", err)
	}

	store.mu.Lock()
	store.m[k] = v
	store.mu.Unlock()

	val, err := Get(k)
	if err != nil {
		t.Error(err)
	}
	if val != v {
		t.Errorf("got wrong value, %v should be %v", val, v)
	}
}

func TestDelete(t *testing.T) {
	const k = "test-del-key"
	const v = "test-del-val"

	defer func() {
		store.mu.Lock()
		delete(store.m, k)
		store.mu.Unlock()
	}()

	store.mu.RLock()
	_, isPresent := store.m[k]
	store.mu.RUnlock()

	if isPresent {
		t.Errorf("%v key should not be present before insertion", k)
	}

	store.mu.Lock()
	store.m[k] = v
	store.mu.Unlock()

	store.mu.RLock()
	_, isPresent = store.m[k]
	store.mu.RUnlock()

	if !isPresent {
		t.Errorf("%v should exist after insertion", k)
	}

	Delete(k)

	store.mu.RLock()
	_, isPresent = store.m[k]
	store.mu.RUnlock()

	if isPresent {
		t.Errorf("%v should not exist after deletion", k)
	}
}
