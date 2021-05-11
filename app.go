package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	"github.com/DharmikOO7/KeyV/store"
	"github.com/DharmikOO7/KeyV/transactionlogger"
	"github.com/gorilla/mux"
)

var logger transactionlogger.TransactionLogger

func initializeTransactionLog() error {
	var err error
	logger, err = transactionlogger.NewTransactionLogger("transaction.log")
	if err != nil {
		return fmt.Errorf("cannot initialize event logger: %w", err)
	}
	events, errors := logger.ReadEvents()
	e, ok := transactionlogger.Event{}, true
	// Get previous events if any and restore state from them
	for ok && err == nil {
		select {
		case err, ok = <-errors:
		case e, ok = <-events:
			switch e.EventType {
			case transactionlogger.EventDelete:
				err = store.Delete(e.Key)
			case transactionlogger.EventPut:
				err = store.Put(e.Key, e.Value)
			}
		}
	}

	// Start the logger
	logger.Run()

	return err
}

func homepageHandler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte(`KeyV("Kiwi") is a key value store written in Go`))
}

func putHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]
	val, err := io.ReadAll(r.Body)
	defer r.Body.Close()

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	err = store.Put(key, string(val))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	logger.WritePut(key, string(val))
	w.WriteHeader(http.StatusCreated)
}

func deleteHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]
	err := store.Delete(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	logger.WriteDelete(key)
	w.WriteHeader(http.StatusOK)
}

func getHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]
	val, err := store.Get(key)
	if errors.Is(err, store.ErrNoSuchKey) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write([]byte(val))
}

func main() {
	err := initializeTransactionLog()
	if err != nil {
		panic(err)
	}
	port := os.Getenv("PORT")

	if port == "" {
		port = "8080"
		// log.Fatal("$PORT must be set")
	}

	r := mux.NewRouter()

	r.HandleFunc("/", homepageHandler)
	r.HandleFunc("/v1/{key}", getHandler).Methods("GET")
	r.HandleFunc("/v1/{key}", putHandler).Methods("PUT")
	r.HandleFunc("/v1/{key}", deleteHandler).Methods("DELETE")
	fmt.Println("Starting server")
	log.Fatal(http.ListenAndServe(":8080", r))
}
