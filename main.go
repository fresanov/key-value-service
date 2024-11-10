package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/gorilla/mux"
)

func putHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	key := vars["key"]
	value, err := io.ReadAll(r.Body)

	if err != nil {
		http.Error(w,
			err.Error(),
			http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()
	err = Put(key, string(value))
	if err != nil {
		http.Error(w,
			err.Error(),
			http.StatusInternalServerError)
		return
	}
	logger.WritePut(key, string(value))
	w.WriteHeader(http.StatusCreated)
}

func deleteHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	key := vars["key"]
	err := Delete(key)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	logger.WriteDelete(key)
	w.WriteHeader(http.StatusNoContent)
}

func getHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	key := vars["key"]
	value, err := Get(key)

	if errors.Is(err, ErrNoSuchKey) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Fprint(w, value)
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

func main() {
	initializeTransactionLog("file")

	r := mux.NewRouter()

	r.HandleFunc("/v1/key/{key}", putHandler).Methods("PUT")
	r.HandleFunc("/v1/key/{key}", getHandler).Methods("GET")
	r.HandleFunc("/v1/key/{key}", deleteHandler).Methods("DELETE")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.Printf("Received signal: %s. Shutting down...", sig)
		gracefulShutdown()
		os.Exit(0)
	}()

	log.Println("Starting server on :8080")
	log.Fatal(http.ListenAndServe(":8080", r))
}
