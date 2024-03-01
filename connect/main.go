package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"tiny-bitcask/db"
)

var dbInstance *db.DB

func init() {
	var err error
	options := db.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-http")
	options.DataDir = dir
	dbInstance, err = db.CreateDB(options)
	if err != nil {
		panic(fmt.Sprintf("failed to open db: %v", err))
	}
}

func putHandler(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var kv map[string]string

	if err := json.NewDecoder(request.Body).Decode(&kv); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}

	for key, value := range kv {
		if err := dbInstance.Put([]byte(key), []byte(value)); err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			log.Printf("failed to put kv in db: %v\n", err)
			return
		}
	}
}

func getHandler(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := request.URL.Query().Get("key")

	value, err := dbInstance.Get([]byte(key))
	if err != nil && err != db.ErrKeyNotFound {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		log.Printf("failed to get kv in db: %v\n", err)
		return
	}

	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(string(value))
}

func deleteHandler(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodDelete {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := request.URL.Query().Get("key")

	err := dbInstance.Delete([]byte(key))
	if err != nil && err != db.ErrEmptyKey {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		log.Printf("failed to get kv in db: %v\n", err)
		return
	}

	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode("OK")
}

func listKeysHandler(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	keys := dbInstance.ListKeys()
	writer.Header().Set("Content-Type", "application/json")
	var result []string
	for _, k := range keys {
		result = append(result, string(k))
	}
	_ = json.NewEncoder(writer).Encode(result)
}

func handleStat(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	stat := dbInstance.GetDBStatus()
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(stat)
}

func main() {
	// register
	http.HandleFunc("/bitcask/put", putHandler)
	http.HandleFunc("/bitcask/get", getHandler)
	http.HandleFunc("/bitcask/delete", deleteHandler)
	http.HandleFunc("/bitcask/listkeys", listKeysHandler)
	http.HandleFunc("/bitcask/stat", handleStat)

	// start
	_ = http.ListenAndServe("0.0.0.0:6378", nil)
}
