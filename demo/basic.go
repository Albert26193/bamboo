package main

import (
	"fmt"
	bitcask "tiny-bitcask/db"
)

func main() {
	opts := bitcask.DefaultOptions
	opts.DataDir = "/tmp/tiny-bitcask-go-x1"
	db, err := bitcask.CreateDB(opts)
	if err != nil {
		panic(err)
	}

	err = db.Put([]byte("name"), []byte("bitcask"))
	if err != nil {
		panic(err)
	}
	val, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	fmt.Println("val = ", string(val))

	err = db.Delete([]byte("name"))
	if err != nil {
		panic(err)
	}
}
