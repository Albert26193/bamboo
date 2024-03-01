package main

import (
	bamboo "bamboo/db"
	"fmt"
)

func main() {
	opts := bamboo.DefaultOptions
	opts.DataDir = "/tmp/bamboo-demo"
	db, err := bamboo.CreateDB(opts)
	if err != nil {
		panic(err)
	}

	err = db.Put([]byte("name"), []byte("bamboo"))
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
