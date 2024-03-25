package main

import (
	bamboo "bamboo/db"
	"bufio"
	"fmt"
	"net"
)

func main() {
	// localDemo()
	redisDemo()
}

func localDemo() {
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
func redisDemo() {
	conn, err := net.Dial("tcp", "127.0.0.1:6378")
	if err != nil {
		panic(err)
	}

	cmd := "set k-name-8 test-kv-8\r\n"
	conn.Write([]byte(cmd))

	// get res
	reader := bufio.NewReader(conn)
	res, err := reader.ReadString('\n')
	if err != nil {
		panic(err)
	}
	fmt.Println(res)

}
