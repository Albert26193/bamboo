package main

import (
	bamboo "bamboo/db"
	bamboo_redis "bamboo/resp"
	"log"
	"sync"

	"github.com/tidwall/redcon"
)

const addr = "0.0.0.0:16378"

type RespServer struct {
	server *redcon.Server
	mu     sync.RWMutex
	dbs    map[int]*bamboo_redis.RedisStructure
}

func main() {
	redisDataStructure, err := bamboo_redis.NewRedisStructure(bamboo.DefaultOptions)
	if err != nil {
		panic(err)
	}

	rsServer := &RespServer{
		dbs: make(map[int]*bamboo_redis.RedisStructure),
	}
	rsServer.dbs[0] = redisDataStructure

	// init server
	rsServer.server = redcon.NewServer(addr, clientCmd, rsServer.accept, rsServer.close)

	rsServer.listen()
}

func (svr *RespServer) listen() {
	log.Println("Server Ready and Listening on", addr)
	_ = svr.server.ListenAndServe()
}

func (rs *RespServer) accept(conn redcon.Conn) bool {
	cli := new(RespClient)

	rs.mu.Lock()
	defer rs.mu.Unlock()

	cli.server = rs
	cli.db = rs.dbs[0]
	conn.SetContext(cli)
	return true
}

func (svr *RespServer) close(conn redcon.Conn, err error) {
	for _, db := range svr.dbs {
		_ = db.Close()
	}

	_ = svr.server.Close()
}
