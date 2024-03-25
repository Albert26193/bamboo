package main

import (
	bamboo "bamboo/db"
	bamboo_redis "bamboo/resp"
	"fmt"
	"strings"

	"github.com/tidwall/redcon"
)

var supportedCommands = map[string]cmdHandler{
	// string
	"set": set,
	"get": get,

	// hash
	"hset": hset,
	"hget": hget,
	"hdel": hdel,

	// list
	"lpush": lpush,
	"rpush": rpush,
	"lpop":  lpop,
	"rpop":  rpop,

	// set
	"sadd":      sadd,
	"srem":      srem,
	"sismember": sismember,

	// zset
	"zadd":   zadd,
	"zscore": zscore,
}

type RespClient struct {
	server *RespServer
	db     *bamboo_redis.RedisStructure
}

func argsError(cmd string) error {
	return fmt.Errorf("ERR: wrong arguments of '%s' cmd", cmd)
}

type cmdHandler func(cli *RespClient, args [][]byte) (interface{}, error)

func clientCmd(conn redcon.Conn, cmd redcon.Command) {
	command := strings.ToLower(string(cmd.Args[0]))
	cmdFunc, ok := supportedCommands[command]
	if !ok {
		conn.WriteError("Err unsupported command: '" + command + "'")
		return
	}

	client, _ := conn.Context().(*RespClient)
	switch command {
	case "quit":
		_ = conn.Close()
	case "hello":
		conn.WriteString("nice to meet you")
	default:
		res, err := cmdFunc(client, cmd.Args[1:])
		if err != nil {
			if err == bamboo.ErrKeyNotFound {
				conn.WriteNull()
			} else {
				conn.WriteError(err.Error())
			}
			return
		}
		conn.WriteAny(res)
	}
}
