package main

import (
	bamboo "bamboo/db"
	"bamboo/db/utils"
	bamboo_redis "bamboo/resp"
	"fmt"
	"strings"

	"github.com/tidwall/redcon"
)

var supportedCommands = map[string]cmdHandler{
	"set":   set,
	"get":   get,
	"hset":  hset,
	"sadd":  sadd,
	"lpush": lpush,
	"zadd":  zadd,
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

func set(cli *RespClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, argsError("set")
	}

	key, value := args[0], args[1]
	if err := cli.db.Set(key, value, 0); err != nil {
		return nil, err
	}
	return redcon.SimpleString("OK"), nil
}

func get(cli *RespClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, argsError("get")
	}

	value, err := cli.db.Get(args[0])
	if err != nil {
		return nil, err
	}
	return value, nil
}

func hset(cli *RespClient, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, argsError("hset")
	}

	var ok = 0
	key, field, value := args[0], args[1], args[2]
	res, err := cli.db.HSet(key, field, value)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}

func sadd(cli *RespClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, argsError("sadd")
	}

	var ok = 0
	key, member := args[0], args[1]
	res, err := cli.db.SAdd(key, member)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}

func lpush(cli *RespClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, argsError("lpush")
	}

	key, value := args[0], args[1]
	res, err := cli.db.LPush(key, value)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(res), nil
}

func zadd(cli *RespClient, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, argsError("zadd")
	}

	var ok = 0
	key, score, member := args[0], args[1], args[2]
	res, err := cli.db.ZAdd(key, utils.BytesToFloat64(score), member)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}
