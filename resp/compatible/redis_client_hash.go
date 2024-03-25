package main

import "github.com/tidwall/redcon"

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

func hget(cli *RespClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, argsError("hget")
	}

	value, err := cli.db.HGet(args[0], args[1])
	if err != nil {
		return nil, err
	}
	return value, nil
}

func hdel(cli *RespClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, argsError("hdel")
	}

	var ok = 0
	key, field := args[0], args[1]
	res, err := cli.db.HDel(key, field)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}
