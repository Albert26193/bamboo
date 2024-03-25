package main

import "github.com/tidwall/redcon"

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
