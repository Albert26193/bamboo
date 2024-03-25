package main

import "github.com/tidwall/redcon"

// sadd is the handler for the SADD command.
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

// srem is the handler for the SREM command.
func srem(cli *RespClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, argsError("srem")
	}

	var ok = 0
	key, member := args[0], args[1]
	res, err := cli.db.SRem(key, member)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}

// sismember is the handler for the SISMEMBER command.
func sismember(cli *RespClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, argsError("sismember")
	}

	key, member := args[0], args[1]
	res, err := cli.db.SIsMember(key, member)
	if err != nil {
		return nil, err
	}

	var ok = 0
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}
