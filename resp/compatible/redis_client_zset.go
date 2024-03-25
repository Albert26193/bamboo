package main

import (
	"bamboo/db/utils"

	"github.com/tidwall/redcon"
)

// zadd is the handler for the ZADD command.
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

// zscore is the handler for the ZSCORE command.
func zscore(cli *RespClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, argsError("zscore")
	}

	value, err := cli.db.ZScore(args[0], args[1])
	if err != nil {
		return nil, err
	}

	return redcon.SimpleString(utils.Float64ToBytes(value)), nil
}
