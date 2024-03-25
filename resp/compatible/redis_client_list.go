package main

import "github.com/tidwall/redcon"

// lpush is the handler for the LPUSH command.
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

// rpush is the handler for the RPUSH command.
func rpush(cli *RespClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, argsError("rpush")
	}

	key, value := args[0], args[1]
	res, err := cli.db.RPush(key, value)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(res), nil
}

// lpop is the handler for the LPOP command.
func lpop(cli *RespClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, argsError("lpop")
	}

	value, err := cli.db.LPop(args[0])
	if err != nil {
		return nil, err
	}
	return value, nil
}

// rpop is the handler for the RPOP command.
func rpop(cli *RespClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, argsError("rpop")
	}

	value, err := cli.db.RPop(args[0])
	if err != nil {
		return nil, err
	}
	return value, nil
}
