package resp

import "errors"

var (
	ErrTypeOperation = errors.New("RESP: Error Type Operation")
	ErrEmptyValue    = errors.New("RESP: Empty Value Error")
)

type redisStructType = byte

const (
	RedisString redisStructType = 0
	RedisHash   redisStructType = 1
	RedisSet    redisStructType = 2
	RedisList   redisStructType = 3
	RedisZset   redisStructType = 4
)
