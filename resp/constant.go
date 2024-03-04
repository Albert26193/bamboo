package resp

import "errors"

var (
	ErrTypeOperation = errors.New("RESP: Error Type Operation")
	ErrEmptyValue    = errors.New("RESP: Empty Value Error")
)

type redisStructType = byte

const (
	RedisString redisStructType = 0
	RedisHash   redisStructType = 0
	RedisSet    redisStructType = 0
	RedisList   redisStructType = 0
	RedisZset   redisStructType = 0
)
