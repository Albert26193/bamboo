package resp

import (
	"encoding/binary"
	"time"
)

/*
key to:
+----------------+-----------------+---------------+
|  data type(1)  |   expire(1-10) |  payload(1-?)  |
+----------------+-----------------+---------------+
*/
func (r *RedisStructure) Set(key []byte, actualValue []byte, ttl time.Duration) error {
	if len(key) == 0 || len(actualValue) == 0 {
		return nil
	}

	// value = type + expire + payload
	// payload = actualValue
	// why +1 : 1byte for type

	// 1. type
	buffer := make([]byte, binary.MaxVarintLen64+1)
	buffer[0] = RedisString
	var pointer = 1

	// 2. expire
	var expire = int64(0)
	if ttl > 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}
	pointer += binary.PutVarint(buffer[1:], expire)

	// 3. put together into wrappedValue
	wrappedValue := make([]byte, pointer+len(actualValue))
	copy(wrappedValue[:pointer], buffer[:pointer])
	copy(wrappedValue[pointer:], actualValue)

	// 4. set into db
	return r.dataBase.Put(key, wrappedValue)
}

/*
key to:
+----------------+-----------------+---------------+
|  data type(1)  |   expire(1-10) |  payload(1-?)  |
+----------------+-----------------+---------------+
get = db.get +  decode operation
*/
func (r *RedisStructure) Get(key []byte) ([]byte, error) {
	wrappedValue, err := r.dataBase.Get(key)
	if err != nil {
		return nil, err
	}

	// decode operation
	// 1. type
	if wrappedValue[0] != RedisString {
		return nil, ErrTypeOperation
	}
	pointer := 1

	// 2. expire
	expire, n := binary.Varint(wrappedValue[pointer:])
	pointer += n
	if expire > 0 && time.Now().UnixNano() >= expire {
		return nil, nil
	}

	// 3. payload is the actual value
	return wrappedValue[pointer:], nil
}
