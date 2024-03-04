package resp

import (
	"bamboo/db"
)

type RedisStructure struct {
	dataBase *db.DB
}

func NewRedisStructure(options db.Options) (*RedisStructure, error) {
	dataBase, err := db.CreateDB(options)
	if err != nil {
		return nil, err
	}
	return &RedisStructure{
		dataBase: dataBase,
	}, nil
}

// ######### basic function #########
func (rs *RedisStructure) Del(key []byte) error {
	return rs.dataBase.Delete(key)
}

func (r *RedisStructure) Type(key []byte) (redisStructType, error) {
	wrappedValue, err := r.dataBase.Get(key)

	if err != nil {
		return 0, err
	}

	if len(wrappedValue) == 0 {
		return 0, ErrEmptyValue
	}

	// first byte is the type
	return wrappedValue[0], nil
}
