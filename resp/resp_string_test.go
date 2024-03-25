package resp

import (
	"bamboo/db"
	"bamboo/db/utils"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRespGet(t *testing.T) {
	opts := db.DefaultOptions
	dir, _ := os.MkdirTemp("/tmp", "resp-test")
	opts.DataDir = dir
	rds, err := NewRedisStructure(opts)
	assert.Nil(t, err)

	err = rds.Set(utils.GetTestKey(1), utils.RandomValue(100), 0)
	assert.Nil(t, err)
	err = rds.Set(utils.GetTestKey(2), utils.RandomValue(100), time.Second*100)
	assert.Nil(t, err)

	val1, err := rds.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	val2, err := rds.Get(utils.GetTestKey(2))
	assert.Nil(t, err)
	assert.NotNil(t, val2)

	_, err = rds.Get(utils.GetTestKey(888))
	assert.Equal(t, db.ErrKeyNotFound, err)
}

func TestRespDel(t *testing.T) {
	opts := db.DefaultOptions
	dir, _ := os.MkdirTemp("/tmp", "resp-del-type")
	opts.DataDir = dir
	rds, err := NewRedisStructure(opts)
	assert.Nil(t, err)

	// del
	err = rds.Del(utils.GetTestKey(11))
	assert.Nil(t, err)

	err = rds.Set(utils.GetTestKey(1), utils.RandomValue(100), 0)
	assert.Nil(t, err)

	// type
	typ, err := rds.Type(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, RedisString, typ)

	err = rds.Del(utils.GetTestKey(1))
	assert.Nil(t, err)

	_, err = rds.Get(utils.GetTestKey(1))
	assert.Equal(t, db.ErrKeyNotFound, err)
}
