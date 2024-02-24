package db

import (
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
	"tiny-bitcask/db/utils"

	"github.com/stretchr/testify/assert"
)


func TestNewIterator(t *testing.T) {
	opts := DefaultOptions
	currentTime := time.Now().Unix()
	dir, _ := os.MkdirTemp("", strings.Join([]string{"bitcask-iterator-", strconv.FormatInt(currentTime, 10)}, ""))
	opts.DataDir = dir
	db, err := CreateDB(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	iterator := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iterator)
	assert.Equal(t, false, iterator.Valid())
}

func TestIterator(t *testing.T) {
	opts := DefaultOptions
	currentTime := time.Now().Unix()
	dir, _ := os.MkdirTemp("", strings.Join([]string{"bitcask-iterator-", strconv.FormatInt(currentTime, 10)}, ""))

	opts.DataDir = dir
	db, err := CreateDB(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(10), utils.GetTestKey(10))
	assert.Nil(t, err)

	iterator := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iterator)
	assert.Equal(t, true, iterator.Valid())
	assert.Equal(t, utils.GetTestKey(10), iterator.Key())
	val, err := iterator.Value()
	assert.Nil(t, err)
	assert.Equal(t, utils.GetTestKey(10), val)
}

func TestDBIteratorMultiValues(t *testing.T) {
	opts := DefaultOptions
	currentTime := time.Now().Unix()
	dir, _ := os.MkdirTemp("", strings.Join([]string{"bitcask-iterator-", strconv.FormatInt(currentTime, 10)}, ""))

	opts.DataDir = dir
	db, err := CreateDB(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("annde"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("cnedc"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("aeeue"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("esnue"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("bnede"), utils.RandomValue(10))
	assert.Nil(t, err)

	// iterate
	iter1 := db.NewIterator(DefaultIteratorOptions)
	for iter1.Rewind(); iter1.Valid(); iter1.Next() {
		assert.NotNil(t, iter1.Key())
	}
	iter1.Rewind()
	for iter1.Seek([]byte("c")); iter1.Valid(); iter1.Next() {
		assert.NotNil(t, iter1.Key())
	}

	// reverse
	iterOpts1 := DefaultIteratorOptions
	iterOpts1.Reverse = true
	iter2 := db.NewIterator(iterOpts1)
	for iter2.Rewind(); iter2.Valid(); iter2.Next() {
		assert.NotNil(t, iter2.Key())
	}
	iter2.Rewind()
	for iter2.Seek([]byte("c")); iter2.Valid(); iter2.Next() {
		assert.NotNil(t, iter2.Key())
	}

	// prefix
	iterOpts2 := DefaultIteratorOptions
	iterOpts2.Prefix = []byte("aee")
	iter3 := db.NewIterator(iterOpts2)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.NotNil(t, iter3.Key())
	}
}
