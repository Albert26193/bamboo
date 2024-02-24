package db

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
	"tiny-bitcask/db/utils"

	"github.com/stretchr/testify/assert"
)

func TestOpen(t *testing.T) {
	opts := DefaultOptions
	currentTime := time.Now().Unix()
	dir, _ := os.MkdirTemp("", strings.Join([]string{"bitcask-open-", strconv.FormatInt(currentTime, 10)}, ""))

	opts.DataDir = dir
	db, err := CreateDB(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
}

func TestPut(t *testing.T) {
	opts := DefaultOptions
	currentTime := time.Now().Unix()
	dir, _ := os.MkdirTemp("", strings.Join([]string{"bitcask-put-", strconv.FormatInt(currentTime, 10)}, ""))

	opts.DataDir = dir
	opts.DataSize = 64 * 1024 * 1024
	db, err := CreateDB(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1. put one entry
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	// 2. put duplicate key
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
	val2, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val2)

	// 3.key is empty
	err = db.Put(nil, utils.RandomValue(24))
	assert.Equal(t, ErrEmptyKey, err)

	// 4.value is empty
	err = db.Put(utils.GetTestKey(22), nil)
	assert.Nil(t, err)
	val3, err := db.Get(utils.GetTestKey(22))
	assert.Equal(t, 0, len(val3))
	assert.Nil(t, err)

	// 5.write data to inactive block
	for i := 0; i < 1000000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.Nil(t, err)
	}

	assert.Equal(t, 2, len(db.inactiveBlock))

	// 6. put data to inactive block and restart db
	err = db.Close()
	assert.Nil(t, err)

	// restart db
	db2, err := CreateDB(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db2)
	val4 := utils.RandomValue(128)
	err = db2.Put(utils.GetTestKey(55), val4)
	assert.Nil(t, err)
	val5, err := db2.Get(utils.GetTestKey(55))
	assert.Nil(t, err)
	assert.Equal(t, val4, val5)
}

func TestGet(t *testing.T) {
	opts := DefaultOptions
	currentTime := time.Now().Unix()
	dir, _ := os.MkdirTemp("", strings.Join([]string{"bitcask-get-", strconv.FormatInt(currentTime, 10)}, ""))

	opts.DataDir = dir
	opts.DataSize = 64 * 1024 * 1024
	db, err := CreateDB(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1.get one entry
	err = db.Put(utils.GetTestKey(11), utils.RandomValue(24))
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(11))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	// 2.read empty key
	val2, err := db.Get([]byte("some key unknown"))
	assert.Nil(t, val2)
	assert.Equal(t, ErrKeyNotFound, err)

	// 3.put and get
	err = db.Put(utils.GetTestKey(22), utils.RandomValue(24))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(22), utils.RandomValue(24))
	assert.Nil(t, err)
	val3, err := db.Get(utils.GetTestKey(22))
	assert.Nil(t, err)
	assert.NotNil(t, val3)

	// 4. put a real value
	err = db.Put([]byte("this is a real key"), []byte("this is a real value"))
	assert.Nil(t, err)
	val4, err := db.Get([]byte("this is a real key"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("this is a real value"), val4)

	// 5.delete and get
	err = db.Put(utils.GetTestKey(33), utils.RandomValue(24))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(33))
	assert.Nil(t, err)
	val4, err = db.Get(utils.GetTestKey(33))
	assert.Equal(t, 0, len(val4))
	assert.Equal(t, ErrKeyNotFound, err)

	// 6.get data from inactive block
	for i := 100; i < 1000000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.Nil(t, err)
	}
	assert.Equal(t, 2, len(db.inactiveBlock))
	val5, err := db.Get(utils.GetTestKey(101))
	assert.Nil(t, err)
	assert.NotNil(t, val5)

	err = db.Close()
	assert.Nil(t, err)

	// restart db
	db2, err := CreateDB(opts)
	assert.Nil(t, err)

	val6, err := db2.Get(utils.GetTestKey(11))
	assert.Nil(t, err)
	assert.NotNil(t, val6)
	assert.Equal(t, val1, val6)

	val7, err := db2.Get(utils.GetTestKey(22))
	assert.Nil(t, err)
	assert.NotNil(t, val7)
	assert.Equal(t, val3, val7)

	val8, err := db.Get(utils.GetTestKey(33))
	assert.Equal(t, 0, len(val8))
	assert.Equal(t, ErrKeyNotFound, err)
}

func TestDelete(t *testing.T) {
	opts := DefaultOptions
	currentTime := time.Now().Unix()
	dir, _ := os.MkdirTemp("", strings.Join([]string{"bitcask-delete-", strconv.FormatInt(currentTime, 10)}, ""))
	opts.DataDir = dir
	opts.DataSize = 64 * 1024 * 1024

	db, err := CreateDB(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	// 0. remove a real key
	err = db.Put([]byte("this is a real key"), []byte("this is a real value"))
	assert.Nil(t, err)
	err = db.Delete([]byte("this is a real key"))
	assert.Nil(t, err)
	val, err := db.Get([]byte("this is a real key"))
	fmt.Println(val)
	assert.Equal(t, ErrKeyNotFound, err)

	// 1.remove one entry
	err = db.Put(utils.GetTestKey(11), utils.RandomValue(128))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(11))
	assert.Nil(t, err)

	_, err = db.Get(utils.GetTestKey(11))
	assert.Equal(t, ErrKeyNotFound, err)

	// 2.remove unknown key
	err = db.Delete([]byte("unknown key"))
	assert.Nil(t, err)

	// 3.remove empty key
	err = db.Delete(nil)
	assert.Equal(t, ErrEmptyKey, err)

	// 4.remove and Put
	err = db.Put(utils.GetTestKey(22), utils.RandomValue(128))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(22))
	assert.Nil(t, err)

	err = db.Put(utils.GetTestKey(22), utils.RandomValue(128))
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(22))
	assert.NotNil(t, val1)
	assert.Nil(t, err)

	// 5.restart db and remove
	err = db.Close()
	assert.Nil(t, err)

	// restart db
	db2, err := CreateDB(opts)
	assert.Nil(t, err)
	_, err = db2.Get(utils.GetTestKey(11))
	assert.Equal(t, ErrKeyNotFound, err)

	val2, err := db2.Get(utils.GetTestKey(22))
	assert.Nil(t, err)
	assert.Equal(t, val1, val2)
}

func TestDBListKeys(t *testing.T) {
	opts := DefaultOptions
	currentTime := time.Now().Unix()
	dir, _ := os.MkdirTemp("", strings.Join([]string{"bitcask-listkeys-", strconv.FormatInt(currentTime, 10)}, ""))
	opts.DataDir = dir
	db, err := CreateDB(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// empty
	keys1 := db.ListKeys()
	assert.Equal(t, 0, len(keys1))

	// one entry
	err = db.Put(utils.GetTestKey(11), utils.RandomValue(20))
	assert.Nil(t, err)
	keys2 := db.ListKeys()
	assert.Equal(t, 1, len(keys2))

	// multiple entries
	err = db.Put(utils.GetTestKey(22), utils.RandomValue(20))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(33), utils.RandomValue(20))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(44), utils.RandomValue(20))
	assert.Nil(t, err)

	keys3 := db.ListKeys()
	assert.Equal(t, 4, len(keys3))
	for _, k := range keys3 {
		assert.NotNil(t, k)
	}
}

func TestDBFold(t *testing.T) {
	opts := DefaultOptions
	currentTime := time.Now().Unix()
	dir, _ := os.MkdirTemp("", strings.Join([]string{"bitcask-fold-", strconv.FormatInt(currentTime, 10)}, ""))

	opts.DataDir = dir
	db, err := CreateDB(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(11), utils.RandomValue(20))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(22), utils.RandomValue(20))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(33), utils.RandomValue(20))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(44), utils.RandomValue(20))
	assert.Nil(t, err)

	err = db.Fold(func(key []byte, value []byte) bool {
		assert.NotNil(t, key)
		assert.NotNil(t, value)
		return true
	})
	assert.Nil(t, err)
}

func TestDBClose(t *testing.T) {
	opts := DefaultOptions
	currentTime := time.Now().Unix()
	dir, _ := os.MkdirTemp("", strings.Join([]string{"bitcask-close-", strconv.FormatInt(currentTime, 10)}, ""))

	opts.DataDir = dir
	db, err := CreateDB(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(11), utils.RandomValue(20))
	assert.Nil(t, err)
}

func TestDB_Sync(t *testing.T) {
	opts := DefaultOptions
	currentTime := time.Now().Unix()
	dir, _ := os.MkdirTemp("", strings.Join([]string{"bitcask-sync-", strconv.FormatInt(currentTime, 10)}, ""))

	opts.DataDir = dir
	db, err := CreateDB(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(11), utils.RandomValue(20))
	assert.Nil(t, err)

	err = db.Sync()
	assert.Nil(t, err)
}
