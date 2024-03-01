package db

import (
	"bamboo/db/utils"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

// empty
func TestMerge(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bamboo-merge-1")
	opts.DataDir = dir
	db, err := CreateDB(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Merge()
	assert.Nil(t, err)
}

// all valid data
func TestMergeValid(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bamboo-merge-2")
	opts.DataSize = 32 * 1024 * 1024
	opts.MergeThreshold = 0
	opts.DataDir = dir
	db, err := CreateDB(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(t, err)
	}

	err = db.Merge()
	assert.Nil(t, err)

	// restart check
	err = db.Close()
	assert.Nil(t, err)

	db2, err := CreateDB(opts)
	defer func() {
		_ = db2.Close()
	}()
	assert.Nil(t, err)
	keys := db2.ListKeys()
	assert.Equal(t, 50000, len(keys))

	for i := 0; i < 50000; i++ {
		val, err := db2.Get(utils.GetTestKey(i))
		assert.Nil(t, err)
		assert.NotNil(t, val)
	}
}

// has invalid data and new data
func TestMergeMixed(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bamboo-merge-3")
	opts.DataSize = 32 * 1024 * 1024
	opts.MergeThreshold = 0
	opts.DataDir = dir
	db, err := CreateDB(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(t, err)
	}
	for i := 0; i < 10000; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}
	for i := 40000; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), []byte("new value in merge"))
		assert.Nil(t, err)
	}

	err = db.Merge()
	assert.Nil(t, err)

	// restart check
	err = db.Close()
	assert.Nil(t, err)

	db2, err := CreateDB(opts)
	defer func() {
		_ = db2.Close()
	}()
	assert.Nil(t, err)
	keys := db2.ListKeys()
	assert.Equal(t, 40000, len(keys))

	for i := 0; i < 10000; i++ {
		_, err := db2.Get(utils.GetTestKey(i))
		assert.Equal(t, ErrKeyNotFound, err)
	}
	for i := 40000; i < 50000; i++ {
		val, err := db2.Get(utils.GetTestKey(i))
		assert.Nil(t, err)
		assert.Equal(t, []byte("new value in merge"), val)
	}
}

// all invalid data
func TestMergeInvalid(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bamboo-merge-4")
	opts.DataSize = 32 * 1024 * 1024
	opts.MergeThreshold = 0
	opts.DataDir = dir
	db, err := CreateDB(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(t, err)
	}
	for i := 0; i < 50000; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}

	err = db.Merge()
	assert.Nil(t, err)

	// restart check
	err = db.Close()
	assert.Nil(t, err)

	db2, err := CreateDB(opts)
	defer func() {
		_ = db2.Close()
	}()
	assert.Nil(t, err)
	keys := db2.ListKeys()
	assert.Equal(t, 0, len(keys))
}

// Merge in process and hav new data insert or update
func TestMergeInProcess(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bamboo-merge-5")
	opts.DataSize = 32 * 1024 * 1024
	opts.MergeThreshold = 0
	opts.DataDir = dir
	db, err := CreateDB(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(t, err)
	}

	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50000; i++ {
			err := db.Delete(utils.GetTestKey(i))
			assert.Nil(t, err)
		}
		for i := 60000; i < 70000; i++ {
			err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
			assert.Nil(t, err)
		}
	}()
	err = db.Merge()
	assert.Nil(t, err)
	wg.Wait()

	// restart
	err = db.Close()
	assert.Nil(t, err)

	db2, err := CreateDB(opts)
	defer func() {
		_ = db2.Close()
	}()
	assert.Nil(t, err)
	keys := db2.ListKeys()
	assert.Equal(t, 10000, len(keys))

	for i := 60000; i < 70000; i++ {
		val, err := db2.Get(utils.GetTestKey(i))
		assert.Nil(t, err)
		assert.NotNil(t, val)
	}
}
