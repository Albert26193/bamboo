package db

import (
	"os"
	"testing"

	"bamboo/db/utils"

	"github.com/stretchr/testify/assert"
)

func TestWriteBatch1(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bamboo-batch-1")
	opts.DataDir = dir
	db, err := CreateDB(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// write and not commit
	wb := db.NewAtomicWrite(DefaultWriteOptions)
	err = wb.Put(utils.GetTestKey(1), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb.Delete(utils.GetTestKey(2))
	assert.Nil(t, err)

	_, err = db.Get(utils.GetTestKey(1))
	assert.Equal(t, ErrKeyNotFound, err)

	err = wb.Commit()
	assert.Nil(t, err)

	val1, err := db.Get(utils.GetTestKey(1))
	assert.NotNil(t, val1)
	assert.Nil(t, err)

	wb2 := db.NewAtomicWrite(DefaultWriteOptions)
	err = wb2.Delete(utils.GetTestKey(1))
	assert.Nil(t, err)
	err = wb2.Commit()
	assert.Nil(t, err)

	_, err = db.Get(utils.GetTestKey(1))
	assert.Equal(t, ErrKeyNotFound, err)
}

func TestWriteBatch2(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bamboo-batch-2")
	opts.DataDir = dir
	db, err := CreateDB(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(1), utils.RandomValue(10))
	assert.Nil(t, err)

	wb := db.NewAtomicWrite(DefaultWriteOptions)
	err = wb.Put(utils.GetTestKey(2), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb.Delete(utils.GetTestKey(1))
	assert.Nil(t, err)

	err = wb.Commit()
	assert.Nil(t, err)

	err = wb.Put(utils.GetTestKey(11), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb.Commit()
	assert.Nil(t, err)

	err = db.Close()
	assert.Nil(t, err)

	db2, err := CreateDB(opts)
	assert.Nil(t, err)

	_, err = db2.Get(utils.GetTestKey(1))
	assert.Equal(t, ErrKeyNotFound, err)

	assert.Equal(t, uint64(2), db.atomicSeq)
}
