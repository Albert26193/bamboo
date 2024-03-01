package content

import (
	"bamboo/diskIO"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOpenBlockFile(t *testing.T) {
	dataFile1, err := OpenBlock(os.TempDir(), 0, diskIO.MMapIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)

	dataFile2, err := OpenBlock(os.TempDir(), 123, diskIO.FileSystemIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile2)

	dataFile3, err := OpenBlock(os.TempDir(), 65535*65535, diskIO.MMapIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile3)
}

func TestDataWrite(t *testing.T) {
	dataFile, err := OpenBlock(os.TempDir(), 0, diskIO.FileSystemIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("temp"))
	assert.Nil(t, err)

	err = dataFile.Write([]byte("this is a test"))
	assert.Nil(t, err)

	err = dataFile.Write([]byte(""))
	assert.Nil(t, err)
}

func TestDataClose(t *testing.T) {
	dataFile, err := OpenBlock(os.TempDir(), 123, diskIO.FileSystemIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("test demo"))
	assert.Nil(t, err)

	err = dataFile.Close()
	assert.Nil(t, err)
}

func TestSync(t *testing.T) {
	dataFile, err := OpenBlock(os.TempDir(), 456, diskIO.FileSystemIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("this is a test"))
	assert.Nil(t, err)

	err = dataFile.Sync()
	assert.Nil(t, err)
}

func TestReadLogRecord(t *testing.T) {
	dataFile, err := OpenBlock(os.TempDir(), 99, diskIO.FileSystemIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	rec1 := &LogStruct{
		Key:   []byte("name"),
		Value: []byte("bamboo"),
	}
	res1, size1 := Encoder(rec1)
	err = dataFile.Write(res1)
	assert.Nil(t, err)

	readRec1, readSize1, err := dataFile.ReadLog(0)
	assert.Nil(t, err)
	assert.Equal(t, rec1, readRec1)
	assert.Equal(t, size1, readSize1)
	t.Log(readSize1)

	rec2 := &LogStruct{
		Key:   []byte("name"),
		Value: []byte("a new value"),
	}
	res2, size2 := Encoder(rec2)
	err = dataFile.Write(res2)
	assert.Nil(t, err)

	readRec2, readSize2, err := dataFile.ReadLog(size1)
	assert.Nil(t, err)
	assert.Equal(t, rec2, readRec2)
	assert.Equal(t, size2, readSize2)

	rec3 := &LogStruct{
		Key:   []byte("1"),
		Value: []byte(""),
		Type:  LogDeleted,
	}
	res3, size3 := Encoder(rec3)
	err = dataFile.Write(res3)
	assert.Nil(t, err)

	readRec3, readSize3, err := dataFile.ReadLog(size1 + size2)
	assert.Nil(t, err)
	assert.Equal(t, rec3, readRec3)
	assert.Equal(t, size3, readSize3)
}
