package content

import (
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeRecord(t *testing.T) {
	rec1 := &LogStruct{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogNormal,
	}
	res1, n1 := Encoder(rec1)
	assert.NotNil(t, res1)
	assert.Greater(t, n1, int64(5))

	// test with empty value
	rec2 := &LogStruct{
		Key:  []byte("name"),
		Type: LogNormal,
	}
	res2, n2 := Encoder(rec2)
	assert.NotNil(t, res2)
	assert.Greater(t, n2, int64(5))

	// test with deleted log
	rec3 := &LogStruct{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogDeleted,
	}
	res3, n3 := Encoder(rec3)
	assert.NotNil(t, res3)
	assert.Greater(t, n3, int64(5))
}

func TestDecodeHeader(t *testing.T) {
	headerBuf1 := []byte{104, 82, 240, 150, 0, 8, 20}
	h1, size1 := DecodeHeader(headerBuf1)
	assert.NotNil(t, h1)
	assert.Equal(t, int64(7), size1)
	assert.Equal(t, uint32(2532332136), h1.crc)
	assert.Equal(t, LogNormal, h1.LogType)
	assert.Equal(t, uint32(4), h1.KeySize)
	assert.Equal(t, uint32(10), h1.ValueSize)

	headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	h2, size2 := DecodeHeader(headerBuf2)
	assert.NotNil(t, h2)
	assert.Equal(t, int64(7), size2)
	assert.Equal(t, uint32(240712713), h2.crc)
	assert.Equal(t, LogNormal, h2.LogType)
	assert.Equal(t, uint32(4), h2.KeySize)
	assert.Equal(t, uint32(0), h2.ValueSize)

	headerBuf3 := []byte{43, 153, 86, 17, 1, 8, 20}
	h3, size3 := DecodeHeader(headerBuf3)
	assert.NotNil(t, h3)
	assert.Equal(t, int64(7), size3)
	assert.Equal(t, uint32(290887979), h3.crc)
	assert.Equal(t, LogDeleted, h3.LogType)
	assert.Equal(t, uint32(4), h3.KeySize)
	assert.Equal(t, uint32(10), h3.ValueSize)
}

func TestGetCRC(t *testing.T) {
	rec1 := &LogStruct{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogNormal,
	}
	headerBuf1 := []byte{104, 82, 240, 150, 0, 8, 20}
	crc1 := getDataCRC(rec1, headerBuf1[crc32.Size:])
	assert.Equal(t, uint32(2532332136), crc1)

	rec2 := &LogStruct{
		Key:  []byte("name"),
		Type: LogNormal,
	}
	headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	crc2 := getDataCRC(rec2, headerBuf2[crc32.Size:])
	assert.Equal(t, uint32(240712713), crc2)

	rec3 := &LogStruct{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogDeleted,
	}
	headerBuf3 := []byte{43, 153, 86, 17, 1, 8, 20}
	crc3 := getDataCRC(rec3, headerBuf3[crc32.Size:])
	assert.Equal(t, uint32(290887979), crc3)
}
