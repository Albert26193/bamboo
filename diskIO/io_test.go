package diskIO

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func destroyFile(path string) {
	if err := os.Remove(path); err != nil {
		panic(err)
	}
}

func TestNewIOManager(t *testing.T) {
	path := filepath.Join("/tmp", "bitcask_a.data")
	io, err := NewIOManager(path)
	if err != nil {
		panic(err)
	}

	defer destroyFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, io)
}

func TestSystemIOWrite(t *testing.T) {
	path := filepath.Join("/tmp", "bitcask_a.data")
	io, err := NewIOManager(path)
	if err != nil {
		panic(err)
	}

	defer destroyFile(path)

	bufLen1, err := io.Write([]byte{})
	assert.Nil(t, err)
	assert.Equal(t, 0, bufLen1)

	bufLen2, err := io.Write([]byte("test"))
	assert.Nil(t, err)
	assert.Equal(t, 4, bufLen2)

	bufLen3, err := io.Write([]byte("test demo"))
	assert.Nil(t, err)
	assert.Equal(t, 9, bufLen3)
}

func TestSystemIORead(t *testing.T) {
	path := filepath.Join("/tmp", "bitcask_a.data")
	io, err := NewIOManager(path)
	if err != nil {
		panic(err)
	}

	defer destroyFile(path)

	_, err = io.Write([]byte("test"))
	assert.Nil(t, err)

	buf := make([]byte, 4)
	n, err := io.Read(buf, 0)
	assert.Nil(t, err)
	assert.Equal(t, 4, n)
	assert.Equal(t, []byte("test"), buf)
}

func TestSystemIOSync(t *testing.T) {
	path := filepath.Join("/tmp", "bitcask_a.data")
	io, err := NewIOManager(path)
	if err != nil {
		panic(err)
	}

	defer destroyFile(path)
	assert.Nil(t, io.Sync())
}

func TestSystemIOClose(t *testing.T) {
	path := filepath.Join("/tmp", "bitcask_a.data")
	io, err := NewIOManager(path)
	if err != nil {
		panic(err)
	}

	assert.Nil(t, io.Close())
}
