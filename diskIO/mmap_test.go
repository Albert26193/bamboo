package diskIO

import (
	"io"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMMapRead(t *testing.T) {
	path := filepath.Join("/tmp", "mmap-temp.data")

	defer destroyFile(path)

	mmapIO, err := NewMMapIOManager(path)
	assert.Nil(t, err)

	b1 := make([]byte, 10)
	n1, err := mmapIO.Read(b1, 0)
	assert.Equal(t, 0, n1)
	assert.Equal(t, io.EOF, err)

	fio, err := NewFileIOManager(path)
	assert.Nil(t, err)
	_, err = fio.Write([]byte("fffff"))
	assert.Nil(t, err)
	_, err = fio.Write([]byte("ggggg"))
	assert.Nil(t, err)
	_, err = fio.Write([]byte("hhhhh"))
	assert.Nil(t, err)

	mmapIO2, err := NewMMapIOManager(path)
	assert.Nil(t, err)
	size, err := mmapIO2.Size()
	assert.Nil(t, err)
	assert.Equal(t, int64(15), size)

	b2 := make([]byte, 2)
	n2, err := mmapIO2.Read(b2, 0)
	assert.Nil(t, err)
	assert.Equal(t, 2, n2)
}
