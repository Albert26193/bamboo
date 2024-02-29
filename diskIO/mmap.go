package diskIO

import (
	"os"

	"golang.org/x/exp/mmap"
)

// MMap
type MMap struct {
	readerPos *mmap.ReaderAt
}

// NewMMapIOManager 初始化 MMap IO
func NewMMapIOManager(fileName string) (*MMap, error) {
	_, err := os.OpenFile(fileName, os.O_CREATE, BlockFileMode)
	if err != nil {
		return nil, err
	}

	readerPos, err := mmap.Open(fileName)
	if err != nil {
		return nil, err
	}
	return &MMap{readerPos: readerPos}, nil
}

func (m *MMap) Read(buf []byte, offset int64) (int, error) {
	return m.readerPos.ReadAt(buf, offset)
}

func (m *MMap) Close() error {
	return m.readerPos.Close()
}

func (m *MMap) Size() (int64, error) {
	return int64(m.readerPos.Len()), nil
}

func (m *MMap) Sync() error {
	panic("not support Sync with MMap")
}

func (m *MMap) Write([]byte) (int, error) {
	panic("not support write with MMap")
}
