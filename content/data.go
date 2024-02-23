package content

import (
	"tiny-bitcask/io"
)

type DataFile struct {
	FileIndex uint32
	IOManager io.IOManager
	WritePos  int64
}

func OpenFile(fileName string, fileIndex uint32) (*DataFile, error) {
	return nil, nil
}

func (d *DataFile) Write(p []byte) error {
	return nil
}

func (d *DataFile) ReadLog(offset int64) (*LogStruct, int64, error) {
	return nil, 0, nil
}

func (d *DataFile) Sync() error {
	return nil
}
