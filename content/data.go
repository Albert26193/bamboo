package content

import (
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
	"tiny-bitcask/diskIO"
)

type DataFile struct {
	FileIndex uint32
	IOManager diskIO.IOManager
	WritePos  int64
}

func GetBlockName(dir string, fileId uint32) string {
	return filepath.Join(dir, fmt.Sprintf("%09d", fileId)+suffix)
}

func GenerateNewBlock(fileName string, fileIndex uint32) (*DataFile, error) {
	fio, err := diskIO.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}

	return &DataFile{
		FileIndex: fileIndex,
		IOManager: fio,
		WritePos:  0,
	}, nil
}

func OpenBlock(path string, fileIndex uint32) (*DataFile, error) {
	name := GetBlockName(path, fileIndex)
	return GenerateNewBlock(name, fileIndex)
}

func (d *DataFile) Write(p []byte) error {
	n, err := d.IOManager.Write(p)
	if err != nil {
		return err
	}

	d.WritePos += int64(n)
	return nil
}

func (d *DataFile) ReadBytes(offset int64, readLen int64) ([]byte, error) {
	toRead := make([]byte, readLen)
	_, err := d.IOManager.Read(toRead, offset)
	return toRead, err
}

func (d *DataFile) ReadLog(offset int64) (*LogStruct, int64, error) {
	fileSize, err := d.IOManager.Size()
	if err != nil {
		return nil, 0, err
	}

	headBytes := MaxLogHeaderSize
	if offset+MaxLogHeaderSize > fileSize {
		headBytes = fileSize - offset
	}

	// read log header
	headBuffer, err := d.ReadBytes(offset, headBytes)
	if err != nil {
		return nil, 0, err
	}

	headInfo, headSize := DecodeHeader(headBuffer)
	// EOF
	if headInfo == nil {
		return nil, 0, io.EOF
	}
	if headInfo.crc == 0 && headInfo.KeySize == 0 && headInfo.ValueSize == 0 {
		return nil, 0, io.EOF
	}

	// key and value
	keySize, valueSize := int64(headInfo.KeySize), int64(headInfo.ValueSize)

	logData := &LogStruct{
		Type: headInfo.LogType,
	}
	if keySize > 0 || valueSize > 0 {
		kvData, err := d.ReadBytes(offset+headSize, keySize+valueSize)
		if err != nil {
			return nil, 0, err
		}
		logData.Key = kvData[:keySize]
		logData.Value = kvData[keySize:]
	}

	var totalSize = headSize + keySize + valueSize
	crc := getDataCRC(logData, headBuffer[crc32.Size:headSize])
	if crc != headInfo.crc {
		return nil, 0, ErrCRCNotMatch
	}

	return logData, totalSize, nil
}

func (d *DataFile) Sync() error {
	return d.IOManager.Sync()
}

func (d *DataFile) Close() error {
	return d.IOManager.Close()
}
