package content

import (
	"encoding/binary"
	"hash/crc32"
)

// Log-Like Append-Only File
type LogStruct struct {
	Key   []byte
	Value []byte
	Type  LogType
}

// LogStructIndex is a struct that holds the index of the log file on disk
type LogStructIndex struct {
	FileIndex uint32
	Offset    int64
}

// Header
type logHeader struct {
	KeySize   uint32
	ValueSize uint32
	LogType   LogType
	crc       uint32
}

//	+--------+-------+-----------+------------+-----------+------------+
//	| crc    |  type |  key size | value size |    key    |    value   |
//	+--------+-------+-----------+------------+-----------+------------+
//	 4 byte    1 byte  maxLen:5    maxLen:5     elastic     elastic
//
// return byte slice and length
func Encoder(log *LogStruct) (encodeData []byte, encodeLen int64) {
	headBuffer := make([]byte, MaxLogHeaderSize)

	headBuffer[4] = log.Type
	var index = 5

	index += binary.PutVarint(headBuffer[index:], int64(len(log.Key)))
	index += binary.PutVarint(headBuffer[index:], int64(len(log.Value)))

	var dataLen = index + len(log.Key) + len(log.Value)
	encodeBytes := make([]byte, dataLen)

	copy(encodeBytes[:index], headBuffer)
	copy(encodeBytes[index:], log.Key)
	copy(encodeBytes[index+len(log.Key):], log.Value)

	crc := crc32.ChecksumIEEE(encodeBytes[4:])
	binary.LittleEndian.PutUint32(encodeBytes[:4], crc)

	return encodeBytes, int64(dataLen)
}

// Decode headers
func DecodeHeader(data []byte) (*logHeader, int64) {
	if len(data) <= 4 {
		return nil, 0
	}

	header := &logHeader{
		crc:     binary.LittleEndian.Uint32(data[:4]),
		LogType: data[4],
	}

	var index = 5
	keySize, n := binary.Varint(data[index:])
	header.KeySize = uint32(keySize)
	index += n

	valueSize, n := binary.Varint(data[index:])
	header.ValueSize = uint32(valueSize)
	index += n

	return header, int64(index)
}

// crc
func getDataCRC(l *LogStruct, head []byte) uint32 {
	if l == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(head)
	crc = crc32.Update(crc, crc32.IEEETable, l.Key)
	crc = crc32.Update(crc, crc32.IEEETable, l.Value)

	return crc
}

// EncodeIndex
func EncodeIndex(indexer *LogStructIndex) []byte {
	buffer := make([]byte, binary.MaxVarintLen32+binary.MaxVarintLen64)
	var cnt = 0
	cnt += binary.PutVarint(buffer[cnt:], int64(indexer.FileIndex))
	cnt += binary.PutVarint(buffer[cnt:], indexer.Offset)
	return buffer[:cnt]
}

// DecodeIndex
func DecodeIndex(buffer []byte) *LogStructIndex {
	indexer := &LogStructIndex{}
	fileIndex, n := binary.Varint(buffer)
	indexer.FileIndex = uint32(fileIndex)
	indexer.Offset, _ = binary.Varint(buffer[n:])
	return indexer
}
