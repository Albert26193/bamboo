package content

type LogType byte

const (
	LogStructCnt LogType = iota
	LogStructDeleted
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

// Encode encodes the log struct into a byte slice
func Encoder(log *LogStruct) (encodeData []byte, encodeLen int64) {
	return nil, 0
}
