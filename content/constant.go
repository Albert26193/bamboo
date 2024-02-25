package content

import (
	"encoding/binary"
	"errors"
)

type LogType = byte

const (
	suffix                   = ".btdata"
	MaxLogHeaderSize int64   = binary.MaxVarintLen32*2 + 5
	LogNormal        LogType = 0
	LogDeleted       LogType = 1
	LogAtomicFinish  LogType = 2
)

var ErrCRCNotMatch = errors.New("crc not match")
