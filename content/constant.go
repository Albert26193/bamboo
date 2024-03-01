package content

import (
	"encoding/binary"
	"errors"
)

type LogType = byte

const (
	Suffix                   = ".btdata"
	MaxLogHeaderSize int64   = binary.MaxVarintLen32*2 + 5
	LogNormal        LogType = 0
	LogDeleted       LogType = 1
	LogAtomicFinish  LogType = 2

	HintFileTag      = "bamboo-hint"
	MergeFinishedTag = "MERGE.FINISHED"
)

var ErrCRCNotMatch = errors.New("crc not match")
