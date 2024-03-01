package content

import (
	"bamboo/diskIO"
	"path/filepath"
)

func GenerateNewHintBlock(fileName string) (*BlockFile, error) {
	name := filepath.Join(fileName, HintFileTag)
	return GenerateNewBlock(name, 0, diskIO.FileSystemIO)
}

func GenerateMergeFinishedBlock(fileName string) (*BlockFile, error) {
	name := filepath.Join(fileName, MergeFinishedTag)
	return GenerateNewBlock(name, 0, diskIO.FileSystemIO)
}

func (d *BlockFile) WriteToHintBlock(key []byte, indexer *LogStructIndex) error {
	log := &LogStruct{
		Key:   key,
		Value: EncodeIndex(indexer),
	}

	encodedLog, _ := Encoder(log)
	return d.Write(encodedLog)
}
