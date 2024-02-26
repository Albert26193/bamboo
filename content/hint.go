package content

import "path/filepath"

func GenerateNewHintBlock(fileName string) (*DataFile, error) {
	name := filepath.Join(fileName, HintFileTag)
	return GenerateNewBlock(name, 0)
}

func GenerateMergeFinishedBlock(fileName string) (*DataFile, error) {
	name := filepath.Join(fileName, MergeFinishedTag)
	return GenerateNewBlock(name, 0)
}

func (d *DataFile) WriteToHintBlock(key []byte, indexer *LogStructIndex) error {
	log := &LogStruct{
		Key:   key,
		Value: EncodeIndex(indexer),
	}

	encodedLog, _ := Encoder(log)
	return d.Write(encodedLog)
}
