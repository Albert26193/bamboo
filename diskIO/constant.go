package diskIO

const BlockFileMode = 0644

type IOType = byte

const (
	FileSystemIO IOType = 0
	MMapIO       IOType = 1
)
