package diskIO

type IOManager interface {
	Read([]byte, int64) (int, error)

	Write([]byte) (int, error)

	Sync() error

	Close() error

	Size() (int64, error)
}

func NewIOManager(fileName string, ioType IOType) (IOManager, error) {
	switch ioType {
	case MMapIO:
		return NewMMapIOManager(fileName)
	case FileSystemIO:
		return NewFileIOManager(fileName)
	default:
		panic("not support io type")
	}
}
