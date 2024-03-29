package diskIO

import "os"

type SystemIO struct {
	fd *os.File
}

func NewFileIOManager(fileName string) (*SystemIO, error) {
	fd, err := os.OpenFile(
		fileName,
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		BlockFileMode,
	)

	if err != nil {
		return nil, err
	}
	return &SystemIO{fd: fd}, nil
}

func (s *SystemIO) Read(p []byte, off int64) (int, error) {
	return s.fd.ReadAt(p, off)
}

func (s *SystemIO) Write(p []byte) (int, error) {
	return s.fd.Write(p)
}

func (s *SystemIO) Sync() error {
	return s.fd.Sync()
}

func (s *SystemIO) Close() error {
	return s.fd.Close()
}

func (s *SystemIO) Size() (int64, error) {
	fi, err := s.fd.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}
