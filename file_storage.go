package common

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
)

const valueSizeInBytes = 8 // 64-bit value

type FileStorage struct {
	filename string
	file     *os.File
}

func NewFileStorage(name string) *FileStorage {
	return &FileStorage{
		filename: name,
	}
}

func (ptr *FileStorage) Start() error {
	if ptr.file != nil {
		return errors.New("file descriptor is not nil")
	}

	file, err := os.OpenFile(ptr.filename, os.O_RDWR, 0644)
	if err != nil {
		if file, err = os.Create(ptr.filename); err != nil {
			return err
		}
	}

	ptr.file = file
	return nil
}

func (ptr *FileStorage) Stop() error {
	if ptr.file == nil {
		return nil
	}

	err := ptr.file.Close()
	ptr.file = nil
	return err
}

func (ptr *FileStorage) SetValue(value, offset int64) error {
	if ptr.file == nil {
		return errors.New("file is not open")
	}

	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, value); err != nil {
		return err
	}

	seekOffset, err := ptr.file.Seek(offset*valueSizeInBytes, os.SEEK_SET)
	if err != nil {
		return err
	}

	if _, err := ptr.file.WriteAt(buf.Bytes(), seekOffset); err != nil {
		return err
	}

	return nil
}

func (ptr *FileStorage) GetValue(offset int64) (int64, error) {

	if ptr.file == nil {
		return 0, errors.New("file is not open")
	}

	seekOffset, err := ptr.file.Seek(offset*valueSizeInBytes, os.SEEK_SET)
	if err != nil {
		return 0, err
	}

	buf := make([]byte, 8)
	if _, err := ptr.file.ReadAt(buf, seekOffset); err != nil {
		return 0, err
	}

	value := int64(binary.LittleEndian.Uint64(buf))

	return value, nil
}

func (ptr *FileStorage) CleanStorage() error {
	if ptr.file == nil {
		return errors.New("file is not open")
	}

	return ptr.file.Truncate(0)
}
