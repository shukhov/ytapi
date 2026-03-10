package mem_storage

import (
	"fmt"
	"github.com/klauspost/compress/zstd"
	"os"
)

type MemTableReader interface {
	ReadPartition(path string) (*[]byte, error)
}

type MemTableJsonReader struct {
	Table *Table
}

func (reader *MemTableJsonReader) ReadPartition(path string) (*[]byte, error) {
	zstdDecoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to load decoder, error: %v", err)
	}
	file, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file, error: %v", err)
	}
	encodedJson, err := zstdDecoder.DecodeAll(file, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decode json, error: %v", err)
	}
	return &encodedJson, nil
}

type MemTableParquetReader struct {
	Table *Table
}

func (reader *MemTableParquetReader) ReadPartition(name string) (*[]byte, error) {
	fmt.Println("implement me")
	return nil, nil
}
