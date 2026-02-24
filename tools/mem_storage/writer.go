package mem_storage

import (
	"encoding/json"
	"fmt"
	"github.com/klauspost/compress/zstd"
	"github.com/siddontang/go-log/log"
	"os"
	"path/filepath"
	"time"
)

type MemTableWriter interface {
	WritePartition(content *[]map[string]interface{}) error
}

type MemTableJsonWriter struct {
	Table *Table
}

func generateMeta(tablePath string, contentHead map[string]interface{}, length uint64) (*Partition, error) {
	var fields = make([]string, 0)
	for key, _ := range contentHead {
		fields = append(fields, key)
	}

	name := fmt.Sprintf("partition_%d", time.Now().UnixNano())
	path := filepath.Join(tablePath, name)

	partition := Partition{
		Path:   path,
		Length: length,
		Fields: fields,
	}

	if err := os.MkdirAll(path, 0o777); err != nil {
		return nil, fmt.Errorf("failed to create partition dir: %w", err)
	}

	jsn, err := json.Marshal(partition)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize metadata of partition: %w", err)
	}

	metaPath := filepath.Join(path, "meta.json")
	if err := os.WriteFile(metaPath, jsn, 0o666); err != nil {
		return nil, fmt.Errorf("failed to write meta.json: %w", err)
	}

	return &partition, nil
}

func (writer *MemTableJsonWriter) WritePartition(content *[]map[string]interface{}) error {

	first := (*content)[0]

	partition, err := generateMeta(writer.Table.Path, first, uint64(len(*content)))
	if err != nil {
		return fmt.Errorf("error to create partition metainformation: %w", err)
	}

	jsn, err := json.Marshal(*content) // content — это []T или []map[string]any
	if err != nil {
		return fmt.Errorf("failed to serialize content: %w", err)
	}

	zstdEnc, err := zstd.NewWriter(nil)
	if err != nil {
		return fmt.Errorf("failed to initialize ZSTD encoder: %w", err)
	}
	compressed := zstdEnc.EncodeAll(jsn, nil)

	dataPath := filepath.Join(partition.Path, "data")
	file, err := os.Create(dataPath)
	if err != nil {
		return fmt.Errorf("error to create data file: %w", err)
	}
	if _, err := file.Write(compressed); err != nil {
		_ = file.Close()
		return fmt.Errorf("error to write data file: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("error to close data file: %w", err)
	}

	writer.Table.Partitions = append(writer.Table.Partitions, *partition)
	return nil
}

type MemTableParquetWriter struct {
	Table *Table
}

func (writer *MemTableParquetWriter) WritePartition(content *[]map[string]interface{}) error {
	log.Info("implement me")
	return nil
}
