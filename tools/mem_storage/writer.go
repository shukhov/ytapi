package mem_storage

import (
	"encoding/json"
	"fmt"
	"github.com/klauspost/compress/zstd"
	"github.com/siddontang/go-log/log"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"
)

type MemTableWriter interface {
	WritePartition(content any) error
}

type MemTableJsonWriter struct {
	Table *Table
}

func generateMeta(tablePath string, contentHead any, length uint64) (*Partition, error) {
	fields, err := extractFieldNames(contentHead)
	if err != nil {
		return nil, err
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

func extractFieldNames(v any) ([]string, error) {
	if v == nil {
		return nil, fmt.Errorf("contentHead is nil")
	}

	rv := reflect.ValueOf(v)
	rt := rv.Type()

	// разыменуем указатели
	for rt.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil, fmt.Errorf("contentHead is nil pointer")
		}
		rv = rv.Elem()
		rt = rv.Type()
	}

	switch rt.Kind() {
	case reflect.Map:
		// ожидаем map[string]any (или совместимое)
		if rt.Key().Kind() != reflect.String {
			return nil, fmt.Errorf("map key must be string, got %s", rt.Key())
		}
		keys := rv.MapKeys()
		out := make([]string, 0, len(keys))
		for _, k := range keys {
			out = append(out, k.String())
		}
		return out, nil

	case reflect.Struct:
		out := make([]string, 0, rt.NumField())
		for i := 0; i < rt.NumField(); i++ {
			f := rt.Field(i)

			// берём имя из yson-тега, если есть, иначе имя поля
			tag := f.Tag.Get("yson")
			if tag != "" && tag != "-" {
				// yson:"name,omitempty" -> "name"
				if idx := strings.IndexByte(tag, ','); idx >= 0 {
					tag = tag[:idx]
				}
				if tag != "" {
					out = append(out, tag)
					continue
				}
			}
			out = append(out, f.Name)
		}
		return out, nil

	default:
		return nil, fmt.Errorf("unsupported contentHead kind: %s", rt.Kind())
	}
}

func (writer *MemTableJsonWriter) WritePartition(content any) error {
	v := reflect.ValueOf(content)
	if !v.IsValid() || v.Kind() != reflect.Slice {
		return fmt.Errorf("content must be slice, got %T", content)
	}
	if v.Len() == 0 {
		return nil // нечего писать
	}

	// Для meta берём первый элемент как map/struct/что угодно — generateMeta должен это уметь.
	first := v.Index(0).Interface()

	partition, err := generateMeta(writer.Table.Path, first, uint64(v.Len()))
	if err != nil {
		return fmt.Errorf("error to create partition metainformation: %w", err)
	}

	jsn, err := json.Marshal(content) // content — это []T или []map[string]any
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

func (writer *MemTableParquetWriter) WritePartition(content any) error {
	log.Info("implement me")
	return nil
}
