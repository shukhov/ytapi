package mem_storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type Partition struct {
	Path   string   `json:"path"`
	Length uint64   `json:"length"`
	Fields []string `json:"fields"`
}

type Table struct {
	Name       string      `json:"name"`
	Path       string      `json:"path"`
	Partitions []Partition `json:"-"`
	Format     string      `json:"format"`
}

func (partition *Partition) DataPath() string {
	return filepath.Join(partition.Path, "data")
}

func createTmpIfNoExists() error {
	wd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("error: %v", err)
	}
	fullPath := filepath.Join(wd, "tmp")
	if _, err := os.Stat(fullPath); os.IsNotExist(err) {
		if err := os.Mkdir(fullPath, 0777); err != nil {
			return fmt.Errorf("failed to create tmp directory, error: %v", err)
		}
	}
	return nil
}

func NewTable(Name string, Format string) (*Table, error) {
	if err := createTmpIfNoExists(); err != nil {
		return nil, err
	}
	wd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	fullPath := filepath.Join(wd, "tmp", Name)
	if _, err := os.Stat(fullPath); !os.IsNotExist(err) {
		return nil, fmt.Errorf("directory %s already exists", fullPath)
	}

	if err := os.Mkdir(fullPath, 0777); err != nil {
		return nil, fmt.Errorf("failed to create: %s, error: %v", fullPath, err)
	}
	table := &Table{Name: Name,
		Path:       fullPath,
		Format:     Format,
		Partitions: make([]Partition, 0),
	}
	file, err := os.Create(filepath.Join(fullPath, "meta.json"))

	if err != nil {
		return nil, fmt.Errorf("failed when trying create file meta.json, error: %v", err)
	}
	defer func() { _ = file.Close() }()

	jsn, err := json.Marshal(table)

	if _, err = file.Write(jsn); err != nil {
		return nil, fmt.Errorf("failed when trying to write meta.json, error: %v", err)
	}
	return table, nil
}

func ConnectTable(name string) (*Table, error) {
	wd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	fullPath := filepath.Join(wd, "tmp", name)

	files, err := os.ReadDir(fullPath)

	meta, err := os.ReadFile(filepath.Join(fullPath, "meta.json"))

	if err != nil {
		return nil, fmt.Errorf("failed when trying to connect to table, error: %v", err)
	}

	table := new(Table)

	if err := json.Unmarshal(meta, &table); err != nil {
		return nil, fmt.Errorf("failed to unmarshall meta.json, error: %v", err)
	}

	for _, file := range files {
		if file.Name() != "meta.json" {
			partitionPath := filepath.Join(fullPath, file.Name())
			table.Partitions = append(table.Partitions, Partition{Path: partitionPath})
		}
	}
	return table, nil
}

func (table *Table) NewWriter() (MemTableWriter, error) {
	switch strings.ToLower(table.Format) {
	case "parquet":
		return &MemTableParquetWriter{Table: table}, nil
	case "json":
		return &MemTableJsonWriter{Table: table}, nil
	default:
		return nil, fmt.Errorf("invalid format '%s' of data", table.Format)
	}
}

func (table *Table) NewReader() (MemTableReader, error) {
	switch strings.ToLower(table.Format) {
	case "parquet":
		return &MemTableParquetReader{Table: table}, nil
	case "json":
		return &MemTableJsonReader{Table: table}, nil
	default:
		return nil, fmt.Errorf("invalid format '%s' of data", table.Format)
	}
}

func (table *Table) Drop() error {
	if err := os.RemoveAll(table.Path); err != nil {
		return fmt.Errorf("failed to drop table, error: %v", err)
	}
	return nil
}

func (partition *Partition) Drop() error {
	if err := os.RemoveAll(partition.Path); err != nil {
		return fmt.Errorf("failed to drop partition, error: %v", err)
	}
	return nil
}

func (table *Table) Refresh() error {
	updatedTable, err := ConnectTable(table.Name)
	if err != nil {
		return err
	}

	// Копируем данные из обновленной таблицы в текущую
	table.Path = updatedTable.Path
	table.Format = updatedTable.Format
	table.Partitions = updatedTable.Partitions

	return nil
}
