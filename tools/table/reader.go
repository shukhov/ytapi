package table

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/shukhov/ytapi/client"
	"github.com/shukhov/ytapi/tools/mem_storage"
	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/skiff"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"log"
	"sync"
)

type Reader struct {
	Path      ypath.Path
	Ctx       *context.Context
	Client    *client.Client
	Schema    schema.Schema
	RowCount  uint64
	BatchSize uint64
}

func NewReader(client *client.Client, path string, context *context.Context) (*Reader, error) {
	var rowCount uint64
	var schema schema.Schema
	err := client.Client.GetNode(*context, ypath.Path(path).Child("@row_count"), &rowCount, nil)
	if err != nil {
		return nil, err
	}
	err = client.Client.GetNode(*context, ypath.Path(path).Attr("schema"), &schema, nil)
	if err != nil {
		return nil, err
	}
	var reader = Reader{
		Client:   client,
		Ctx:      context,
		Path:     ypath.Path(path),
		RowCount: rowCount,
		Schema:   schema,
	}
	return &reader, nil
}

func (reader *Reader) ReadPartition(offset, limit uint64) (*[]map[string]interface{}, error) {
	// т.е. [offset:limit)
	if limit < offset {
		limit = offset
	}

	var partition = make([]map[string]interface{}, 0, limit-offset)

	currentPart := fmt.Sprintf("[#%d:#%d]", offset, limit)
	partitionPath := ypath.Path(reader.Path.String() + currentPart)

	rs, err := reader.Client.Client.ReadTable(*reader.Ctx, partitionPath, &yt.ReadTableOptions{
		Unordered: true,
		Smart:     ptr.Bool(true),
	})
	if err != nil {
		return nil, err
	}
	defer rs.Close()

	for rs.Next() {
		var row map[string]interface{}
		if err := rs.Scan(&row); err != nil {
			fmt.Println(err)
			return nil, err
		}
		partition = append(partition, row)
	}
	if err := rs.Err(); err != nil {
		return nil, err
	}
	fmt.Println(partitionPath)
	return &partition, nil
}

func (reader *Reader) ReadPartitionJson(limit, offset uint64) RowJson {
	currentPart := fmt.Sprintf("[#%d:#%d]", limit, offset)
	partitionPath := ypath.Path(reader.Path.String() + currentPart)
	tableSchema := skiff.FromTableSchema(reader.Schema)
	readerSession, err := reader.Client.Client.ReadTable(*reader.Ctx, partitionPath, &yt.ReadTableOptions{Format: skiff.Format{
		Name:         "skiff",
		TableSchemas: []any{&tableSchema},
	}})
	if err != nil {
		log.Fatalf("Error reading table: %v", err)
		return nil
	}
	defer readerSession.Close()

	var buffer bytes.Buffer
	first := true
	var i = 0
	for readerSession.Next() {
		var row Row
		if err := readerSession.Scan(&row); err != nil {
			log.Printf("Error scanning row: %v", err)
			continue
		}

		marshalled, err := json.Marshal(row)
		if err != nil {
			log.Printf("Error marshalling row: %v", err)
			continue
		}

		if !first {
			buffer.WriteByte(',')
		} else {
			first = false
		}

		buffer.Write(marshalled)
		i++
	}
	fmt.Println("count: ", i)
	return buffer.Bytes()
}

func (reader *Reader) ReadTableJson(tableName string) (*mem_storage.Table, error) {
	newTable, err := mem_storage.NewTable(tableName, "json")
	if err != nil {
		return nil, err
	}

	w, err := newTable.NewWriter()
	if err != nil {
		newTable.Drop()
		return nil, fmt.Errorf("failed to create new writer: %w", err)
	}

	workerCount := GetWorkerPool()
	batchSize := GetBatchSize()
	rowCount := reader.RowCount

	sem := make(chan struct{}, workerCount)
	errCh := make(chan error, 1) // Initialize the channel

	var wg sync.WaitGroup

	for start := uint64(0); start < rowCount; start += batchSize {
		end := start + batchSize
		if end > rowCount {
			end = rowCount
		}

		sem <- struct{}{}
		wg.Add(1)

		go func(start, end uint64) {
			defer wg.Done()
			defer func() { <-sem }()

			part, err := reader.ReadPartition(start, end)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}

			if err := w.WritePartition(part); err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
		}(start, end)
	}

	wg.Wait()
	close(errCh)

	if err := <-errCh; err != nil { // This will block if no errors occurred
		func() { _ = newTable.Drop() }()
		return nil, fmt.Errorf("failed to write partition: %+v", err)
	}

	return newTable, nil
}

/*func (reader *Reader) ReadTable() (*[]Row, error) {
	var table = make([]Row, 0, reader.RowCount)

	var wg sync.WaitGroup
	var mu sync.Mutex

	for i := 0; i <= int(reader.RowCount); i += BatchSize {
		wg.Add(1)
		go func() {
			defer wg.Done()
			part := reader.ReadPartition(uint64(i), uint64(i+BatchSize-1))
			mu.Lock()
			table = append(table, *part...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	runtime.GC()
	return &table, nil
}

func (reader *Reader) ReadTable() (reflect.Value, error) {
	rowCount := reader.RowCount
	batchSize := GetBatchSize()

	sliceType := reflect.SliceOf(reader.rowType)
	table := reflect.MakeSlice(sliceType, 0, int(rowCount))

	var wg sync.WaitGroup
	var mu sync.Mutex

	workerCount := GetWorkerPool()
	sem := make(chan struct{}, workerCount)

	errCh := make(chan error, workerCount)

	for start := uint64(0); start < rowCount; start += batchSize {
		end := start + batchSize
		if end > rowCount {
			end = rowCount
		}

		sem <- struct{}{}
		wg.Add(1)

		go func(start, end uint64) {
			defer wg.Done()
			defer func() { <-sem }()

			part, err := reader.ReadPartition(start, end)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}

			mu.Lock()
			table = reflect.AppendSlice(table, part)
			mu.Unlock()
		}(start, end)
	}

	wg.Wait()
	close(errCh)

	if err := <-errCh; err != nil {
		return reflect.Value{}, err
	}
	return table, nil
}*/
