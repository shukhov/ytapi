package table

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"go.ytsaurus.tech/yt/go/ypath"
	"log"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"ytapi/client"
)

var BatchSize = 1000000

type Reader struct {
	Path      ypath.Path
	Ctx       *context.Context
	Client    *client.Client
	RowCount  uint64
	BatchSize uint64
}

type Row = map[string]interface{}

type RowJson = []byte

func NewReader(client *client.Client, path string, context *context.Context) (*Reader, error) {
	var rowCount uint64
	err := client.Client.GetNode(*context, ypath.Path(path).Child("@row_count"), &rowCount, nil)
	if err != nil {
		return nil, err
	}
	reader := &Reader{
		Client:   client,
		Ctx:      context,
		Path:     ypath.Path(path),
		RowCount: rowCount,
	}
	return reader, nil
}

func upperFirst(s string) string {
	if s == "" {
		return ""
	}
	return string(s[0]-32) + s[1:]
}

func (reader *Reader) InferType() reflect.Value {
	var r, _ = reader.Client.Client.ReadTable(*reader.Ctx, reader.Path, nil)
	defer r.Close()
	var abstractRow map[string]interface{}
	r.Next()
	err := r.Scan(&abstractRow)
	if err != nil {
		log.Fatal(err)
	}
	fields := make([]reflect.StructField, 0, len(abstractRow))
	fmt.Println(abstractRow)
	for key, value := range abstractRow {
		fields = append(fields, reflect.StructField{
			Name: upperFirst(key),
			Type: reflect.TypeOf(value),
			Tag:  reflect.StructTag(fmt.Sprintf(`yson:"%s"`, strings.ToLower(key))),
		})
	}
	typ := reflect.StructOf(fields)
	v := reflect.New(typ).Elem()
	return v
}

func (reader *Reader) ReadPartition(offset, limit uint64) *[]Row {
	var partition = make([]Row, 0, limit-offset)
	currentPart := fmt.Sprintf("[#%d:#%d]", offset, limit)
	partitionPath := ypath.Path(reader.Path.String() + currentPart)
	log.Printf(string(partitionPath))
	readerSession, err := reader.Client.Client.ReadTable(*reader.Ctx, partitionPath, nil)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	defer readerSession.Close()
	for readerSession.Next() {
		var row Row
		readerSession.Scan(&row)
		partition = append(partition, row)
	}
	runtime.GC()
	return &partition
}

func (reader *Reader) ReadPartitionJson(limit, offset uint64) RowJson {
	currentPart := fmt.Sprintf("[#%d:#%d]", limit, offset)
	partitionPath := ypath.Path(reader.Path.String() + currentPart)
	log.Println(string(partitionPath))

	readerSession, err := reader.Client.Client.ReadTable(*reader.Ctx, partitionPath, nil)
	if err != nil {
		log.Printf("Error reading table: %v", err)
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

func (reader *Reader) ReadTableJson() *RowJson {
	var parts = make([]RowJson, 0)
	var buffer bytes.Buffer
	var wg sync.WaitGroup
	var mu sync.Mutex

	buffer.Write([]byte("["))
	for i := 0; i <= int(reader.RowCount); i += BatchSize {
		wg.Add(1)
		go func() {
			defer wg.Done()
			part := reader.ReadPartitionJson(uint64(i), uint64(i+BatchSize-1))
			mu.Lock()
			parts = append(parts, part)
			mu.Unlock()
		}()
	}
	wg.Wait()
	buffer.Write(bytes.Join(parts, []byte(", ")))
	buffer.Write([]byte("]"))
	result := buffer.Bytes()
	runtime.GC()
	return &result

}

func (reader *Reader) ReadTable() (*[]Row, error) {
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
