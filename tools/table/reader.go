package table

import (
	"context"
	"fmt"
	"github.com/shukhov/ytapi/client"
	"github.com/shukhov/ytapi/tools/mem_storage"
	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"reflect"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
)

type Reader struct {
	Path        ypath.Path
	Ctx         *context.Context
	Client      *client.Client
	Schema      schema.Schema
	RowCount    uint64
	BatchSize   uint64
	tableSchema reflect.Type
}

func NewReader(client *client.Client, path string, context *context.Context) (*Reader, error) {
	var rowCount uint64
	var tableSchema schema.Schema
	err := client.Client.GetNode(*context, ypath.Path(path).Child("@row_count"), &rowCount, nil)
	if err != nil {
		return nil, err
	}
	err = client.Client.GetNode(*context, ypath.Path(path).Attr("schema"), &tableSchema, nil)
	if err != nil {
		return nil, err
	}
	var reader = Reader{
		Client:   client,
		Ctx:      context,
		Path:     ypath.Path(path),
		RowCount: rowCount,
		Schema:   tableSchema,
	}
	if err = reader.InferSchema(); err != nil {
		return nil, err
	}
	return &reader, nil
}

func ReflectType(t schema.Type) (reflect.Type, error) {
	switch t {
	case schema.TypeInt64:
		return reflect.TypeOf(int64(0)), nil
	case schema.TypeInt32:
		return reflect.TypeOf(int32(0)), nil
	case schema.TypeInt16:
		return reflect.TypeOf(int16(0)), nil
	case schema.TypeInt8:
		return reflect.TypeOf(int8(0)), nil

	case schema.TypeUint64:
		return reflect.TypeOf(uint64(0)), nil
	case schema.TypeUint32:
		return reflect.TypeOf(uint32(0)), nil
	case schema.TypeUint16:
		return reflect.TypeOf(uint16(0)), nil
	case schema.TypeUint8:
		return reflect.TypeOf(uint8(0)), nil

	case schema.TypeFloat32:
		return reflect.TypeOf(float32(0)), nil
	case schema.TypeFloat64:
		return reflect.TypeOf(float64(0)), nil

	case schema.TypeBytes:
		return reflect.TypeOf("а"), nil
	case schema.TypeString:
		return reflect.TypeOf("а"), nil

	case schema.TypeBoolean:
		return reflect.TypeOf(false), nil

	case schema.TypeAny, schema.TypeNull:
		return reflect.TypeOf((*any)(nil)).Elem(), nil

	case schema.TypeDate, schema.TypeDatetime, schema.TypeTimestamp:
		return reflect.TypeOf(time.Millisecond), nil
	case schema.TypeInterval:
		return reflect.TypeOf(time.Duration(0)), nil

	default:
		return nil, fmt.Errorf("unknown type: %q", t)
	}
}

// делает Exported Go identifier из snake_case / произвольной строки.
func exportFieldName(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "Field"
	}

	parts := strings.FieldsFunc(s, func(r rune) bool {
		return r == '_' || r == '-' || r == ' ' || r == '.'
	})

	var b strings.Builder
	for _, p := range parts {
		if p == "" {
			continue
		}
		r, size := utf8.DecodeRuneInString(p)
		if r == utf8.RuneError && size == 0 {
			continue
		}
		b.WriteRune(unicode.ToUpper(r))
		b.WriteString(p[size:])
	}

	out := b.String()
	if out == "" {
		return "Field"
	}

	r, _ := utf8.DecodeRuneInString(out)
	if !unicode.IsLetter(r) && r != '_' {
		out = "Field" + out
	}
	return out
}

func (reader *Reader) InferSchema() error {
	fields := make([]reflect.StructField, 0, len(reader.Schema.Columns))

	for _, col := range reader.Schema.Columns {
		typ, err := ReflectType(col.Type)
		if err != nil {
			return err
		}
		if !col.Required {
			typ = reflect.PointerTo(typ)
		}
		fmt.Println(col.Name, col.Type, typ, col.Required)

		fields = append(fields, reflect.StructField{
			Name: exportFieldName(col.Name), // <-- важно: экспортируемое имя
			Type: typ,
			Tag:  reflect.StructTag(fmt.Sprintf(`yson:"%s" json:"%s"`, col.Name, col.Name))})
	}

	st := reflect.StructOf(fields)
	v := reflect.New(st).Elem()
	reader.tableSchema = v.Type()

	return nil
}

func (reader *Reader) ReadPartition(offset, limit uint64) (reflect.Value, error) {
	// т.е. [offset:limit)
	if limit < offset {
		limit = offset
	}

	sliceType := reflect.SliceOf(reader.tableSchema)
	partition := reflect.MakeSlice(sliceType, 0, int(limit-offset))

	currentPart := fmt.Sprintf("[#%d:#%d]", offset, limit)
	partitionPath := ypath.Path(reader.Path.String() + currentPart)

	rs, err := reader.Client.Client.ReadTable(*reader.Ctx, partitionPath, &yt.ReadTableOptions{
		Unordered: true,
		Smart:     ptr.Bool(true),
	})
	if err != nil {
		return reflect.Value{}, err
	}
	defer func() { _ = rs.Close() }()

	for rs.Next() {
		rowPtr := reflect.New(reader.tableSchema) // *T
		if err := rs.Scan(rowPtr.Interface()); err != nil {
			fmt.Println(err)
			return reflect.Value{}, err
		}
		partition = reflect.Append(partition, rowPtr.Elem()) // append T
	}
	if err := rs.Err(); err != nil {
		return reflect.Value{}, err
	}
	fmt.Println(partitionPath)
	return partition, nil
}

func (reader *Reader) ReadTableJson(tableName string) (*mem_storage.Table, error) {
	newTable, err := mem_storage.NewTable(tableName, "json")
	if err != nil {
		return nil, err
	}

	writer, err := newTable.NewWriter()
	if err != nil {
		func() { _ = newTable.Drop() }()
		return nil, fmt.Errorf("failed to create new writer: %w", err)
	}

	workerCount := GetWorkerPool()
	batchSize := GetBatchSize()
	rowCount := reader.RowCount

	sem := make(chan struct{}, workerCount)

	errCh := make(chan error, workerCount)

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

			if err := writer.WritePartition(part.Interface()); err != nil {
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

	if err := <-errCh; err != nil {
		func() { _ = newTable.Drop() }()
		return nil, fmt.Errorf("failed to write partition: %+v", err)
	}

	return newTable, nil
}

func (reader *Reader) ReadTable() (reflect.Value, error) {
	rowCount := reader.RowCount
	batchSize := GetBatchSize()

	sliceType := reflect.SliceOf(reader.tableSchema)
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
}
