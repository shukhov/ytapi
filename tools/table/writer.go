package table

import (
	"context"
	"github.com/shukhov/ytapi/client"
	"go.ytsaurus.tech/yt/go/ypath"
)

type Writer struct {
	Path      ypath.Path
	Ctx       *context.Context
	Client    *client.Client
	BatchSize uint64
}

func NewWriter(client *client.Client, path string, context *context.Context) (*Writer, error) {
	return &Writer{
		Path:      ypath.Path(path),
		Client:    client,
		Ctx:       context,
		BatchSize: GetBatchSize(),
	}, nil
}

//func (writer *Writer) Write() {
//	writer.Client.Client.WriteTable()
//}/
