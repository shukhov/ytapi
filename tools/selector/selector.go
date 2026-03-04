package selector

import (
	"context"
	"fmt"
	"github.com/shukhov/ytapi/client"
	"github.com/shukhov/ytapi/tools/table"
	"go.ytsaurus.tech/yt/go/yt"
	"log"
	"math/rand/v2"
	"time"
)

var (
	TempDir  = "//tmp"
	template = "USE `%s`;\n" +
		"DEFINE SUBQUERY $main() AS\n" +
		"%s\n" +
		"END DEFINE;\n" +
		"INSERT INTO `%s` WITH (TRUNCATE, USER_ATTRS='{expiration_time=\"%s\"}')\n" +
		"SELECT * FROM $main()"
)

type Select struct {
	Client    *client.Client
	QueryId   yt.QueryID
	Ctx       *context.Context
	TempTable string
}

func formatQuery(cluster string, query string) (string, string) {
	tableName := fmt.Sprintf("%d_%d", time.Now().Second(), rand.Uint32())
	tempTable := fmt.Sprintf("%s/%s", TempDir, tableName)
	ttl := time.Now().AddDate(0, 0, 3).UTC().Format("2006-01-02T15:04:05")

	newQuery := fmt.Sprintf(template, cluster, query, tempTable, ttl)
	return newQuery, tempTable
}

func NewSelect(client *client.Client, query string, ctx *context.Context) (*Select, error) {
	formattedQuery, tempTable := formatQuery(client.Cluster, query)
	log.Println(formattedQuery)
	qid, err := client.Client.StartQuery(*ctx, yt.QueryEngineYQL, formattedQuery, &yt.StartQueryOptions{
		Settings: map[string]any{
			"cluster": client.Cluster,
		},
	})
	if err != nil {
		return nil, err
	}
	return &Select{
			client,
			qid,
			ctx,
			tempTable},
		nil
}

func (s *Select) Status() (*yt.Query, error) {
	query, err := s.Client.Client.GetQuery(*s.Ctx, s.QueryId, &yt.GetQueryOptions{
		QueryTrackerOptions: &yt.QueryTrackerOptions{},
	})
	if err != nil {
		return nil, err
	}
	return query, nil
}

func (s *Select) WaitStatus(logging bool) (string, error) {
	for {
		query, err := s.Status()
		if err != nil {
			return "error", err
		}
		if logging {
			log.Printf("Query status: %s", *query.State)
		}
		if *query.State == "completed" || *query.State == "failed" {
			return string(*query.State), nil
		}
		time.Sleep(time.Second)
	}
}

func (s *Select) Result() (*table.Reader, error) {
	reader, err := table.NewReader(s.Client, s.TempTable, s.Ctx)
	if err != nil {
		return nil, err
	} else {
		return reader, nil
	}
}
