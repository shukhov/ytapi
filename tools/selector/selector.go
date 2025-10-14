package selector

import (
	"context"
	"fmt"
	"go.ytsaurus.tech/yt/go/yt"
	"golang.org/x/exp/rand"
	"log"
	"time"
	"ytapi/client"
	"ytapi/tools/table"
)

var (
	TempDir  = "//tmp"
	template = "USE `%s`; \n INSERT INTO `%s/%d` WITH (TRUNCATE, EXPIRATION=\"2d\")\n%s"
)

type Select struct {
	Client    *client.Client
	QueryId   yt.QueryID
	Ctx       *context.Context
	TempTable string
}

func formatQuery(query string, cluster string) (string, string) {
	randNum := rand.Int31()
	newQuery := fmt.Sprintf(template, cluster, TempDir, randNum, query)
	tempTable := fmt.Sprintf("%s/%d", TempDir, randNum)
	return newQuery, tempTable
}

func NewSelect(client *client.Client, query string, ctx *context.Context) (*Select, error) {
	formattedQuery, tempTable := formatQuery(query, client.Cluster)
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
