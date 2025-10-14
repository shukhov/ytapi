package client

import (
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
)

type Client struct {
	Client  yt.Client
	Cluster string
}

func NewClient(Proxy string, Token string, Cluster string) (*Client, error) {
	yc, err := ythttp.NewClient(&yt.Config{
		Proxy: Proxy,
		Token: Token,
	})
	if err != nil {
		return nil, err
	}
	client := &Client{Client: yc, Cluster: Cluster}
	return client, nil
}
