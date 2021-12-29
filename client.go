package redists

import (
	"context"
)

type Doer interface {
	Do(ctx context.Context, cmd string, args ...interface{}) (interface{}, error)
}

type Client struct {
	d Doer
}

func NewClient(d Doer) *Client {
	return &Client{d: d}
}
