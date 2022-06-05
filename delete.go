package redists

import (
	"context"
	"time"
)

type cmdDel struct {
	key  string
	from time.Time
	to   time.Time
}

func newCmdDel(key string, from time.Time, to time.Time) *cmdDel {
	return &cmdDel{key: key, from: from, to: to}
}

func (c *cmdDel) Name() string {
	return "TS.DEL"
}

func (c *cmdDel) Args() []interface{} {
	return []interface{}{c.key, c.from.UnixMilli(), c.to.UnixMilli()}
}

// Del deletes samples between two timestamps for a given key.
func (c *Client) Del(ctx context.Context, key string, from time.Time, to time.Time) (int64, error) {
	cmd := newCmdDel(key, from, to)
	res, err := c.d.Do(ctx, cmd.Name(), cmd.Args()...)
	if err != nil {
		return 0, err
	}
	return res.(int64), err
}
