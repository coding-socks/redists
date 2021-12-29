package redists

import (
	"context"
	"time"
)

type CmdDel struct {
	key  string
	from time.Time
	to   time.Time
}

func newCmdDel(key string, from time.Time, to time.Time) *CmdDel {
	return &CmdDel{key: key, from: from, to: to}
}

func (c *CmdDel) Name() string {
	return "TS.DEL"
}

func (c *CmdDel) Args() []interface{} {
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
