package redists

import (
	"context"
	"time"
)

type CmdCreate struct {
	key             string
	retention       *time.Duration
	encoding        *Encoding
	chunkSize       *int
	duplicatePolicy *DuplicatePolicy
	labels          map[string]string
}

func newCmdCreate(key string) *CmdCreate {
	return &CmdCreate{key: key}
}

func (c *CmdCreate) Name() string {
	return "TS.CREATE"
}

func (c *CmdCreate) Args() []interface{} {
	args := []interface{}{c.key}
	if c.retention != nil {
		args = append(args, optionNameRetention, c.retention.Milliseconds())
	}
	if c.encoding != nil {
		args = append(args, optionNameEncoding, string(*c.encoding))
	}
	if c.chunkSize != nil {
		args = append(args, optionNameChunkSize, *c.chunkSize)
	}
	if c.duplicatePolicy != nil {
		args = append(args, optionNameDuplicatePolicy, string(*c.duplicatePolicy))
	}
	if len(c.labels) > 0 {
		args = append(args, optionNameLabels)
		args = append(args, encodeLabels(c.labels)...)
	}
	return args
}

type OptionCreate func(cmd *CmdCreate)

// Create creates a new time-series.
func (c *Client) Create(ctx context.Context, key string, options ...OptionCreate) error {
	cmd := newCmdCreate(key)
	for i := range options {
		options[i](cmd)
	}
	_, err := c.d.Do(ctx, cmd.Name(), cmd.Args()...)
	return err
}

func CreateWithRetention(r time.Duration) OptionCreate {
	return func(cmd *CmdCreate) {
		cmd.retention = &r
	}
}

func CreateWithEncoding(e Encoding) OptionCreate {
	return func(cmd *CmdCreate) {
		cmd.encoding = &e
	}
}

func CreateWithChunkSize(cs int) OptionCreate {
	return func(cmd *CmdCreate) {
		cmd.chunkSize = &cs
	}
}

func CreateWithDuplicatePolicy(dp DuplicatePolicy) OptionCreate {
	return func(cmd *CmdCreate) {
		cmd.duplicatePolicy = &dp
	}
}

func CreateWithLabels(labels ...Label) OptionCreate {
	return func(cmd *CmdCreate) {
		cmd.labels = map[string]string{}
		for _, l := range labels {
			cmd.labels[l.Name] = l.Value
		}
	}
}
