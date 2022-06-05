package redists

import (
	"context"
	"time"
)

type cmdCreateRule struct {
	srcKey, destKey string
	agg             Aggregation
	alignTimestamp  *time.Time
}

func (c *cmdCreateRule) Name() string {
	return "TS.CREATERULE"
}

func (c *cmdCreateRule) Args() []interface{} {
	args := []interface{}{c.srcKey, c.destKey, optionNameAggregation, string(c.agg.Type), c.agg.Bucket.Milliseconds()}
	if c.alignTimestamp != nil {
		args = append(args, c.alignTimestamp.UnixMilli())
	}
	return args
}

func newCmdCreateRule(srcKey, destKey string, t AggregationType, bucket Duration) *cmdCreateRule {
	return &cmdCreateRule{srcKey: srcKey, destKey: destKey, agg: Aggregation{Type: t, Bucket: bucket}}
}

type OptionCreateRule func(cmd *cmdCreateRule)

// CreateRule creates a compaction rule.
func (c *Client) CreateRule(ctx context.Context, srcKey, destKey string, a AggregationType, bucket Duration, options ...OptionCreateRule) error {
	cmd := newCmdCreateRule(srcKey, destKey, a, bucket)
	for i := range options {
		options[i](cmd)
	}
	_, err := c.d.Do(ctx, cmd.Name(), cmd.Args()...)
	return err
}

func CreateRuleWithAlignTimestamp(t time.Time) OptionCreateRule {
	return func(cmd *cmdCreateRule) {
		cmd.alignTimestamp = &t
	}
}

type cmdDeleteRule struct {
	srcKey, destKey string
}

func (c *cmdDeleteRule) Name() string {
	return "TS.DELETERULE"
}
func (c *cmdDeleteRule) Args() []interface{} {
	return []interface{}{c.srcKey, c.destKey}
}

func newCmdDeleteRule(srcKey, destKey string) *cmdDeleteRule {
	return &cmdDeleteRule{srcKey: srcKey, destKey: destKey}
}

// DeleteRule deletes a compaction rule.
func (c *Client) DeleteRule(ctx context.Context, srcKey, destKey string) error {
	cmd := newCmdDeleteRule(srcKey, destKey)
	_, err := c.d.Do(ctx, cmd.Name(), cmd.Args()...)
	return err
}
