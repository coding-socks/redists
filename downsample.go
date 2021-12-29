package redists

import (
	"context"
	"time"
)

type CmdCreateRule struct {
	srcKey, destKey string
	agg             Aggregation
}

func (c *CmdCreateRule) Name() string {
	return "TS.CREATERULE"
}
func (c *CmdCreateRule) Args() []interface{} {
	return []interface{}{c.srcKey, c.destKey, optionNameAggregation, string(c.agg.Type), c.agg.TimeBucket.Milliseconds()}
}

func newCmdCreateRule(srcKey, destKey string, t AggregationType, timeBucket time.Duration) *CmdCreateRule {
	return &CmdCreateRule{srcKey: srcKey, destKey: destKey, agg: Aggregation{Type: t, TimeBucket: timeBucket}}
}

// CreateRule creates a compaction rule.
func (c *Client) CreateRule(ctx context.Context, srcKey, destKey string, a AggregationType, timeBucket time.Duration) error {
	cmd := newCmdCreateRule(srcKey, destKey, a, timeBucket)
	_, err := c.d.Do(ctx, cmd.Name(), cmd.Args()...)
	return err
}

type CmdDeleteRule struct {
	srcKey, destKey string
}

func (c *CmdDeleteRule) Name() string {
	return "TS.DELETERULE"
}
func (c *CmdDeleteRule) Args() []interface{} {
	return []interface{}{c.srcKey, c.destKey}
}

func newCmdDeleteRule(srcKey, destKey string) *CmdDeleteRule {
	return &CmdDeleteRule{srcKey: srcKey, destKey: destKey}
}

// DeleteRule deletes a compaction rule.
func (c *Client) DeleteRule(ctx context.Context, srcKey, destKey string) error {
	cmd := newCmdDeleteRule(srcKey, destKey)
	_, err := c.d.Do(ctx, cmd.Name(), cmd.Args()...)
	return err
}
