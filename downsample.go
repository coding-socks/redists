package redists

import (
	"context"
)

type cmdCreateRule struct {
	srcKey, destKey string
	agg             Aggregation
}

func (c *cmdCreateRule) Name() string {
	return "TS.CREATERULE"
}

func (c *cmdCreateRule) Args() []interface{} {
	return []interface{}{c.srcKey, c.destKey, optionNameAggregation, string(c.agg.Type), c.agg.TimeBucket.Milliseconds()}
}

func newCmdCreateRule(srcKey, destKey string, t AggregationType, timeBucket Duration) *cmdCreateRule {
	return &cmdCreateRule{srcKey: srcKey, destKey: destKey, agg: Aggregation{Type: t, TimeBucket: timeBucket}}
}

// CreateRule creates a compaction rule.
func (c *Client) CreateRule(ctx context.Context, srcKey, destKey string, a AggregationType, timeBucket Duration) error {
	cmd := newCmdCreateRule(srcKey, destKey, a, timeBucket)
	_, err := c.d.Do(ctx, cmd.Name(), cmd.Args()...)
	return err
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
