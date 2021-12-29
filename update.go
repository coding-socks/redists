package redists

import (
	"context"
	"fmt"
	"time"
)

type CmdAlter struct {
	key       string
	retention *time.Duration
	labels    map[string]string
}

func newCmdAlter(key string) *CmdAlter {
	return &CmdAlter{key: key}
}

func (c *CmdAlter) Name() string {
	return "TS.ALTER"
}

func (c *CmdAlter) Args() []interface{} {
	args := []interface{}{c.key}
	if c.retention != nil {
		args = append(args, optionNameRetention, c.retention.Milliseconds())
	}
	if len(c.labels) > 0 {
		args = append(args, optionNameLabels)
		args = append(args, encodeLabels(c.labels)...)
	}
	return args
}

type OptionAlter func(cmd *CmdAlter)

// Alter updates the retention, labels of an existing key.
func (c *Client) Alter(ctx context.Context, key string, options ...OptionAlter) error {
	cmd := newCmdAlter(key)
	for i := range options {
		options[i](cmd)
	}
	_, err := c.d.Do(ctx, cmd.Name(), cmd.Args()...)
	return err
}

func AlterWithRetention(r time.Duration) OptionAlter {
	return func(cmd *CmdAlter) {
		cmd.retention = &r
	}
}

func AlterWithLabels(labels ...Label) OptionAlter {
	return func(cmd *CmdAlter) {
		cmd.labels = map[string]string{}
		for _, l := range labels {
			cmd.labels[l.Name] = l.Value
		}
	}
}

type CmdAdd struct {
	sample          Sample
	retention       *time.Duration
	encoding        *Encoding
	chunkSize       *int
	duplicatePolicy *DuplicatePolicy
	labels          map[string]string
}

func newCmdAdd(s Sample) *CmdAdd {
	return &CmdAdd{sample: s}
}

func (c *CmdAdd) Name() string {
	return "TS.ADD"
}

func (c *CmdAdd) Args() []interface{} {
	args := []interface{}{c.sample.Key, c.sample.Timestamp.Arg(), c.sample.Value}
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
		args = append(args, optionNameOnDuplicate, string(*c.duplicatePolicy))
	}
	if len(c.labels) > 0 {
		args = append(args, optionNameLabels)
		args = append(args, encodeLabels(c.labels)...)
	}
	return args
}

type OptionAdd func(cmd *CmdAdd)

// Add updates the retention, labels of an existing key.
func (c *Client) Add(ctx context.Context, s Sample, options ...OptionAdd) (time.Time, error) {
	cmd := newCmdAdd(s)
	for i := range options {
		options[i](cmd)
	}
	res, err := c.d.Do(ctx, cmd.Name(), cmd.Args()...)
	if err != nil {
		return time.Time{}, err
	}
	return time.UnixMilli(res.(int64)), err
}

func AddWithRetention(r time.Duration) OptionAdd {
	return func(cmd *CmdAdd) {
		cmd.retention = &r
	}
}

func AddWithEncoding(e Encoding) OptionAdd {
	return func(cmd *CmdAdd) {
		cmd.encoding = &e
	}
}

func AddWithChunkSize(cs int) OptionAdd {
	return func(cmd *CmdAdd) {
		cmd.chunkSize = &cs
	}
}

func AddWithOnDuplicate(dp DuplicatePolicy) OptionAdd {
	return func(cmd *CmdAdd) {
		cmd.duplicatePolicy = &dp
	}
}

func AddWithLabels(labels ...Label) OptionAdd {
	return func(cmd *CmdAdd) {
		cmd.labels = map[string]string{}
		for _, l := range labels {
			cmd.labels[l.Name] = l.Value
		}
	}
}

type CmdMAdd struct {
	samples []Sample
}

func newCmdMAdd(s []Sample) *CmdMAdd {
	return &CmdMAdd{samples: s}
}

func (c *CmdMAdd) Name() string {
	return "TS.MADD"
}

func (c *CmdMAdd) Args() []interface{} {
	var args []interface{}
	for _, s := range c.samples {
		args = append(args, s.Key, s.Timestamp.Arg(), s.Value)
	}
	return args
}

// MultiResult contains an error when a specific Sample triggers an error.
type MultiResult struct {
	t   time.Time
	err error
}

func (r MultiResult) Time() time.Time {
	return r.t
}

func (r MultiResult) Err() error {
	return r.err
}

// MAdd appends new samples to a list of series.
func (c *Client) MAdd(ctx context.Context, s []Sample) ([]MultiResult, error) {
	cmd := newCmdMAdd(s)
	res, err := c.d.Do(ctx, cmd.Name(), cmd.Args()...)
	if err != nil {
		return nil, err
	}
	var rs []MultiResult
	if is, ok := res.([]interface{}); ok {
		for i := range is {
			switch v := is[i].(type) {
			case error:
				rs = append(rs, MultiResult{err: v})
			case int64:
				rs = append(rs, MultiResult{t: time.UnixMilli(v)})
			default:
				panic(fmt.Sprintf("val %T not convertible to MultiResult", v))
			}
		}
	}
	return rs, err
}

const (
	nameIncrBy = nameCounter("TS.INCRBY")
	nameDecrBy = nameCounter("TS.DECRBY")
)

type nameCounter string

type CmdCounter struct {
	name      nameCounter
	key       string
	value     float64
	timestamp *time.Time
	retention *time.Duration
	encoding  *Encoding
	chunkSize *int
	labels    map[string]string
}

func newCmdCounter(name nameCounter, key string, value float64) *CmdCounter {
	return &CmdCounter{name: name, key: key, value: value}
}

func (c *CmdCounter) Name() string {
	return string(c.name)
}

func (c *CmdCounter) Args() []interface{} {
	args := []interface{}{c.key, c.value}
	if c.timestamp != nil {
		args = append(args, optionNameTimestamp, c.timestamp.UnixMilli())
	}
	if c.retention != nil {
		args = append(args, optionNameRetention, c.retention.Milliseconds())
	}
	if c.encoding != nil && *c.encoding == EncodingUncompressed {
		args = append(args, optionNameUncompressed)
	}
	if c.chunkSize != nil {
		args = append(args, optionNameChunkSize, *c.chunkSize)
	}
	if len(c.labels) > 0 {
		args = append(args, optionNameLabels)
		args = append(args, encodeLabels(c.labels)...)
	}
	return args
}

type OptionCounter func(cmd *CmdCounter)

// IncrBy creates a new sample that increments the latest sample's value.
func (c *Client) IncrBy(ctx context.Context, key string, value float64, options ...OptionCounter) (time.Time, error) {
	return c.counter(ctx, nameIncrBy, key, value, options...)
}

// DecrBy creates a new sample that decrements the latest sample's value.
func (c *Client) DecrBy(ctx context.Context, key string, value float64, options ...OptionCounter) (time.Time, error) {
	return c.counter(ctx, nameDecrBy, key, value, options...)
}

func (c *Client) counter(ctx context.Context, name nameCounter, key string, value float64, options ...OptionCounter) (time.Time, error) {
	cmd := newCmdCounter(name, key, value)
	for i := range options {
		options[i](cmd)
	}
	res, err := c.d.Do(ctx, cmd.Name(), cmd.Args()...)
	if err != nil {
		return time.Time{}, err
	}
	return time.UnixMilli(res.(int64)), err
}

func CounterWithRetention(r time.Duration) OptionCounter {
	return func(cmd *CmdCounter) {
		cmd.retention = &r
	}
}

func CounterWithTimestamp(t time.Time) OptionCounter {
	return func(cmd *CmdCounter) {
		cmd.timestamp = &t
	}
}

func CounterWithEncoding(e Encoding) OptionCounter {
	return func(cmd *CmdCounter) {
		cmd.encoding = &e
	}
}

func CounterWithChunkSize(cs int) OptionCounter {
	return func(cmd *CmdCounter) {
		cmd.chunkSize = &cs
	}
}

func CounterWithLabels(labels ...Label) OptionCounter {
	return func(cmd *CmdCounter) {
		cmd.labels = map[string]string{}
		for _, l := range labels {
			cmd.labels[l.Name] = l.Value
		}
	}
}
