package redists

import (
	"context"
	"fmt"
	"time"
)

type cmdAlter struct {
	key       string
	retention Duration
	labels    map[string]string
}

func newCmdAlter(key string) *cmdAlter {
	return &cmdAlter{key: key}
}

func (c *cmdAlter) Name() string {
	return "TS.ALTER"
}

func (c *cmdAlter) Args() []interface{} {
	args := []interface{}{c.key}
	if c.retention != nil {
		args = append(args, optionNameRetention, c.retention.Milliseconds())
	}
	if c.labels != nil {
		args = append(args, optionNameLabels)
		args = append(args, encodeLabels(c.labels)...)
	}
	return args
}

type OptionAlter func(cmd *cmdAlter)

// Alter updates the retention, labels of an existing key.
func (c *Client) Alter(ctx context.Context, key string, options ...OptionAlter) error {
	cmd := newCmdAlter(key)
	for i := range options {
		options[i](cmd)
	}
	_, err := c.d.Do(ctx, cmd.Name(), cmd.Args()...)
	return err
}

func AlterWithRetention(r Duration) OptionAlter {
	return func(cmd *cmdAlter) {
		cmd.retention = r
	}
}

func AlterWithLabels(ls Labels) OptionAlter {
	return func(cmd *cmdAlter) {
		cmd.labels = ls
	}
}

type cmdAdd struct {
	sample          Sample
	retention       Duration
	encoding        *Encoding
	chunkSize       *int
	duplicatePolicy *DuplicatePolicy
	labels          map[string]string
}

func newCmdAdd(s Sample) *cmdAdd {
	return &cmdAdd{sample: s}
}

func (c *cmdAdd) Name() string {
	return "TS.ADD"
}

func (c *cmdAdd) Args() []interface{} {
	args := []interface{}{c.sample.Key, timestampArg(c.sample.Timestamp), c.sample.Value}
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
	if c.labels != nil {
		args = append(args, optionNameLabels)
		args = append(args, encodeLabels(c.labels)...)
	}
	return args
}

type OptionAdd func(cmd *cmdAdd)

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

func AddWithRetention(r Duration) OptionAdd {
	return func(cmd *cmdAdd) {
		cmd.retention = r
	}
}

func AddWithEncoding(e Encoding) OptionAdd {
	return func(cmd *cmdAdd) {
		cmd.encoding = &e
	}
}

func AddWithChunkSize(cs int) OptionAdd {
	return func(cmd *cmdAdd) {
		cmd.chunkSize = &cs
	}
}

func AddWithOnDuplicate(dp DuplicatePolicy) OptionAdd {
	return func(cmd *cmdAdd) {
		cmd.duplicatePolicy = &dp
	}
}

func AddWithLabels(ls Labels) OptionAdd {
	return func(cmd *cmdAdd) {
		cmd.labels = ls
	}
}

type cmdMAdd struct {
	samples []Sample
}

func newCmdMAdd(s []Sample) *cmdMAdd {
	return &cmdMAdd{samples: s}
}

func (c *cmdMAdd) Name() string {
	return "TS.MADD"
}

func (c *cmdMAdd) Args() []interface{} {
	var args []interface{}
	for _, s := range c.samples {
		args = append(args, s.Key, timestampArg(s.Timestamp), s.Value)
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

type cmdCounter struct {
	name      nameCounter
	key       string
	value     float64
	timestamp *time.Time
	retention Duration
	encoding  *Encoding
	chunkSize *int
	labels    map[string]string
}

func newCmdCounter(name nameCounter, key string, value float64) *cmdCounter {
	return &cmdCounter{name: name, key: key, value: value}
}

func (c *cmdCounter) Name() string {
	return string(c.name)
}

func (c *cmdCounter) Args() []interface{} {
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
	if c.labels != nil {
		args = append(args, optionNameLabels)
		args = append(args, encodeLabels(c.labels)...)
	}
	return args
}

type OptionCounter func(cmd *cmdCounter)

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

func CounterWithRetention(r Duration) OptionCounter {
	return func(cmd *cmdCounter) {
		cmd.retention = r
	}
}

func CounterWithTimestamp(t time.Time) OptionCounter {
	return func(cmd *cmdCounter) {
		cmd.timestamp = &t
	}
}

func CounterWithEncoding(e Encoding) OptionCounter {
	return func(cmd *cmdCounter) {
		cmd.encoding = &e
	}
}

func CounterWithChunkSize(cs int) OptionCounter {
	return func(cmd *cmdCounter) {
		cmd.chunkSize = &cs
	}
}

func CounterWithLabels(ls Labels) OptionCounter {
	return func(cmd *cmdCounter) {
		cmd.labels = ls
	}
}
