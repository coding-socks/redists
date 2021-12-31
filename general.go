package redists

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"time"
)

type Rule struct {
	Key         string
	Aggregation Aggregation
}

func parseRule(is []interface{}) Rule {
	return Rule{
		Key: parseString(is[0]),
		Aggregation: Aggregation{
			TimeBucket: time.Duration(is[1].(int64)) * time.Millisecond,
			Type:       parseAggregationType(is[2]),
		},
	}
}

type ChunkInfo struct {
	StartTimestamp time.Time
	EndTimestamp   time.Time
	Samples        int64
	Size           int64
	BytesPerSample float64
}

func parseChunkInfo(is []interface{}) ChunkInfo {
	var inf ChunkInfo
	for i := 0; i < len(is); i += 2 {
		key := parseString(is[i])
		val := is[i+1]
		if val == nil {
			continue
		}
		switch key {
		case "startTimestamp":
			inf.StartTimestamp = time.UnixMilli(val.(int64))
		case "endTimestamp":
			inf.EndTimestamp = time.UnixMilli(val.(int64))
		case "samples":
			inf.Samples = val.(int64)
		case "size":
			inf.Size = val.(int64)
		case "bytesPerSample":
			inf.BytesPerSample, _ = strconv.ParseFloat(parseString(val), 64)
		}
	}
	return inf
}

type Info struct {
	TotalSamples    int64
	MemoryUsage     int64
	FirstTimestamp  time.Time
	LastTimestamp   time.Time
	RetentionTime   time.Duration
	ChunkCount      int64
	ChunkSize       int64
	ChunkType       Encoding
	DuplicatePolicy *DuplicatePolicy
	Labels          []Label
	SourceKey       string
	Rules           []Rule
	Chunks          []ChunkInfo
}

func parseInfo(is []interface{}) Info {
	var inf Info
	for i := 0; i < len(is); i += 2 {
		key := parseString(is[i])
		val := is[i+1]
		if val == nil {
			continue
		}
		// some clients (e.g. radix) decode nil as []uint8(nil) instead of nil(nil)
		if v := reflect.ValueOf(val); v.Kind() == reflect.Slice && v.IsNil() {
			continue
		}
		switch key {
		case "totalSamples":
			inf.TotalSamples = val.(int64)
		case "memoryUsage":
			inf.MemoryUsage = val.(int64)
		case "firstTimestamp":
			inf.FirstTimestamp = time.UnixMilli(val.(int64))
		case "lastTimestamp":
			inf.LastTimestamp = time.UnixMilli(val.(int64))
		case "retentionTime":
			inf.RetentionTime = time.Duration(val.(int64)) * time.Millisecond
		case "chunkCount":
			inf.ChunkCount = val.(int64)
		case "chunkSize":
			inf.ChunkSize = val.(int64)
		case "chunkType":
			inf.ChunkType = parseEncoding(val)
		case "duplicatePolicy":
			policy := parseDuplicatePolicy(val)
			inf.DuplicatePolicy = &policy
		case "labels":
			for _, v := range val.([]interface{}) {
				inf.Labels = append(inf.Labels, parseLabel(v.([]interface{})))
			}
		case "sourceKey":
			inf.SourceKey = parseString(val)
		case "rules":
			for _, v := range val.([]interface{}) {
				inf.Rules = append(inf.Rules, parseRule(v.([]interface{})))
			}
		case "Chunks":
			inf.Chunks = []ChunkInfo{}
			for _, v := range val.([]interface{}) {
				inf.Chunks = append(inf.Chunks, parseChunkInfo(v.([]interface{})))
			}
		}
	}
	return inf
}

func parseString(val interface{}) string {
	switch val.(type) {
	case []byte:
		return string(val.([]byte))
	case string: // some clients decodes values as string
		return val.(string)
	default:
		panic(fmt.Sprintf("val %T not convertible to string", val))
	}
}

type cmdInfo struct {
	key   string
	debug bool
}

func newCmdInfo(key string) *cmdInfo {
	return &cmdInfo{key: key}
}

func (c *cmdInfo) Name() string {
	return "TS.INFO"
}

func (c *cmdInfo) Args() []interface{} {
	args := []interface{}{c.key}
	if c.debug {
		args = append(args, optionNameDebug)
	}
	return args
}

type OptionInfo func(cmd *cmdInfo)

// Info returns information and statistics on the time-series.
func (c *Client) Info(ctx context.Context, key string, options ...OptionInfo) (Info, error) {
	cmd := newCmdInfo(key)
	for i := range options {
		options[i](cmd)
	}
	i, err := c.d.Do(ctx, cmd.Name(), cmd.Args()...)
	var inf Info
	if is, ok := i.([]interface{}); ok {
		inf = parseInfo(is)
	}
	return inf, err
}

func InfoWithDebug() OptionInfo {
	return func(cmd *cmdInfo) {
		cmd.debug = true
	}
}

type cmdQueryIndex struct {
	filters []Filter
}

func newCmdQueryIndex(filters []Filter) *cmdQueryIndex {
	return &cmdQueryIndex{filters: filters}
}

func (c *cmdQueryIndex) Name() string {
	return "TS.QUERYINDEX"
}

func (c *cmdQueryIndex) Args() []interface{} {
	args := []interface{}{}
	for _, f := range c.filters {
		args = append(args, f.Arg())
	}
	return args
}

// QueryIndex lists all the keys matching the filter list.
func (c *Client) QueryIndex(ctx context.Context, filters []Filter) ([]string, error) {
	cmd := newCmdQueryIndex(filters)
	res, err := c.d.Do(ctx, cmd.Name(), cmd.Args()...)
	var keys []string
	if is, ok := res.([]interface{}); ok {
		keys = make([]string, len(is))
		for i := range is {
			keys[i] = parseString(is[i])
		}
	}
	return keys, err
}
