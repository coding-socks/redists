package redists

import (
	"sort"
	"strings"
	"time"
)

var (
	optionNameAggregation     = "AGGREGATION"
	optionNameAlign           = "ALIGN"
	optionNameChunkSize       = "CHUNK_SIZE"
	optionNameCount           = "COUNT"
	optionNameDebug           = "DEBUG"
	optionNameDuplicatePolicy = "DUPLICATE_POLICY"
	optionNameEncoding        = "ENCODING"
	optionNameFilter          = "FILTER"
	optionNameFilterByTS      = "FILTER_BY_TS"
	optionNameFilterByValue   = "FILTER_BY_VALUE"
	optionNameGroupBy         = "GROUPBY"
	optionNameLabels          = "LABELS"
	optionNameOnDuplicate     = "ON_DUPLICATE"
	optionNameReduce          = "REDUCE"
	optionNameRetention       = "RETENTION"
	optionNameSelectedLabels  = "SELECTED_LABELS"
	optionNameTimestamp       = "TIMESTAMP"
	optionNameUncompressed    = "UNCOMPRESSED"
	optionNameWithLabels      = "WITHLABELS"
)

const (
	// EncodingCompressed applies the DoubleDelta compression to the series samples.
	EncodingCompressed = Encoding("COMPRESSED")
	// EncodingUncompressed keeps the raw samples in memory.
	EncodingUncompressed = Encoding("UNCOMPRESSED")
)

type Encoding string

func parseEncoding(i interface{}) Encoding {
	return Encoding(strings.ToUpper(parseString(i)))
}

const (
	// DuplicatePolicyBlock raises an error for any out of order sample.
	DuplicatePolicyBlock = DuplicatePolicy("BLOCK")
	// DuplicatePolicyFirst ignores the new value.
	DuplicatePolicyFirst = DuplicatePolicy("FIRST")
	// DuplicatePolicyLast overrides with the latest value.
	DuplicatePolicyLast = DuplicatePolicy("LAST")
	// DuplicatePolicyMin only overrides if the value is lower than the existing value.
	DuplicatePolicyMin = DuplicatePolicy("MIN")
	// DuplicatePolicyMax only overrides if the value is higher than the existing value.
	DuplicatePolicyMax = DuplicatePolicy("MAX")
	// DuplicatePolicySum in case a previous sample exists, adds the new sample to it so that the updated value is equal to (previous + new). If no previous sample exists, set the updated value equal to the new value.
	DuplicatePolicySum = DuplicatePolicy("SUM")
)

type DuplicatePolicy string

func parseDuplicatePolicy(i interface{}) DuplicatePolicy {
	return DuplicatePolicy(strings.ToUpper(parseString(i)))
}

const (
	ReducerSum = ReducerType("SUM")
	ReducerMin = ReducerType("MIN")
	ReducerMax = ReducerType("MAX")
)

type ReducerType string

type GroupBy struct {
	Label   string
	Reducer ReducerType
}

const (
	// AggregationTypeAvg aggregates arithmetic mean of all values
	AggregationTypeAvg = AggregationType("AVG")
	// AggregationTypeSum aggregates sum of all values
	AggregationTypeSum = AggregationType("SUM")
	// AggregationTypeMin aggregates minimum value
	AggregationTypeMin = AggregationType("MIN")
	// AggregationTypeMax aggregates maximum value
	AggregationTypeMax = AggregationType("MAX")
	// AggregationTypeRange aggregates difference between the highest and the lowest value
	AggregationTypeRange = AggregationType("RANGE")
	// AggregationTypeCount aggregates number of values
	AggregationTypeCount = AggregationType("COUNT")
	// AggregationTypeFirst aggregates the value with the lowest timestamp in the bucket
	AggregationTypeFirst = AggregationType("FIRST")
	// AggregationTypeLast aggregates the value with the highest timestamp in the bucket
	AggregationTypeLast = AggregationType("LAST")
	// AggregationTypeStdP aggregates population standard deviation of the values
	AggregationTypeStdP = AggregationType("STD.P")
	// AggregationTypeStdS aggregates sample standard deviation of the values
	AggregationTypeStdS = AggregationType("STD.S")
	// AggregationTypeVarP aggregates population variance of the values
	AggregationTypeVarP = AggregationType("VAR.P")
	// AggregationTypeVarS aggregates sample variance of the values
	AggregationTypeVarS = AggregationType("VAR.S")
	// AggregationTypeTWA aggregates time-weighted average of all values
	AggregationTypeTWA = AggregationType("TWA")
)

type AggregationType string

func parseAggregationType(i interface{}) AggregationType {
	return AggregationType(strings.ToUpper(parseString(i)))
}

type Aggregation struct {
	Type   AggregationType
	Bucket Duration
}

type Labels map[string]string

func parseLabels(is []interface{}) Labels {
	ls := make(Labels, len(is))
	for i := range is {
		v := is[i].([]interface{})
		ls[parseString(v[0])] = parseString(v[1])
	}
	return ls
}

func encodeLabels(ls map[string]string) []interface{} {
	var args []interface{}
	// keep order consistent for testing
	keys := make([]string, 0, len(ls))
	for key := range ls {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		args = append(args, key, ls[key])
	}
	return args
}

type Sample struct {
	Key       string
	Timestamp Timestamp
	Value     float64
}

func NewSample(key string, timestamp Timestamp, value float64) Sample {
	return Sample{Key: key, Timestamp: timestamp, Value: value}
}

type Filter struct {
	Label  string
	Equal  bool
	Values []string
}

func (f Filter) Arg() interface{} {
	arg := f.Label
	if f.Equal {
		arg += "="
	} else {
		arg += "!="
	}
	if len(f.Values) > 1 {
		arg += "(" + strings.Join(f.Values, ",") + ")"
	} else if len(f.Values) == 1 {
		arg += f.Values[0]
	}
	return arg
}

func FilterEqual(label string, values ...string) Filter {
	return Filter{Label: label, Equal: true, Values: values}
}

func FilterNotEqual(label string, values ...string) Filter {
	return Filter{Label: label, Equal: false, Values: values}
}

type valueFilter struct {
	min float64
	max float64
}

type Duration interface {
	Milliseconds() int64
}

type Timestamp interface {
	UnixMilli() int64
}

type TimestampMin interface {
	Timestamp
	Min() bool
}

type TimestampMax interface {
	Timestamp
	Max() bool
}

type TimestampAuto interface {
	Timestamp
	Auto() bool
}

// TS can represent time.Time, `-`, `+`, and `*`.
type TS struct {
	time.Time
	min  bool
	max  bool
	auto bool
}

func (t TS) Min() bool {
	return t.min
}

func (t TS) Max() bool {
	return t.max
}

func (t TS) Auto() bool {
	return t.auto
}

func timestampArg(ts Timestamp) interface{} {
	if v, ok := ts.(TimestampMin); ok && v.Min() {
		return "-"
	}
	if v, ok := ts.(TimestampMax); ok && v.Max() {
		return "+"
	}
	if v, ok := ts.(TimestampAuto); ok && v.Auto() {
		return "*"
	}
	return ts.UnixMilli()
}

// TSMin returns a TS which represents `-`.
func TSMin() TS {
	return TS{min: true}
}

// TSMax returns a TS which represents `+`.
func TSMax() TS {
	return TS{max: true}
}

// TSAuto returns a TS which represents `*`.
func TSAuto() TS {
	return TS{auto: true}
}
