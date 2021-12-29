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
	AggregationTypeAvg   = AggregationType("AVG")
	AggregationTypeSum   = AggregationType("SUM")
	AggregationTypeMin   = AggregationType("MIN")
	AggregationTypeMax   = AggregationType("MAX")
	AggregationTypeRange = AggregationType("RANGE")
	AggregationTypeCount = AggregationType("COUNT")
	AggregationTypeFirst = AggregationType("FIRST")
	AggregationTypeLast  = AggregationType("LAST")
	AggregationTypeStdP  = AggregationType("STD.P")
	AggregationTypeStdS  = AggregationType("STD.S")
	AggregationTypeVarP  = AggregationType("VAR.P")
	AggregationTypeVarS  = AggregationType("VAR.S")
)

type AggregationType string

func parseAggregationType(i interface{}) AggregationType {
	return AggregationType(strings.ToUpper(parseString(i)))
}

type Aggregation struct {
	Type       AggregationType
	TimeBucket time.Duration
}

type Label struct {
	Name, Value string
}

func NewLabel(name, value string) Label {
	return Label{Name: name, Value: value}
}

func parseLabel(is []interface{}) Label {
	return Label{Name: parseString(is[0]), Value: parseString(is[1])}
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

type ValueFilter struct {
	Min float64
	Max float64
}

// Timestamp can represent time.Time, `-`, `+`, and `*`.
type Timestamp struct {
	time.Time
	min  bool
	max  bool
	auto bool
}

func (t Timestamp) Min() bool {
	return t.min
}

func (t Timestamp) Max() bool {
	return t.max
}

func (t Timestamp) Auto() bool {
	return t.auto
}

func (t Timestamp) Arg() interface{} {
	if t.min {
		return "-"
	}
	if t.max {
		return "+"
	}
	if t.auto {
		return "*"
	}
	return t.Time.UnixMilli()
}

func TS(t time.Time) Timestamp {
	return Timestamp{Time: t}
}

// TSMin returns a Timestamp which represents `-`.
func TSMin() Timestamp {
	return Timestamp{min: true}
}

// TSMax returns a Timestamp which represents `+`.
func TSMax() Timestamp {
	return Timestamp{max: true}
}

// TSAuto returns a Timestamp which represents `*`.
func TSAuto() Timestamp {
	return Timestamp{auto: true}
}
