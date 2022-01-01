package redists

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"
)

var (
	secondMillennium = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).In(time.Local)
	thirdMillennium  = time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC).In(time.Local)
)

func TestCmdRanger(t *testing.T) {
	t.Run("range", func(t *testing.T) {
		cmd := newCmdRanger(nameRange, "key:any", secondMillennium, thirdMillennium)
		if got, want := cmd.Name(), "TS.RANGE"; got != want {
			t.Errorf("Name() = %v, want %v", got, want)
		}
		if got, want := cmd.Args(), []interface{}{"key:any", secondMillennium.UnixMilli(), thirdMillennium.UnixMilli()}; !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
	t.Run("rev range", func(t *testing.T) {
		cmd := newCmdRanger(nameRevRange, "key:any", secondMillennium, thirdMillennium)
		if got, want := cmd.Name(), "TS.REVRANGE"; got != want {
			t.Errorf("Name() = %v, want %v", got, want)
		}
		if got, want := cmd.Args(), []interface{}{"key:any", secondMillennium.UnixMilli(), thirdMillennium.UnixMilli()}; !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
	t.Run("filter by ts", func(t *testing.T) {
		cmd := newCmdRanger(nameRange, "key:any", secondMillennium, thirdMillennium)
		RangerWithTSFilter(time.Unix(1500000000, 0), time.Unix(1600000000, 0))(cmd)
		if got, want := cmd.Args(), []interface{}{"key:any", secondMillennium.UnixMilli(), thirdMillennium.UnixMilli(), "FILTER_BY_TS", int64(1500000000000), int64(1600000000000)}; !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
	t.Run("filter by value", func(t *testing.T) {
		cmd := newCmdRanger(nameRange, "key:any", secondMillennium, thirdMillennium)
		RangerWithValueFilter(0.2, 0.4)(cmd)
		if got, want := cmd.Args(), []interface{}{"key:any", secondMillennium.UnixMilli(), thirdMillennium.UnixMilli(), "FILTER_BY_VALUE", 0.2, 0.4}; !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
	t.Run("count", func(t *testing.T) {
		cmd := newCmdRanger(nameRange, "key:any", secondMillennium, thirdMillennium)
		RangerWithCount(100)(cmd)
		if got, want := cmd.Args(), []interface{}{"key:any", secondMillennium.UnixMilli(), thirdMillennium.UnixMilli(), "COUNT", int64(100)}; !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
	t.Run("align", func(t *testing.T) {
		cmd := newCmdRanger(nameRange, "key:any", secondMillennium, thirdMillennium)
		RangerWithAlign(time.Unix(1500000000, 0))(cmd)
		if got, want := cmd.Args(), []interface{}{"key:any", secondMillennium.UnixMilli(), thirdMillennium.UnixMilli(), "ALIGN", int64(1500000000000)}; !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
	t.Run("aggregation", func(t *testing.T) {
		cmd := newCmdRanger(nameRange, "key:any", secondMillennium, thirdMillennium)
		RangerWithAggregation(AggregationTypeAvg, time.Second)(cmd)
		if got, want := cmd.Args(), []interface{}{"key:any", secondMillennium.UnixMilli(), thirdMillennium.UnixMilli(), "AGGREGATION", "AVG", int64(1000)}; !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
	t.Run("args order", func(t *testing.T) {
		cmd := newCmdRanger(nameRange, "key:any", secondMillennium, thirdMillennium)
		want := []interface{}{
			"key:any", secondMillennium.UnixMilli(), thirdMillennium.UnixMilli(),
			"FILTER_BY_TS", int64(1500000000000), int64(1600000000000),
			"FILTER_BY_VALUE", 0.2, 0.4,
			"COUNT", int64(100),
			"ALIGN", int64(1500000000000),
			"AGGREGATION", "AVG", int64(1000),
		}
		RangerWithTSFilter(time.Unix(1500000000, 0), time.Unix(1600000000, 0))(cmd)
		RangerWithValueFilter(0.2, 0.4)(cmd)
		RangerWithCount(100)(cmd)
		RangerWithAlign(time.Unix(1500000000, 0))(cmd)
		RangerWithAggregation(AggregationTypeAvg, time.Second)(cmd)
		if got := cmd.Args(); !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
		cmd = newCmdRanger(nameRange, "key:any", secondMillennium, thirdMillennium)
		RangerWithAggregation(AggregationTypeAvg, time.Second)(cmd)
		RangerWithAlign(time.Unix(1500000000, 0))(cmd)
		RangerWithCount(100)(cmd)
		RangerWithValueFilter(0.2, 0.4)(cmd)
		RangerWithTSFilter(time.Unix(1500000000, 0), time.Unix(1600000000, 0))(cmd)
		if got := cmd.Args(); !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
}

func TestClient_Range(t *testing.T) {
	if testing.Short() {
		t.Skip("skip client test")
	}
	for _, tt := range doerTests {
		t.Run(tt.name, func(t *testing.T) {
			key := fmt.Sprintf("example:%s", t.Name())

			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			doer, err := tt.doer(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer doer.Close()
			defer doer.Do(context.Background(), "DEL", key)

			tsclient := NewClient(doer)
			if err = tsclient.Create(ctx, key); err != nil {
				t.Fatalf("Create() error = %v", err)
			}
			_, err = tsclient.MAdd(ctx, []Sample{
				NewSample(key, secondMillennium, 1),
				NewSample(key, thirdMillennium, 2),
				NewSample(key, thirdMillennium.Add(time.Minute), 3),
			})
			if err != nil {
				t.Fatalf("MAdd() error = %v", err)
			}
			points, err := tsclient.Range(ctx, key, TSMin(), TSMax(), RangerWithCount(2))
			if err != nil {
				t.Errorf("Range() error = %v", err)
			}
			want := []DataPoint{
				{secondMillennium, 1.0},
				{thirdMillennium, 2.0},
			}
			if got := points; !reflect.DeepEqual(got, want) {
				t.Errorf("Range() got = %v, want %v", got, want)
			}
		})
	}
}

func TestCmdMRanger(t *testing.T) {
	t.Run("range", func(t *testing.T) {
		cmd := newCmdMRanger(nameMRange, secondMillennium, thirdMillennium, []Filter{FilterEqual("l", "v")})
		if got, want := cmd.Name(), "TS.MRANGE"; got != want {
			t.Errorf("Name() = %v, want %v", got, want)
		}
		if got, want := cmd.Args(), []interface{}{secondMillennium.UnixMilli(), thirdMillennium.UnixMilli(), "FILTER", "l=v"}; !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
	t.Run("rev range", func(t *testing.T) {
		cmd := newCmdMRanger(nameMRevRange, secondMillennium, thirdMillennium, []Filter{FilterEqual("l", "v")})
		if got, want := cmd.Name(), "TS.MREVRANGE"; got != want {
			t.Errorf("Name() = %v, want %v", got, want)
		}
		if got, want := cmd.Args(), []interface{}{secondMillennium.UnixMilli(), thirdMillennium.UnixMilli(), "FILTER", "l=v"}; !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
	t.Run("filter by ts", func(t *testing.T) {
		cmd := newCmdMRanger(nameMRange, secondMillennium, thirdMillennium, []Filter{FilterEqual("l", "v")})
		MRangerWithTSFilter(time.Unix(1500000000, 0), time.Unix(1600000000, 0))(cmd)
		want := []interface{}{
			secondMillennium.UnixMilli(), thirdMillennium.UnixMilli(),
			"FILTER_BY_TS", int64(1500000000000), int64(1600000000000),
			"FILTER", "l=v",
		}
		if got := cmd.Args(); !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
	t.Run("filter by value", func(t *testing.T) {
		cmd := newCmdMRanger(nameMRange, secondMillennium, thirdMillennium, []Filter{FilterEqual("l", "v")})
		MRangerWithValueFilter(0.2, 0.4)(cmd)
		want := []interface{}{
			secondMillennium.UnixMilli(), thirdMillennium.UnixMilli(),
			"FILTER_BY_VALUE", 0.2, 0.4,
			"FILTER", "l=v",
		}
		if got := cmd.Args(); !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
	t.Run("with labels", func(t *testing.T) {
		cmd := newCmdMRanger(nameMRange, secondMillennium, thirdMillennium, []Filter{FilterEqual("l", "v")})
		MRangerWithLabels()(cmd)
		want := []interface{}{
			secondMillennium.UnixMilli(), thirdMillennium.UnixMilli(),
			"WITHLABELS",
			"FILTER", "l=v",
		}
		if got := cmd.Args(); !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
	t.Run("selected labels", func(t *testing.T) {
		cmd := newCmdMRanger(nameMRange, secondMillennium, thirdMillennium, []Filter{FilterEqual("l", "v")})
		MRangerWithLabels("l")(cmd)
		want := []interface{}{
			secondMillennium.UnixMilli(), thirdMillennium.UnixMilli(),
			"SELECTED_LABELS", "l",
			"FILTER", "l=v",
		}
		if got := cmd.Args(); !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
	t.Run("count", func(t *testing.T) {
		cmd := newCmdMRanger(nameMRange, secondMillennium, thirdMillennium, []Filter{FilterEqual("l", "v")})
		MRangerWithCount(100)(cmd)
		want := []interface{}{
			secondMillennium.UnixMilli(), thirdMillennium.UnixMilli(),
			"COUNT", int64(100),
			"FILTER", "l=v",
		}
		if got := cmd.Args(); !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
	t.Run("align", func(t *testing.T) {
		cmd := newCmdMRanger(nameMRange, secondMillennium, thirdMillennium, []Filter{FilterEqual("l", "v")})
		MRangerWithAlign(time.Unix(1500000000, 0))(cmd)
		want := []interface{}{
			secondMillennium.UnixMilli(), thirdMillennium.UnixMilli(),
			"ALIGN", int64(1500000000000),
			"FILTER", "l=v",
		}
		if got := cmd.Args(); !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
	t.Run("aggregation", func(t *testing.T) {
		cmd := newCmdMRanger(nameMRange, secondMillennium, thirdMillennium, []Filter{FilterEqual("l", "v")})
		MRangerWithAggregation(AggregationTypeAvg, time.Second)(cmd)
		want := []interface{}{
			secondMillennium.UnixMilli(), thirdMillennium.UnixMilli(),
			"AGGREGATION", "AVG", int64(1000),
			"FILTER", "l=v",
		}
		if got := cmd.Args(); !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
	t.Run("group by", func(t *testing.T) {
		cmd := newCmdMRanger(nameMRange, secondMillennium, thirdMillennium, []Filter{FilterEqual("l", "v")})
		MRangerWithGroupBy("l", ReducerSum)(cmd)
		want := []interface{}{
			secondMillennium.UnixMilli(), thirdMillennium.UnixMilli(),
			"FILTER", "l=v",
			"GROUPBY", "l", "REDUCE", "SUM",
		}
		if got := cmd.Args(); !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
	t.Run("args order", func(t *testing.T) {
		cmd := newCmdMRanger(nameMRange, secondMillennium, thirdMillennium, []Filter{FilterEqual("l", "v")})
		want := []interface{}{
			secondMillennium.UnixMilli(), thirdMillennium.UnixMilli(),
			"FILTER_BY_TS", int64(1500000000000), int64(1600000000000),
			"FILTER_BY_VALUE", 0.2, 0.4,
			"WITHLABELS",
			"COUNT", int64(100),
			"ALIGN", int64(1500000000000),
			"AGGREGATION", "AVG", int64(1000),
			"FILTER", "l=v",
			"GROUPBY", "l", "REDUCE", "SUM",
		}
		MRangerWithTSFilter(time.Unix(1500000000, 0), time.Unix(1600000000, 0))(cmd)
		MRangerWithValueFilter(0.2, 0.4)(cmd)
		MRangerWithLabels()(cmd)
		MRangerWithCount(100)(cmd)
		MRangerWithAlign(time.Unix(1500000000, 0))(cmd)
		MRangerWithAggregation(AggregationTypeAvg, time.Second)(cmd)
		MRangerWithGroupBy("l", ReducerSum)(cmd)
		if got := cmd.Args(); !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
		cmd = newCmdMRanger(nameMRange, secondMillennium, thirdMillennium, []Filter{FilterEqual("l", "v")})
		MRangerWithGroupBy("l", ReducerSum)(cmd)
		MRangerWithAggregation(AggregationTypeAvg, time.Second)(cmd)
		MRangerWithAlign(time.Unix(1500000000, 0))(cmd)
		MRangerWithCount(100)(cmd)
		MRangerWithLabels()(cmd)
		MRangerWithValueFilter(0.2, 0.4)(cmd)
		MRangerWithTSFilter(time.Unix(1500000000, 0), time.Unix(1600000000, 0))(cmd)
		if got := cmd.Args(); !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
}

func TestClient_MRange(t *testing.T) {
	if testing.Short() {
		t.Skip("skip client test")
	}
	for _, tt := range doerTests {
		t.Run(tt.name, func(t *testing.T) {
			key := fmt.Sprintf("example:%s", t.Name())

			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			doer, err := tt.doer(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer doer.Close()
			defer doer.Do(context.Background(), "DEL", key)

			tsclient := NewClient(doer)
			if err = tsclient.Create(ctx, key, CreateWithLabels(NewLabel("l", "v"))); err != nil {
				t.Fatalf("Create() error = %v", err)
			}
			_, err = tsclient.MAdd(ctx, []Sample{
				NewSample(key, secondMillennium, 1),
				NewSample(key, thirdMillennium, 2),
			})
			if err != nil {
				t.Fatalf("MAdd() error = %v", err)
			}
			points, err := tsclient.MRange(ctx, TSMin(), TSMax(), []Filter{FilterEqual("l", "v")},
				MRangerWithLabels(),
			)
			if err != nil {
				t.Errorf("MRange() error = %v", err)
			}
			want := []TimeSeries{
				{
					Key:    key,
					Labels: []Label{{Name: "l", Value: "v"}},
					DataPoints: []DataPoint{
						{secondMillennium, 1.0},
						{thirdMillennium, 2.0},
					},
				},
			}
			if got := points; !reflect.DeepEqual(got, want) {
				t.Errorf("MRange() got = %v, want %v", got, want)
			}
		})
	}
}

func TestCmdGet(t *testing.T) {
	cmd := newCmdGet("key:any")
	if got, want := cmd.Name(), "TS.GET"; got != want {
		t.Errorf("Name() = %v, want %v", got, want)
	}
	if got, want := cmd.Args(), []interface{}{"key:any"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Args() = %v, want %v", got, want)
	}
}

func TestClient_Get(t *testing.T) {
	if testing.Short() {
		t.Skip("skip client test")
	}
	for _, tt := range doerTests {
		t.Run(tt.name, func(t *testing.T) {
			key := fmt.Sprintf("example:%s", t.Name())

			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			doer, err := tt.doer(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer doer.Close()
			defer doer.Do(context.Background(), "DEL", key)

			tsclient := NewClient(doer)
			if err = tsclient.Create(ctx, key); err != nil {
				t.Fatalf("Create() error = %v", err)
			}
			_, err = tsclient.MAdd(ctx, []Sample{
				NewSample(key, secondMillennium, 1),
				NewSample(key, thirdMillennium, 2),
			})
			if err != nil {
				t.Fatalf("MAdd() error = %v", err)
			}
			points, err := tsclient.Get(ctx, key)
			if err != nil {
				t.Errorf("Range() error = %v", err)
			}
			want := DataPoint{thirdMillennium, 2.0}
			if got := points; !reflect.DeepEqual(got, want) {
				t.Errorf("Range() got = %v, want %v", got, want)
			}
		})
	}
}

func TestCmdMGet(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		cmd := newCmdMGet([]Filter{FilterEqual("l", "v")})
		if got, want := cmd.Name(), "TS.MGET"; got != want {
			t.Errorf("Name() = %v, want %v", got, want)
		}
		if got, want := cmd.Args(), []interface{}{"FILTER", "l=v"}; !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
	t.Run("with labels", func(t *testing.T) {
		cmd := newCmdMGet([]Filter{FilterEqual("l", "v")})
		MGetWithLabels()(cmd)
		want := []interface{}{
			"WITHLABELS",
			"FILTER", "l=v",
		}
		if got := cmd.Args(); !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
	t.Run("selected labels", func(t *testing.T) {
		cmd := newCmdMGet([]Filter{FilterEqual("l", "v")})
		MGetWithLabels("l")(cmd)
		want := []interface{}{
			"SELECTED_LABELS", "l",
			"FILTER", "l=v",
		}
		if got := cmd.Args(); !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
}

func TestClient_MGet(t *testing.T) {
	if testing.Short() {
		t.Skip("skip client test")
	}
	for _, tt := range doerTests {
		t.Run(tt.name, func(t *testing.T) {
			key := fmt.Sprintf("example:%s", t.Name())

			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			doer, err := tt.doer(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer doer.Close()
			defer doer.Do(context.Background(), "DEL", key)

			tsclient := NewClient(doer)
			if err = tsclient.Create(ctx, key, CreateWithLabels(NewLabel("l", "v"))); err != nil {
				t.Fatalf("Create() error = %v", err)
			}
			_, err = tsclient.MAdd(ctx, []Sample{
				NewSample(key, secondMillennium, 1),
				NewSample(key, thirdMillennium, 2),
			})
			if err != nil {
				t.Fatalf("MAdd() error = %v", err)
			}
			points, err := tsclient.MGet(ctx, []Filter{FilterEqual("l", "v")},
				MGetWithLabels(),
			)
			if err != nil {
				t.Errorf("MRange() error = %v", err)
			}
			want := []LastDatapoint{
				{
					Key:       key,
					Labels:    []Label{{Name: "l", Value: "v"}},
					DataPoint: DataPoint{thirdMillennium, 2.0},
				},
			}
			if got := points; !reflect.DeepEqual(got, want) {
				t.Errorf("MRange() got = %v, want %v", got, want)
			}
		})
	}
}
