package redists

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestCmdAlter(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		cmd := newCmdAlter("key:any")
		if got, want := cmd.Name(), "TS.ALTER"; got != want {
			t.Errorf("Name() = %v, want %v", got, want)
		}
		if got, want := cmd.Args(), []interface{}{"key:any"}; !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
	t.Run("retention", func(t *testing.T) {
		cmd := newCmdAlter("key:any")
		AlterWithRetention(time.Second)(cmd)
		if got, want := cmd.Args(), []interface{}{"key:any", "RETENTION", int64(1000)}; !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
	t.Run("chunk size", func(t *testing.T) {
		cmd := newCmdAlter("key:any")
		AlterWithChunkSize(8)(cmd)
		if got, want := cmd.Args(), []interface{}{"key:any", "CHUNK_SIZE", 8}; !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
	t.Run("duplicate policy", func(t *testing.T) {
		cmd := newCmdAlter("key:any")
		AlterWithDuplicatePolicy(DuplicatePolicyBlock)(cmd)
		if got, want := cmd.Args(), []interface{}{"key:any", "DUPLICATE_POLICY", "BLOCK"}; !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
	t.Run("label", func(t *testing.T) {
		cmd := newCmdAlter("key:any")
		AlterWithLabels(Labels{
			"label:any":   "value:any",
			"label:other": "value:other",
		})(cmd)
		if got, want := cmd.Args(), []interface{}{"key:any", "LABELS", "label:any", "value:any", "label:other", "value:other"}; !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
	t.Run("args order", func(t *testing.T) {
		cmd := newCmdAlter("key:any")
		want := []interface{}{
			"key:any",
			"RETENTION", int64(1000),
			"CHUNK_SIZE", 8,
			"DUPLICATE_POLICY", "BLOCK",
			"LABELS", "label:any", "value:any",
		}
		AlterWithRetention(time.Second)(cmd)
		AlterWithChunkSize(8)(cmd)
		AlterWithDuplicatePolicy(DuplicatePolicyBlock)(cmd)
		AlterWithLabels(Labels{"label:any": "value:any"})(cmd)
		if got := cmd.Args(); !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
		cmd = newCmdAlter("key:any")
		AlterWithLabels(Labels{"label:any": "value:any"})(cmd)
		AlterWithDuplicatePolicy(DuplicatePolicyBlock)(cmd)
		AlterWithChunkSize(8)(cmd)
		AlterWithRetention(time.Second)(cmd)
		if got := cmd.Args(); !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
}

func TestClient_Alter(t *testing.T) {
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
			err = tsclient.Create(ctx, key,
				CreateWithRetention(time.Hour),
				CreateWithLabels(Labels{"l": "v"}),
			)
			if err != nil {
				t.Fatalf("Create() error = %v", err)
			}
			err = tsclient.Alter(ctx, key,
				AlterWithRetention(time.Minute),
				AlterWithLabels(Labels{"ll": "vv"}),
			)
			if err != nil {
				t.Fatalf("Alter() error = %v", err)
			}
			inf, err := tsclient.Info(ctx, key)
			if err != nil {
				t.Fatalf("Info() error = %v", err)
			}
			if got, want := inf.RetentionTime, time.Minute; got != want {
				t.Errorf("Info().RetentionTime got = %v, want %v", got, want)
			}
			if got, want := inf.Labels, (Labels{"ll": "vv"}); !reflect.DeepEqual(got, want) {
				t.Errorf("Info().Labels got = %v, want %v", got, want)
			}
			err = tsclient.Alter(ctx, key,
				AlterWithLabels(Labels{}),
			)
			if err != nil {
				t.Fatalf("Alter() error = %v", err)
			}
			inf, err = tsclient.Info(ctx, key)
			if err != nil {
				t.Fatalf("Info() error = %v", err)
			}
			if got, want := inf.RetentionTime, time.Minute; got != want {
				t.Errorf("Info().RetentionTime got = %v, want %v", got, want)
			}
			if got, want := inf.Labels, (Labels{}); !reflect.DeepEqual(got, want) {
				t.Errorf("Info().Labels got = %v, want %v", got, want)
			}
		})
	}
}

func TestCmdAdd(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		cmd := newCmdAdd(NewSample("key:any", time.UnixMilli(1001), 0.5))
		if got, want := cmd.Name(), "TS.ADD"; got != want {
			t.Errorf("Name() = %v, want %v", got, want)
		}
		if got, want := cmd.Args(), []interface{}{"key:any", int64(1001), 0.5}; !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
	t.Run("retention", func(t *testing.T) {
		cmd := newCmdAdd(NewSample("key:any", time.UnixMilli(1001), 0.5))
		AddWithRetention(time.Second)(cmd)
		if got, want := cmd.Args(), []interface{}{"key:any", int64(1001), 0.5, "RETENTION", int64(1000)}; !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
	t.Run("encoding", func(t *testing.T) {
		cmd := newCmdAdd(NewSample("key:any", time.UnixMilli(1001), 0.5))
		AddWithEncoding(EncodingCompressed)(cmd)
		if got, want := cmd.Args(), []interface{}{"key:any", int64(1001), 0.5, "ENCODING", "COMPRESSED"}; !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
	t.Run("chunk size", func(t *testing.T) {
		cmd := newCmdAdd(NewSample("key:any", time.UnixMilli(1001), 0.5))
		AddWithChunkSize(8)(cmd)
		if got, want := cmd.Args(), []interface{}{"key:any", int64(1001), 0.5, "CHUNK_SIZE", 8}; !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
	t.Run("duplicate policy", func(t *testing.T) {
		cmd := newCmdAdd(NewSample("key:any", time.UnixMilli(1001), 0.5))
		AddWithOnDuplicate(DuplicatePolicyBlock)(cmd)
		if got, want := cmd.Args(), []interface{}{"key:any", int64(1001), 0.5, "ON_DUPLICATE", "BLOCK"}; !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
	t.Run("labels", func(t *testing.T) {
		cmd := newCmdAdd(NewSample("key:any", time.UnixMilli(1001), 0.5))
		AddWithLabels(Labels{
			"label:any":   "value:any",
			"label:other": "value:other",
		})(cmd)
		if got, want := cmd.Args(), []interface{}{"key:any", int64(1001), 0.5, "LABELS", "label:any", "value:any", "label:other", "value:other"}; !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
	t.Run("args order", func(t *testing.T) {
		cmd := newCmdAdd(NewSample("key:any", time.UnixMilli(1001), 0.5))
		want := []interface{}{
			"key:any", int64(1001), 0.5,
			"RETENTION", int64(1000),
			"ENCODING", "COMPRESSED",
			"CHUNK_SIZE", 8,
			"ON_DUPLICATE", "BLOCK",
			"LABELS", "label:any", "value:any",
		}
		AddWithRetention(time.Second)(cmd)
		AddWithEncoding(EncodingCompressed)(cmd)
		AddWithChunkSize(8)(cmd)
		AddWithOnDuplicate(DuplicatePolicyBlock)(cmd)
		AddWithLabels(Labels{"label:any": "value:any"})(cmd)
		if got := cmd.Args(); !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
		cmd = newCmdAdd(NewSample("key:any", time.UnixMilli(1001), 0.5))
		AddWithLabels(Labels{"label:any": "value:any"})(cmd)
		AddWithOnDuplicate(DuplicatePolicyBlock)(cmd)
		AddWithChunkSize(8)(cmd)
		AddWithEncoding(EncodingCompressed)(cmd)
		AddWithRetention(time.Second)(cmd)
		if got := cmd.Args(); !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
}

func TestClient_Add(t *testing.T) {
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
			got, err := tsclient.Add(ctx, NewSample(key, secondMillennium, 1), AddWithRetention(time.Hour))
			if err != nil {
				t.Fatalf("Add() error = %v", err)
			}
			if want := secondMillennium; got != want {
				t.Errorf("Add() got = %v, want %v", got, want)
			}
			inf, err := tsclient.Info(ctx, key)
			if err != nil {
				t.Fatalf("Info() error = %v", err)
			}
			if got, want := inf.TotalSamples, int64(1); got != want {
				t.Errorf("Info().TotalSamples got = %v, want %v", got, want)
			}
			if got, want := inf.RetentionTime, time.Hour; got != want {
				t.Errorf("Info().RetentionTime got = %v, want %v", got, want)
			}
		})
	}
}

func TestCmdMAdd(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		cmd := newCmdMAdd([]Sample{
			NewSample("key:any", time.UnixMilli(1001), 0.5),
			NewSample("key:any", TSAuto(), 0.5),
		})
		if got, want := cmd.Name(), "TS.MADD"; got != want {
			t.Errorf("Name() = %v, want %v", got, want)
		}
		if got, want := cmd.Args(), []interface{}{"key:any", int64(1001), 0.5, "key:any", "*", 0.5}; !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
}

func TestClient_MAdd(t *testing.T) {
	if testing.Short() {
		t.Skip("skip client test")
	}
	for _, tt := range doerTests {
		t.Run(tt.name, func(t *testing.T) {
			key := fmt.Sprintf("example:%s", t.Name())
			unknownKey := fmt.Sprintf("unknown:%s", t.Name())

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
			got, err := tsclient.MAdd(ctx, []Sample{
				NewSample(key, secondMillennium, 1),
				NewSample(unknownKey, secondMillennium, 1),
				NewSample(key, thirdMillennium, 2),
			})
			if tt.name == "radix" { // https://github.com/mediocregopher/radix/issues/305
				if wantErr := true; (err != nil) != wantErr {
					t.Errorf("MAdd() error = %v, wantErr = %v", err, wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("MAdd() error = %v", err)
			}
			if got, want := len(got), 3; got != want {
				t.Fatalf("len(MAdd()) got = %v, want %v", got, want)
			}
			if err, wantErr := got[0].Err(), false; (err != nil) != wantErr {
				t.Errorf("MAdd()[0] error = %v, wantErr = %v", err, wantErr)
			}
			if got, want := got[0], (MultiResult{t: secondMillennium}); got != want {
				t.Errorf("MAdd()[0] got = %v, want %v", got, want)
			}
			if err, wantErr := got[1].Err(), true; (err != nil) != wantErr {
				t.Errorf("MAdd()[1] error = %v, wantErr = %v", err, wantErr)
			}
			if err, wantErr := got[2].Err(), false; (err != nil) != wantErr {
				t.Errorf("MAdd()[2] error = %v, wantErr = %v", err, wantErr)
			}
			if got, want := got[2], (MultiResult{t: thirdMillennium}); got != want {
				t.Errorf("MAdd()[2] got = %v, want %v", got, want)
			}
		})
	}
}

func TestCmdCounter(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		cmd := newCmdCounter(nameIncrBy, "key:any", 0.5)
		if got, want := cmd.Name(), "TS.INCRBY"; got != want {
			t.Errorf("Name() = %v, want %v", got, want)
		}
		if got, want := cmd.Args(), []interface{}{"key:any", 0.5}; !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
		cmd = newCmdCounter(nameDecrBy, "key:any", 0.5)
		if got, want := cmd.Name(), "TS.DECRBY"; got != want {
			t.Errorf("Name() = %v, want %v", got, want)
		}
		if got, want := cmd.Args(), []interface{}{"key:any", 0.5}; !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
	t.Run("timestamp", func(t *testing.T) {
		cmd := newCmdCounter(nameIncrBy, "key:any", 0.5)
		CounterWithTimestamp(time.UnixMilli(1001))(cmd)
		if got, want := cmd.Args(), []interface{}{"key:any", 0.5, "TIMESTAMP", int64(1001)}; !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
	t.Run("retention", func(t *testing.T) {
		cmd := newCmdCounter(nameIncrBy, "key:any", 0.5)
		CounterWithRetention(time.Second)(cmd)
		if got, want := cmd.Args(), []interface{}{"key:any", 0.5, "RETENTION", int64(1000)}; !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
	t.Run("encoding", func(t *testing.T) {
		cmd := newCmdCounter(nameIncrBy, "key:any", 0.5)
		CounterWithEncoding(EncodingUncompressed)(cmd)
		if got, want := cmd.Args(), []interface{}{"key:any", 0.5, "UNCOMPRESSED"}; !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
		CounterWithEncoding(EncodingCompressed)(cmd)
		if got, want := cmd.Args(), []interface{}{"key:any", 0.5}; !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
	t.Run("chunk size", func(t *testing.T) {
		cmd := newCmdCounter(nameIncrBy, "key:any", 0.5)
		CounterWithChunkSize(8)(cmd)
		if got, want := cmd.Args(), []interface{}{"key:any", 0.5, "CHUNK_SIZE", 8}; !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
	t.Run("labels", func(t *testing.T) {
		cmd := newCmdCounter(nameIncrBy, "key:any", 0.5)
		CounterWithLabels(Labels{
			"label:any":   "value:any",
			"label:other": "value:other",
		})(cmd)
		if got, want := cmd.Args(), []interface{}{"key:any", 0.5, "LABELS", "label:any", "value:any", "label:other", "value:other"}; !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
	t.Run("args order", func(t *testing.T) {
		cmd := newCmdCounter(nameIncrBy, "key:any", 0.5)
		want := []interface{}{
			"key:any", 0.5,
			"TIMESTAMP", int64(1001),
			"RETENTION", int64(1000),
			"UNCOMPRESSED",
			"CHUNK_SIZE", 8,
			"LABELS", "label:any", "value:any",
		}
		CounterWithTimestamp(time.UnixMilli(1001))(cmd)
		CounterWithRetention(time.Second)(cmd)
		CounterWithEncoding(EncodingUncompressed)(cmd)
		CounterWithChunkSize(8)(cmd)
		CounterWithLabels(Labels{"label:any": "value:any"})(cmd)
		if got := cmd.Args(); !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
		cmd = newCmdCounter(nameIncrBy, "key:any", 0.5)
		CounterWithLabels(Labels{"label:any": "value:any"})(cmd)
		CounterWithChunkSize(8)(cmd)
		CounterWithEncoding(EncodingUncompressed)(cmd)
		CounterWithRetention(time.Second)(cmd)
		CounterWithTimestamp(time.UnixMilli(1001))(cmd)
		if got := cmd.Args(); !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
}

func TestClient_IncrBy(t *testing.T) {
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
			got, err := tsclient.IncrBy(ctx, key, 0.5, CounterWithRetention(time.Hour))
			if err != nil {
				t.Fatalf("IncrBy() error = %v", err)
			}
			if want := time.Now(); !want.Add(-time.Second).Before(got) && !want.Add(time.Second).After(got) {
				t.Errorf("IncrBy() got = %v, want %v", got, want)
			}
			inf, err := tsclient.Info(ctx, key)
			if err != nil {
				t.Fatalf("Info() error = %v", err)
			}
			if got, want := inf.TotalSamples, int64(1); got != want {
				t.Errorf("Info().TotalSamples got = %v, want %v", got, want)
			}
			if got, want := inf.RetentionTime, time.Hour; got != want {
				t.Errorf("Info().RetentionTime got = %v, want %v", got, want)
			}
		})
	}
}
