package redists

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestCmdCreateRule(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		cmd := newCmdCreateRule("key:src", "key:dst", AggregationTypeAvg, time.Second)
		if got, want := cmd.Name(), "TS.CREATERULE"; got != want {
			t.Errorf("Name() = %v, want %v", got, want)
		}
		if got, want := cmd.Args(), []interface{}{"key:src", "key:dst", "AGGREGATION", "AVG", int64(1000)}; !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
}

func TestClient_CreateRule(t *testing.T) {
	if testing.Short() {
		t.Skip("skip client test")
	}
	for _, tt := range doerTests {
		t.Run(tt.name, func(t *testing.T) {
			srcKey := fmt.Sprintf("example:src:%s", t.Name())
			destKey := fmt.Sprintf("example:dest:%s", t.Name())

			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			doer, err := tt.doer(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer doer.Close()
			defer doer.Do(context.Background(), "DEL", srcKey, destKey)

			tsclient := NewClient(doer)
			if err = tsclient.Create(ctx, srcKey); err != nil {
				t.Fatalf("Create() error = %v", err)
			}
			if err := tsclient.Create(ctx, destKey); err != nil {
				t.Fatalf("Create() error = %v", err)
			}
			if err = tsclient.CreateRule(ctx, srcKey, destKey, AggregationTypeAvg, time.Minute); err != nil {
				t.Fatalf("CreateRule() error = %v", err)
			}
			inf, err := tsclient.Info(ctx, srcKey)
			if err != nil {
				t.Fatalf("Info() error = %v", err)
			}
			want := []Rule{{Key: destKey, Aggregation: Aggregation{AggregationTypeAvg, time.Minute}}}
			if got, want := inf.Rules, want; !reflect.DeepEqual(got, want) {
				t.Errorf("Info().Rules got = %v, want %v", got, want)
			}
		})
	}
}

func TestCmdDeleteRule(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		cmd := newCmdDeleteRule("key:src", "key:dst")
		if got, want := cmd.Name(), "TS.DELETERULE"; got != want {
			t.Errorf("Name() = %v, want %v", got, want)
		}
		if got, want := cmd.Args(), []interface{}{"key:src", "key:dst"}; !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
}

func TestClient_DeleteRule(t *testing.T) {
	if testing.Short() {
		t.Skip("skip client test")
	}
	for _, tt := range doerTests {
		t.Run(tt.name, func(t *testing.T) {
			srcKey := fmt.Sprintf("example:src:%s", t.Name())
			destKey := fmt.Sprintf("example:dest:%s", t.Name())

			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			doer, err := tt.doer(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer doer.Close()
			defer doer.Do(context.Background(), "DEL", srcKey, destKey)

			tsclient := NewClient(doer)
			if err = tsclient.Create(ctx, srcKey); err != nil {
				t.Fatalf("Create() error = %v", err)
			}
			if err := tsclient.Create(ctx, destKey); err != nil {
				t.Fatalf("Create() error = %v", err)
			}
			if err = tsclient.CreateRule(ctx, srcKey, destKey, AggregationTypeAvg, time.Minute); err != nil {
				t.Fatalf("CreateRule() error = %v", err)
			}
			inf, err := tsclient.Info(ctx, srcKey)
			if err != nil {
				t.Fatalf("Info() error = %v", err)
			}
			want := []Rule{{Key: destKey, Aggregation: Aggregation{AggregationTypeAvg, time.Minute}}}
			if got, want := inf.Rules, want; !reflect.DeepEqual(got, want) {
				t.Errorf("Info().Rules got = %v, want %v", got, want)
			}
			if err = tsclient.DeleteRule(ctx, srcKey, destKey); err != nil {
				t.Fatalf("DeleteRule() error = %v", err)
			}
			inf, err = tsclient.Info(ctx, srcKey)
			if err != nil {
				t.Fatalf("Info() error = %v", err)
			}
			want = nil
			if got, want := inf.Rules, want; !reflect.DeepEqual(got, want) {
				t.Errorf("Info().Rules got = %v, want %v", got, want)
			}
		})
	}
}
