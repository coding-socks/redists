package redists

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestCmdInfo(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		cmd := newCmdInfo("key:any")
		if got, want := cmd.Name(), "TS.INFO"; got != want {
			t.Errorf("Name() = %v, want %v", got, want)
		}
		if got, want := cmd.Args(), []interface{}{"key:any"}; !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
	t.Run("debug", func(t *testing.T) {
		cmd := newCmdInfo("key:any")
		InfoWithDebug()(cmd)
		if got, want := cmd.Args(), []interface{}{"key:any", "DEBUG"}; !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
}

func TestClient_Info(t *testing.T) {
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
			_, err = tsclient.Info(ctx, key)
			if wantErr := true; (err != nil) != wantErr {
				t.Fatalf("Info() error = %v, wantErr = %v", err, wantErr)
			}
			err = tsclient.Create(ctx, key,
				CreateWithRetention(time.Hour),
				CreateWithLabels(Labels{"l": "v"}),
			)
			if err != nil {
				t.Fatalf("Create() error = %v", err)
			}
			if _, err := tsclient.Add(ctx, NewSample(key, secondMillennium, 1)); err != nil {
				t.Fatalf("Add() error = %v", err)
			}
			inf, err := tsclient.Info(ctx, key)
			if err != nil {
				t.Fatalf("Info() error = %v", err)
			}
			if got, want := inf.RetentionTime, time.Hour; got != want {
				t.Errorf("Info().RetentionTime got = %v, want %v", got, want)
			}
			if got, want := inf.FirstTimestamp, secondMillennium; got != want {
				t.Errorf("Info().FirstTimestamp got = %v, want %v", got, want)
			}
			if got, want := inf.Labels, (Labels{"l": "v"}); !reflect.DeepEqual(got, want) {
				t.Errorf("Info().Labels got = %v, want %v", got, want)
			}
			inf, err = tsclient.Info(ctx, key, InfoWithDebug())
			if err != nil {
				t.Fatalf("Info() error = %v", err)
			}
			if got, want := len(inf.Chunks), 1; got != want {
				t.Fatalf("len(Info().Chunks) got = %v, want %v", got, want)
			}
			if got, want := inf.Chunks[0].StartTimestamp, secondMillennium; got != want {
				t.Errorf("Info().Chunks[0].StartTimestamp got = %v, want %v", got, want)
			}
		})
	}
}

func TestCmdQueryIndex(t *testing.T) {
	cmd := newCmdQueryIndex([]Filter{FilterEqual("l1", "v1"), FilterNotEqual("l2", "v2")})
	if got, want := cmd.Name(), "TS.QUERYINDEX"; got != want {
		t.Errorf("Name() = %v, want %v", got, want)
	}
	if got, want := cmd.Args(), []interface{}{"l1=v1", "l2!=v2"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Args() = %v, want %v", got, want)
	}
}

func TestClient_QueryIndex(t *testing.T) {
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
			if err := tsclient.Create(ctx, key, CreateWithLabels(Labels{"l": "v"})); err != nil {
				t.Fatalf("Create() error = %v", err)
			}
			idxs, err := tsclient.QueryIndex(ctx, []Filter{FilterEqual("l", "v")})
			if err != nil {
				t.Errorf("QueryIndex() error = %v", err)
			}
			if got, want := idxs, []string{key}; !reflect.DeepEqual(got, want) {
				t.Errorf("QueryIndex() got = %v, want %v", got, want)
			}
		})
	}
}
