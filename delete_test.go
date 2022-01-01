package redists

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestCmdDel(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		cmd := newCmdDel("key:any", time.UnixMilli(1001), time.UnixMilli(1002))
		if got, want := cmd.Name(), "TS.DEL"; got != want {
			t.Errorf("Name() = %v, want %v", got, want)
		}
		if got, want := cmd.Args(), []interface{}{"key:any", int64(1001), int64(1002)}; !reflect.DeepEqual(got, want) {
			t.Errorf("Args() = %v, want %v", got, want)
		}
	})
}

func TestClient_Del(t *testing.T) {
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
			got, err := tsclient.Del(ctx, key, secondMillennium, thirdMillennium)
			if err != nil {
				t.Fatalf("Del() error = %v", err)
			}
			if want := int64(2); got != want {
				t.Errorf("Del() got = %v, want %v", got, want)
			}
		})
	}
}
