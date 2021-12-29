package redists

import (
	"context"
	"fmt"
	goredis "github.com/go-redis/redis/v8"
	redigo "github.com/gomodule/redigo/redis"
	redispipe "github.com/joomcode/redispipe/redis"
	"github.com/joomcode/redispipe/redisconn"
	"github.com/mediocregopher/radix/v4"
	"io"
	"testing"
	"time"
)

type goredisDoer struct {
	goredis.UniversalClient
}

func (f goredisDoer) Do(ctx context.Context, cmd string, args ...interface{}) (interface{}, error) {
	args = append([]interface{}{cmd}, args...)
	return f.UniversalClient.Do(ctx, args...).Result()
}

type redigoDoer struct {
	redigo.Conn
}

func (f redigoDoer) Do(ctx context.Context, cmd string, args ...interface{}) (interface{}, error) {
	deadline, ok := ctx.Deadline()
	if ok {
		return redigo.DoWithTimeout(f.Conn, deadline.Sub(time.Now()), cmd, args...)
	}
	return f.Conn.Do(cmd, args...)
}

type radixDoer struct {
	radix.Client
}

func (f radixDoer) Do(ctx context.Context, cmd string, args ...interface{}) (interface{}, error) {
	var val interface{}
	err := f.Client.Do(ctx, radix.FlatCmd(&val, cmd, args...))
	return val, err
}

type redispipeDoer struct {
	redispipe.Sender
}

func (f redispipeDoer) Do(ctx context.Context, cmd string, args ...interface{}) (interface{}, error) {
	res := redispipe.SyncCtx{f.Sender}.Do(ctx, cmd, args...)
	if err := redispipe.AsError(res); err != nil {
		return nil, err
	}
	return res, nil
}

func (f redispipeDoer) Close() error {
	f.Sender.Close()
	return nil
}

type doCloser interface {
	Doer
	io.Closer
}

var doerTests = []struct {
	name string
	doer func(ctx context.Context) (doCloser, error)
}{
	{
		name: "goredis",
		doer: func(ctx context.Context) (doCloser, error) {
			client := goredis.NewClient(&goredis.Options{Addr: "localhost:6379"})
			if err := client.Ping(ctx).Err(); err != nil {
				return nil, err
			}
			return goredisDoer{client}, nil
		},
	},
	{
		name: "redigo",
		doer: func(ctx context.Context) (doCloser, error) {
			conn, err := redigo.DialContext(ctx, "tcp", "localhost:6379")
			if err != nil {
				return nil, err
			}
			return redigoDoer{conn}, nil
		},
	},
	{
		name: "radix",
		doer: func(ctx context.Context) (doCloser, error) {
			client, err := (radix.PoolConfig{}).New(ctx, "tcp", "localhost:6379")
			if err != nil {
				return nil, err
			}
			return radixDoer{client}, err
		},
	},
	{
		name: "redispipe",
		doer: func(ctx context.Context) (doCloser, error) {
			sender, err := redisconn.Connect(ctx, "localhost:6379", redisconn.Opts{})
			if err != nil {
				return nil, err
			}
			return redispipeDoer{sender}, nil
		},
	},
}

func TestNewClient(t *testing.T) {
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
			if err := tsclient.Create(ctx, key); err != nil {
				t.Errorf("Create() error = %v", err)
			}
		})
	}
}
