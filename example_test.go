package redists_test

import (
	"context"
	goredis "github.com/go-redis/redis/v8"
	redigo "github.com/gomodule/redigo/redis"
	redispipe "github.com/joomcode/redispipe/redis"
	"github.com/joomcode/redispipe/redisconn"
	"github.com/mediocregopher/radix/v4"
	"github.com/nerg4l/redists"
	"time"
)

type goredisDoer struct {
	c *goredis.Client
}

func (f goredisDoer) Do(ctx context.Context, cmd string, args ...interface{}) (interface{}, error) {
	args = append([]interface{}{cmd}, args...)
	return f.c.Do(ctx, args...).Result()
}

func ExampleNewClient_goredis() {
	client := goredis.NewClient(&goredis.Options{
		Addr:     "localhost:6379", // use default Addr
		Password: "",               // no password set
		DB:       0,                // use default DB
	})
	defer client.Close()
	err := client.Ping(context.Background()).Err()
	if err != nil {
		panic(err)
	}
	tsclient := redists.NewClient(goredisDoer{client})

	v := client.Exists(context.Background(), "example:goredis").Val()
	if v == 0 {
		err = tsclient.Create(context.Background(), "example:goredis")
		if err != nil {
			panic(err)
		}
	}
	_, err = tsclient.Add(context.Background(), redists.NewSample("example:goredis", redists.TSAuto(), 0.5))
	if err != nil {
		panic(err)
	}

}

type redigoDoer struct {
	c redigo.Conn
}

func (f redigoDoer) Do(ctx context.Context, cmd string, args ...interface{}) (interface{}, error) {
	deadline, ok := ctx.Deadline()
	if ok {
		return redigo.DoWithTimeout(f.c, time.Now().Sub(deadline), cmd, args...)
	}
	return f.c.Do(cmd, args...)
}

func ExampleNewClient_redigo() {
	conn, err := redigo.Dial("tcp", "localhost:6379")
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	tsclient := redists.NewClient(redigoDoer{conn})

	v, _ := redigo.Int(conn.Do("EXISTS", "example:redigo"))
	if v == 0 {
		err = tsclient.Create(context.Background(), "example:redigo")
		if err != nil {
			panic(err)
		}
	}
	_, err = tsclient.Add(context.Background(), redists.NewSample("example:redigo", redists.TSAuto(), 0.5))
	if err != nil {
		panic(err)
	}
}

type radixDoer struct {
	c radix.Client
}

func (f radixDoer) Do(ctx context.Context, cmd string, args ...interface{}) (interface{}, error) {
	var val interface{}
	err := f.c.Do(ctx, radix.FlatCmd(&val, cmd, args...))
	return val, err
}

func ExampleNewClient_radix() {
	client, err := (radix.PoolConfig{}).New(context.Background(), "tcp", "localhost:6379")
	if err != nil {
		panic(err)
	}
	defer client.Close()
	tsclient := redists.NewClient(radixDoer{client})

	var v int
	client.Do(context.Background(), radix.Cmd(&v, "EXISTS", "example:radix"))
	if v == 0 {
		err = tsclient.Create(context.Background(), "example:radix")
		if err != nil {
			panic(err)
		}
	}
	_, err = tsclient.Add(context.Background(), redists.NewSample("example:radix", redists.TSAuto(), 0.5))
	if err != nil {
		panic(err)
	}
}

type redispipeDoer struct {
	s redispipe.Sender
}

func (f redispipeDoer) Do(ctx context.Context, cmd string, args ...interface{}) (interface{}, error) {
	res := redispipe.SyncCtx{f.s}.Do(ctx, cmd, args...)
	if err := redispipe.AsError(res); err != nil {
		return nil, err
	}
	return res, nil
}

func ExampleNewClient_redispipe() {
	sender, err := redisconn.Connect(context.Background(), "localhost:6379", redisconn.Opts{})
	defer sender.Close()
	if err != nil {
		panic(err)
	}
	tsclient := redists.NewClient(redispipeDoer{sender})

	sync := redispipe.SyncCtx{sender}
	res := sync.Do(context.Background(), "EXISTS", "example:redispipe")
	if res.(int64) == 0 {
		err = tsclient.Create(context.Background(), "example:redispipe")
		if err != nil {
			panic(err)
		}
	}
	_, err = tsclient.Add(context.Background(), redists.NewSample("example:redispipe", redists.TSAuto(), 0.5))
	if err != nil {
		panic(err)
	}
}
