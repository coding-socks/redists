/*
Package redists is a typesafe Go client for RedisTimeSeries. It tries to
support multiple Redis clients, because applications probably already use one.

Creating a client with GoRedis

The following example shows how to create a Doer implementation with GoRedis:

	package main

	import (
		"context"
		goredis "github.com/go-redis/redis/v8"
		"github.com/coding-socks/redists"
		"time"
	)

	type goredisDoer struct {
		c *goredis.Client
	}

	func (f goredisDoer) Do(ctx context.Context, cmd string, args ...interface{}) (interface{}, error) {
		args = append([]interface{}{cmd}, args...)
		return f.c.Do(ctx, args...).Result()
	}

	func main() {
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

Creating a client with Redigo

The following example shows how to create a Doer implementation with Redigo:

	package main

	import (
		"context"
		redigo "github.com/gomodule/redigo/redis"
		"github.com/coding-socks/redists"
		"time"
	)

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

	func main() {
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

Creating a client with RedisPipe

The following example shows how to create a Doer implementation with RedisPipe:

	package main

	import (
		"context"
		redispipe "github.com/joomcode/redispipe/redis"
		"github.com/joomcode/redispipe/redisconn"
		"github.com/coding-socks/redists"
		"time"
	)

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

	func main() {
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

Creating a client with Radix

The following example shows how to create a Doer implementation with Radix:

	package main

	import (
		"context"
		"github.com/mediocregopher/radix/v4"
		"github.com/coding-socks/redists"
		"time"
	)

	type radixDoer struct {
		c radix.Client
	}

	func (f radixDoer) Do(ctx context.Context, cmd string, args ...interface{}) (interface{}, error) {
		var val interface{}
		err := f.c.Do(ctx, radix.FlatCmd(&val, cmd, args...))
		if e, ok := val.(error); err == nil && ok {
			return nil, e
		}
		return val, err
	}

	func main() {
		d := radix.Dialer{
			NewRespOpts: func() *resp.Opts {
				opts := resp.NewOpts()
				opts.DisableErrorBubbling = true
				return opts
			},
		}
		client, err := (radix.PoolConfig{Dialer: d}).New(context.Background(), "tcp", "localhost:6379")
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

*/
package redists
