# RedisTS

RedisTS is a typesafe Go client for [RedisTimeSeries](github.com/RedisTimeSeries/RedisTimeSeries).

[![Go Reference](https://pkg.go.dev/badge/github.com/nerg4l/redists.svg)](https://pkg.go.dev/github.com/nerg4l/redists)

RedisTimeSeries documentation: https://oss.redis.com/redistimeseries/commands/

This library tries to support multiple Redis clients, because applications probably already use one. There are examples in the reference for `github.com/go-redis/redis/v8`, `github.com/gomodule/redigo`, `github.com/joomcode/redispipe`, and `github.com/mediocregopher/radix/v4` demonstrating how one can create a new RedisTS client using them.

## Focus

RedisTS was created during a coding spree which had the following focus:

1. [Functional options for friendly APIs](https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis).
2. Type safety.
3. Switched word order for better autocompletion and to follow the naming scheme used in stdlib (e.g. `http.MethodGet`, `http.StatusNotFound`).
4. Compatibility with multiple Redis clients.

## Get module

```
go get -u github.com/nerg4l/redists
```

## Running tests

```
go test
```

The tests expect a Redis server with RedisTimeSeries `^v1.6` module to be available at `localhost:6379`. One can use `-test.short` to skip those tests.

```
go test -test.short
```

Below you can find an example code to run a Redis server with "edge" version of RedisTimeSeries via docker.

```
docker run --name dev-redists -p 6379:6379 -d redislabs/redistimeseries:edge
```

## Supported clients

RedisTS is tested with the following clients:

- `github.com/go-redis/redis/v8`
- `github.com/gomodule/redigo`
- `github.com/joomcode/redispipe`
- `github.com/mediocregopher/radix/v4`

It probably works with others, but it's not guaranteed. Feel free to open an issue to get support for other clients, because if it isn't too much effort it will be added to the list above.

**!IMPORTANT!** `MAdd` will not return a list of results in case of `github.com/mediocregopher/radix/v4` an issue was already opened: https://github.com/mediocregopher/radix/issues/305

## Production readiness

This project is still in alpha phase. In this stage the public API can change multiple times a day.

Beta version will be considered when the feature set covers the documents the implementation is based on, and the public API reaches a mature state.

## Alternative libraries

- [RedisTimeSeries/redistimeseries-go](https://github.com/RedisTimeSeries/redistimeseries-go)
- [rueian/rueidis](https://github.com/rueian/rueidis)

## Contribution

Any type of contribution is welcome; from features, bug fixes, documentation improvements, feedbacks, questions. While GitHub uses the word "issue" feel free to open up a GitHub issue for any of these.

## License

RedisTS is distributed under the MIT license.
