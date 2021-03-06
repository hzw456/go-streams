package main

import (
	"context"
	"strings"
	"time"

	"github.com/go-redis/redis"
	ext "github.com/hzw456/go-streams/extension"
	"github.com/hzw456/go-streams/flow"
	"github.com/hzw456/go-streams/util"
)

//docker exec -it pubsub bash
//https://redis.io/topics/pubsub
func main() {
	ctx, cancelFunc := context.WithCancel(context.Background())

	timer := time.NewTimer(time.Minute)
	go func() {
		select {
		case <-timer.C:
			cancelFunc()
		}
	}()

	config := &redis.Options{
		Addr:     "localhost:6379", // use default Addr
		Password: "",               // no password set
		DB:       0,                // use default DB
	}

	source, err := ext.NewRedisSource(ctx, config, "test")
	util.Check(err)
	flow1 := flow.NewMap(toUpper, 1)
	sink := ext.NewRedisSink(config, "test2")

	source.Via(flow1).To(sink)
}

var toUpper = func(in interface{}) interface{} {
	msg := in.(*redis.Message)
	return strings.ToUpper(msg.Payload)
}
