package main

import (
	"context"
	"strings"
	"sync"

	"github.com/go-redis/redis/v8"
	"github.com/panjf2000/gnet/v2"
	"github.com/panjf2000/gnet/v2/pkg/logging"
	"github.com/panjf2000/gnet/v2/pkg/pool/goroutine"
)

type PubSub struct {
	redisClient *redis.Client

	mu            sync.Mutex
	subscriptions map[string]*conns

	pool *goroutine.Pool
}

func NewPubSub(redisAddr, redisPassword string, pool *goroutine.Pool) *PubSub {
	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: redisPassword,
		Username: "default",
	})

	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		panic(err)
	}

	return &PubSub{
		redisClient:   redisClient,
		subscriptions: make(map[string]*conns),
		pool:          pool,
	}
}

type conns struct {
	connections []gnet.Conn
	pubsub      *redis.PubSub
	ctxCancel   func()
}

func (c *conns) removeConnection(conn gnet.Conn) {
	connIndex := -1

	for i, c := range c.connections {
		if c == conn {
			connIndex = i

			break
		}
	}

	if connIndex == -1 {
		return
	}

	c.connections[len(c.connections)-1], c.connections[connIndex] = nil, c.connections[len(c.connections)-1]
	c.connections = c.connections[:len(c.connections)-1]

	// cancel the context if there are no more connections
	// listening to the channel
	// this will kill the open goroutine associated with the pubsub
	if len(c.connections) == 0 {
		c.ctxCancel()
	}
}

func (ps *PubSub) ActiveChannels() map[string]string {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	activeChannels := make(map[string]string)

	for channel, conns := range ps.subscriptions {
		connsSlice := make([]string, 0, len(conns.connections))
		for _, conn := range conns.connections {
			connsSlice = append(connsSlice, conn.RemoteAddr().String())
		}

		activeChannels[channel] = strings.Join(connsSlice, ", ")
	}

	return activeChannels
}

func (ps *PubSub) Publish(channel string, message interface{}) error {
	return ps.redisClient.Publish(context.Background(), channel, message).Err()
}

func (ps *PubSub) Subscribe(conn gnet.Conn, channel string) {
	_ = ps.pool.Submit(func() {
		ps.mu.Lock()
		ps.track(conn, channel)
		ps.mu.Unlock()

		codec := conn.Context().(*connCodec)
		codec.writeJSON(conn, R{"success": true, "channel": channel})

		codec.currentState = joined
		codec.subscribedChannels = append(codec.subscribedChannels, channel)
	})
}

func (ps *PubSub) Unsubscribe(conn gnet.Conn, channels []string) {
	_ = ps.pool.Submit(func() {
		ps.mu.Lock()
		ps.untrack(conn, channels)
		ps.mu.Unlock()

		codec, ok := conn.Context().(*connCodec)
		if !ok {
			return
		}

		codec.writeJSON(conn, R{"success": true, "unsubscribed_from": channels})

		codec.currentState = connected
	})
}

func (ps *PubSub) track(conn gnet.Conn, channel string) {
	if _, ok := ps.subscriptions[channel]; !ok {
		ctx, cancel := context.WithCancel(context.Background())

		ps.subscriptions[channel] = &conns{
			connections: []gnet.Conn{conn},
			pubsub:      ps.redisClient.Subscribe(context.Background(), channel),

			ctxCancel: cancel,
		}

		ps.pool.Submit(ps.handleSubscription(ctx, channel))

		return
	}

	ps.subscriptions[channel].connections = append(ps.subscriptions[channel].connections, conn)
}

func (ps *PubSub) untrack(conn gnet.Conn, channels []string) {
	for _, channel := range channels {
		sub := ps.subscriptions[channel]

		sub.removeConnection(conn)
	}
}

func (ps *PubSub) deleteSubscription(channel string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	delete(ps.subscriptions, channel)
}

func (ps *PubSub) handleSubscription(ctx context.Context, channel string) func() {
	return func() {
		sub := ps.subscriptions[channel]
		ch := sub.pubsub.Channel()

		for {
			select {
			case <-ctx.Done():
				logging.Infof("killing goroutine for channel %s", channel)

				ps.deleteSubscription(channel)

				return
			case msg := <-ch:
				ps.mu.Lock()

				for _, conn := range sub.connections {
					connCtx := conn.Context().(*connCodec)

					connCtx.writeJSON(conn, R{"message": msg.Payload, "channel": channel})
				}

				ps.mu.Unlock()
			}
		}
	}
}
