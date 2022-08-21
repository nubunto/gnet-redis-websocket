package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/panjf2000/gnet/v2"
	"github.com/panjf2000/gnet/v2/pkg/logging"
	"github.com/panjf2000/gnet/v2/pkg/pool/bytebuffer"
	"github.com/panjf2000/gnet/v2/pkg/pool/goroutine"
)

type RTHub struct {
	gnet.BuiltinEventEngine
	addr    string
	pubsub  *PubSub
	handler actionHandler
}

type connCodec struct {
	upgradedConnection bool

	currentState connState

	subscribedChannels []string
	pubsub             *PubSub
}

type connState int

const (
	invalid connState = iota

	connected
	tryingJoin
	joined
	left
)

type M struct {
	Action  string          `json:"action"`
	Channel string          `json:"channel"`
	Message *dynamicMessage `json:"message"`
}

type dynamicMessage map[string]interface{}

func (d *dynamicMessage) MarshalBinary() ([]byte, error) {
	return json.Marshal(d)
}

type R map[string]interface{}

func (codec *connCodec) upgrade() {
	codec.upgradedConnection = true
}

func (codec *connCodec) isUpgraded() bool {
	return codec.upgradedConnection
}

func (codec *connCodec) writeJSON(conn gnet.Conn, value R) error {
	dst := bytebuffer.Get()
	defer bytebuffer.Put(dst)

	m, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshalling value: %w", err)
	}

	err = wsutil.WriteServerText(conn, m)
	if err != nil {
		return fmt.Errorf("writing to connection: %w", err)
	}

	return nil
}

func New(addr string, pubsub *PubSub, handler actionHandler) *RTHub {
	return &RTHub{
		addr:    addr,
		pubsub:  pubsub,
		handler: handler,
	}
}

type actionHandler interface {
	Handle(io.Reader)
}

type actionHandlerFn func(io.Reader)

func (fn actionHandlerFn) Handle(reader io.Reader) {
	fn(reader)
}

func (rt *RTHub) handleMessage(conn gnet.Conn) {
	rt.handler.Handle(conn)
}

func (rt *RTHub) OnBoot(_ gnet.Engine) gnet.Action {
	logging.Infof("booting RTHub on address %s!", rt.addr)

	return gnet.None
}

func (rt *RTHub) OnOpen(conn gnet.Conn) ([]byte, gnet.Action) {
	codec := &connCodec{
		pubsub:             rt.pubsub,
		subscribedChannels: make([]string, 0, 50),
		currentState:       connected,
	}

	conn.SetContext(codec)

	return nil, gnet.None
}

func (rt *RTHub) OnClose(conn gnet.Conn, err error) gnet.Action {
	codec := conn.Context().(*connCodec)

	rt.pubsub.Unsubscribe(conn, codec.subscribedChannels)

	return gnet.Close
}

func (rt *RTHub) OnTraffic(conn gnet.Conn) gnet.Action {
	codec, ok := conn.Context().(*connCodec)
	if !ok {
		// shouldn't happen
		panic("unexpected type in Context")
	}

	if !codec.isUpgraded() {
		_, err := ws.Upgrade(conn)
		if err != nil {
			logging.Errorf("error upgrading connection: %w", err)

			return gnet.Close
		}

		codec.upgrade()

		return gnet.None
	}

	rt.handleMessage(conn)

	return gnet.None
}

func main() {
	var (
		port                     int64
		redisAddr, redisPassword string
	)

	port, _ = strconv.ParseInt(os.Getenv("PORT"), 10, 64)
	if port == 0 {
		port = 9000
	}

	redisAddr = os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	redisPassword = os.Getenv("REDIS_PASSWORD")

	pool := goroutine.Default()
	pubsub := NewPubSub(redisAddr, redisPassword, pool)

	addr := fmt.Sprintf("tcp://0.0.0.0:%d", port)
	hub := New(addr, pubsub, actionHandlerFn(myHandler))

	gnet.Run(
		hub,
		addr,
		gnet.WithMulticore(true),
		gnet.WithReusePort(true),
	)
}

func myHandler(r io.Reader) {
	conn, _ := r.(gnet.Conn)
	codec, _ := conn.Context().(*connCodec)

	msg, _, err := wsutil.ReadClientData(conn)
	if err != nil {
		if _, ok := err.(wsutil.ClosedError); !ok {
			logging.Errorf("error reading client data: %w", err)
		}

		return
	}

	var message M

	err = json.Unmarshal(msg, &message)
	if err != nil {
		logging.Errorf("error unmarshalling client data: %w", err)

		return
	}

	switch message.Action {
	case "join":
		if message.Channel == "" {
			codec.writeJSON(conn, R{"message": "`channel` cannot be empty"})

			return
		}

		logging.Infof("joining conn %v to channel %s", conn.RemoteAddr().String(), message.Channel)

		codec.pubsub.Subscribe(conn, message.Channel)
	case "message":
		if message.Message == nil {
			return
		}

		logging.Infof("sending message %v to channel %s", message.Message, message.Channel)

		err := codec.pubsub.Publish(message.Channel, message.Message)
		if err != nil {
			logging.Errorf("error publishing: %v", err)
		}
	case "debug:connected":
		codec.writeJSON(conn, R{"channels": codec.pubsub.ActiveChannels()})
	}
}
