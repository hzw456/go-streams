package extension

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hzw456/go-streams"
	"github.com/hzw456/go-streams/flow"
)

// Message represents a message from peer
type Message struct {
	// The message types are defined in RFC 6455, section 11.8.
	MsgType int
	Payload []byte
}

// WebSocketSource connector
type WebSocketSource struct {
	ctx        context.Context
	connection *websocket.Conn
	out        chan interface{}
	errsChan   chan error
}

// NewWebSocketSource returns a new WebSocketSource instance
func NewWebSocketSource(ctx context.Context, url string, errsChan chan error) (*WebSocketSource, error) {
	return NewWebSocketSourceWithDialer(ctx, url, websocket.DefaultDialer, errsChan)
}

// NewWebSocketSourceWithDialer returns a new WebSocketSource instance
func NewWebSocketSourceWithDialer(ctx context.Context, url string, dialer *websocket.Dialer, errsChan chan error) (*WebSocketSource, error) {
	conn, _, err := dialer.Dial(url, nil)
	if err != nil {
		return nil, err
	}
	source := &WebSocketSource{
		ctx:        ctx,
		connection: conn,
		out:        make(chan interface{}),
		errsChan:   errsChan,
	}
	go source.init()
	return source, nil
}

// init starts the main loop
func (wsock *WebSocketSource) init() {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	retry := 0
loop:
	for {
		select {
		case <-sigchan:
			break loop
		case <-wsock.ctx.Done():
			break loop
		default:
			t, msg, err := wsock.connection.ReadMessage()
			if err != nil {
				retry++
				if retry >= 5 {
					wsock.errsChan <- errors.New("Error on ws ReadMessage: err=%v url=%v", err, wsock.connection.LocalAddr().String())
					break loop
				}
				time.Sleep(3 * time.Second)
			} else {
				retry = 0
				wsock.out <- Message{
					MsgType: t,
					Payload: msg,
				}
				// exit on CloseMessage
				if t == websocket.CloseMessage {
					break loop
				}
			}
		}
	}

	log.Print("Closing the WebSocketSource connection")
	close(wsock.out)
	wsock.connection.Close()
}

// Via streams data through the given flow
func (wsock *WebSocketSource) Via(_flow streams.Flow) streams.Flow {
	flow.DoStream(wsock, _flow)
	return _flow
}

// Out returns an output channel for sending data
func (wsock *WebSocketSource) Out() <-chan interface{} {
	return wsock.out
}

// WebSocketSink connector
type WebSocketSink struct {
	ctx        context.Context
	connection *websocket.Conn
	in         chan interface{}
}

// NewWebSocketSink returns a new WebSocketSink instance
func NewWebSocketSink(ctx context.Context, url string) (*WebSocketSink, error) {
	return NewWebSocketSinkWithDialer(ctx, url, websocket.DefaultDialer)
}

// NewWebSocketSinkWithDialer returns a new WebSocketSink instance
func NewWebSocketSinkWithDialer(ctx context.Context, url string, dialer *websocket.Dialer) (*WebSocketSink, error) {
	conn, _, err := dialer.Dial(url, nil)
	if err != nil {
		return nil, err
	}

	sink := &WebSocketSink{
		ctx:        ctx,
		connection: conn,
		in:         make(chan interface{}),
	}
	go sink.init()
	return sink, nil
}

// init starts the main loop
func (wsock *WebSocketSink) init() {
	for msg := range wsock.in {
		var err error
		switch m := msg.(type) {
		case Message:
			err = wsock.connection.WriteMessage(m.MsgType, m.Payload)
		case string:
			err = wsock.connection.WriteMessage(websocket.TextMessage, []byte(m))
		case []byte:
			err = wsock.connection.WriteMessage(websocket.TextMessage, m)
		default:
			log.Printf("WebSocketSink Unsupported message type %v", m)
		}
		if err != nil {
			log.Printf("Error on ws WriteMessage: %v", err)
		}
	}
	log.Print("Closing the WebSocketSink connection")
	wsock.connection.Close()
}

// NewKafkaSink returns a new KafkaSink instance
func NewWebSocketSinkWithConn(ctx context.Context, conn *websocket.Conn) (*WebSocketSink, error) {
	defer func() {
		if r := recover(); r != nil {
			panic(r)
		}
	}()
	sink := &WebSocketSink{
		connection: conn,
		in:         make(chan interface{}),
		ctx:        ctx,
	}

	go sink.init()
	return sink, nil
}

// In returns an input channel for receiving data
func (wsock *WebSocketSink) In() chan<- interface{} {
	return wsock.in
}
