package ext

import (
	"context"
	"log"
	"net/http"
	"net/url"

	"github.com/gorilla/websocket"
	"github.com/hzw456/go-streams"
	"github.com/hzw456/go-streams/flow"
)

// ChanSource streams data from the input channel
type WebSocketSource struct {
	ctx  context.Context
	url  url.URL
	conn *websocket.Conn
	out  chan interface{}
}

// NewChanSource returns a new ChanSource instance
func NewWebSocketSource(ctx context.Context, u url.URL) (*WebSocketSource, error) {
	out := make(chan interface{})

	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatal("dial:", err)
	}
	go websocketConnection(c, out)
	source := &WebSocketSource{
		ctx:  ctx,
		url:  u,
		conn: c,
		out:  out,
	}
	go source.listenCtx()
	return source, nil
}

func (ws *WebSocketSource) listenCtx() {
	select {
	case <-ws.ctx.Done():
		if ws.conn != nil {
			ws.conn.Close()
		}
		close(ws.out)
	}
}

// a handleConnection routine
func websocketConnection(conn *websocket.Conn, out chan<- interface{}) {
	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			log.Println("read:", err)
			break
		}
		out <- string(message)
	}
	log.Printf("Closing a webSocketSource connection %v", conn)
	conn.Close()
}

// Via streams data through the given flow
func (ws *WebSocketSource) Via(_flow streams.Flow) streams.Flow {
	flow.DoStream(ws, _flow)
	return _flow
}

// Out returns an output channel for sending data
func (ws *WebSocketSource) Out() <-chan interface{} {
	return ws.out
}

// KafkaSink connector
type WebSocketSink struct {
	w    http.ResponseWriter
	r    *http.Request
	conn *websocket.Conn
	in   chan interface{}
}

// var upgrader = websocket.Upgrader{HandshakeTimeout: 200 * time.Millisecond} // use default options
// NewKafkaSink returns a new KafkaSink instance
func NewWebSocketSink(w http.ResponseWriter, r *http.Request, upgrader *websocket.Upgrader) (*WebSocketSink, error) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return nil, err
	}
	sink := &WebSocketSink{
		w,
		r,
		conn,
		make(chan interface{}),
	}
	go sink.init()
	return sink, nil
}

// init starts the main loop
func (ws *WebSocketSink) init() {
	for msg := range ws.in {
		switch m := msg.(type) {
		case []byte:
			msgBt := msg.([]byte)
			err := ws.conn.WriteMessage(websocket.TextMessage, msgBt)
			if err != nil {
				log.Println("write:", err)
			}
		case string:
			msgBt := []byte(msg.(string))
			err := ws.conn.WriteMessage(websocket.TextMessage, msgBt)
			if err != nil {
				log.Println("write:", err)
			}
		default:
			log.Printf("Unsupported message type %v", m)
		}
	}
	log.Printf("Closing kafka producer")
	ws.conn.Close()
}

// In returns an input channel for receiving data
func (ws *WebSocketSink) In() chan<- interface{} {
	return ws.in
}
