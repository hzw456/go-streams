package ext

import (
	"context"
	"log"
	"net/http"
	"net/url"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/hzw456/go-streams"
	"github.com/hzw456/go-streams/flow"
)

// ChanSource streams data from the input channel
type WebSocketSource struct {
	url       url.URL
	conn      *websocket.Conn
	out       chan interface{}
	ctx       context.Context
	cancelCtx context.CancelFunc
	wg        *sync.WaitGroup
}

// NewChanSource returns a new ChanSource instance
func NewWebSocketSource(cctx context.Context, cancel context.CancelFunc, u url.URL, dialer *websocket.Dialer) (*WebSocketSource, error) {
	out := make(chan interface{}, 1000)

	c, _, err := dialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatal("dial:", err)
	}

	source := &WebSocketSource{
		ctx:       cctx,
		url:       u,
		conn:      c,
		out:       out,
		cancelCtx: cancel,
		wg:        &sync.WaitGroup{},
	}
	go source.Connect()
	return source, nil
}

// a handleConnection routine
func (ws *WebSocketSource) Connect() {
	go func() {
		for {
			_, message, err := ws.conn.ReadMessage()
			if err != nil {
				log.Println("read:", err)
				break
			}
			ws.out <- string(message)
		}
	}()
	select {
	case <-ws.ctx.Done():
		if ws.conn != nil {
			ws.conn.Close()
		}
		close(ws.out)
	}
	log.Printf("Closing a webSocketSource connection")
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
	w         http.ResponseWriter
	r         *http.Request
	conn      *websocket.Conn
	in        chan interface{}
	ctx       context.Context
	cancelCtx context.CancelFunc
	wg        *sync.WaitGroup
}

// var upgrader = websocket.Upgrader{HandshakeTimeout: 200 * time.Millisecond} // use default options
// NewKafkaSink returns a new KafkaSink instance
func NewWebSocketSink(cctx context.Context, cancel context.CancelFunc, w http.ResponseWriter, r *http.Request, upgrader *websocket.Upgrader) (*WebSocketSink, error) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return nil, err
	}
	sink := &WebSocketSink{
		w:         w,
		r:         r,
		conn:      conn,
		in:        make(chan interface{}),
		ctx:       cctx,
		cancelCtx: cancel,
		wg:        &sync.WaitGroup{},
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
				ws.cancelCtx()
				return
			}
		case string:
			msgBt := []byte(msg.(string))
			err := ws.conn.WriteMessage(websocket.TextMessage, msgBt)
			if err != nil {
				log.Println("write:", err)
				ws.cancelCtx()
				return
			}
		default:
			log.Printf("Unsupported message type %v", m)
		}
	}
	log.Printf("Closing websocket")
	ws.conn.Close()
}

// In returns an input channel for receiving data
func (ws *WebSocketSink) In() chan<- interface{} {
	return ws.in
}
