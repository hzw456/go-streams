package flow_test

import (
	"container/heap"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	ext "github.com/hzw456/go-streams/extension"
	"github.com/hzw456/go-streams/flow"
	"github.com/hzw456/go-streams/util"
)

var toUpper = func(in interface{}) (interface{}, error) {
	msg := in.(string)
	return strings.ToUpper(msg), nil
}

var appendAsterix = func(in interface{}) ([]interface{}, error) {
	arr := in.([]interface{})
	rt := make([]interface{}, len(arr))
	for i, item := range arr {
		msg := item.(string)
		rt[i] = msg + "*"
	}
	return rt, nil
}

var flatten = func(in interface{}) ([]interface{}, error) {
	return in.([]interface{}), nil
}

var filterA = func(in interface{}) (bool, error) {
	msg := in.(string)
	return msg != "a", nil
}

func ingest(source []string, in chan interface{}) {
	for _, e := range source {
		in <- e
	}
}

func deferClose(in chan interface{}, d time.Duration) {
	time.Sleep(d)
	close(in)
}

func TestFlow(t *testing.T) {
	in := make(chan interface{})
	out := make(chan interface{})
	errChan := make(chan Error)
	source := ext.NewChanSource(in)
	flow1 := flow.NewMap(toUpper, 1, errChan)
	flow2 := flow.NewFlatMap(appendAsterix, 1, errChan)
	flow3 := flow.NewFlatMap(flatten, 1, errChan)
	throttler := flow.NewThrottler(10, time.Second*1, 50, flow.Backpressure)
	slidingWindow := flow.NewSlidingWindow(time.Second*2, time.Second*2)
	tumblingWindow := flow.NewTumblingWindow(time.Second * 1)
	sink := ext.NewChanSink(out)

	var _input = []string{"a", "b", "c"}
	var _expectedOutput = []string{"A*", "B*", "C*"}

	go ingest(_input, in)
	go deferClose(in, time.Second*3)
	go func() {
		source.Via(flow1).
			Via(tumblingWindow).
			Via(flow3).
			Via(slidingWindow).
			Via(flow2).
			Via(throttler).
			To(sink)
	}()
	var _output []string
	for e := range sink.Out {
		_output = append(_output, e.(string))
	}
	assertEqual(t, _expectedOutput, _output)
}

func TestFlowUtil(t *testing.T) {
	in := make(chan interface{})
	out := make(chan interface{})
	errChan := make(chan Error)
	source := ext.NewChanSource(in)
	flow1 := flow.NewMap(toUpper, 1, errChan)
	filter := flow.NewFilter(filterA, 1, errChan)
	sink := ext.NewChanSink(out)

	var _input = []string{"a", "b", "c"}
	var _expectedOutput = []string{"B", "B", "C", "C"}

	go ingest(_input, in)
	go deferClose(in, time.Second*1)
	go func() {
		fanOut := flow.FanOut(source.Via(filter).Via(flow1), 2)
		flow.Merge(fanOut...).To(sink)
	}()
	var _output []string
	for e := range sink.Out {
		_output = append(_output, e.(string))
	}
	sort.Strings(_output)
	assertEqual(t, _expectedOutput, _output)
}

func TestQueue(t *testing.T) {
	queue := &flow.PriorityQueue{}
	heap.Push(queue, flow.NewItem(1, util.NowNano(), 0))
	heap.Push(queue, flow.NewItem(2, 1234, 0))
	heap.Push(queue, flow.NewItem(3, util.NowNano(), 0))
	queue.Swap(0, 1)
	head := queue.Head()
	queue.Update(head, util.NowNano())
	first := heap.Pop(queue).(*flow.Item)
	assertEqual(t, first.Msg.(int), 2)
}

func assertEqual(t *testing.T, a interface{}, b interface{}) {
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("%s != %s", a, b)
	}
}
