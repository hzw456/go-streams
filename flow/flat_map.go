package flow

import (
	"github.com/hzw456/go-streams"
)

// FlatMapFunc is a FlatMap transformation function.
type FlatMapFunc func(interface{}) ([]interface{}, error)

// FlatMap takes one element and produces zero, one, or more elements.
//
// in  -- 1 -- 2 ---- 3 -- 4 ------ 5 --
//        |    |      |    |        |
//    [---------- FlatMapFunc ----------]
//        |    |           |   |    |
// out -- 1' - 2' -------- 4'- 4''- 5' -
type FlatMap struct {
	FlatMapF    FlatMapFunc
	in          chan interface{}
	out         chan interface{}
	parallelism uint
	errChan     chan error
}

// Verify FlatMap satisfies the Flow interface.
var _ streams.Flow = (*FlatMap)(nil)

// NewFlatMap returns a new FlatMap instance.
// flatMapFunc is the FlatMap transformation function.
// parallelism is the flow parallelism factor. In case the events order matters, use parallelism = 1.
func NewFlatMap(flatMapFunc FlatMapFunc, parallelism uint, errChan chan error) *FlatMap {
	flatMap := &FlatMap{
		flatMapFunc,
		make(chan interface{}),
		make(chan interface{}),
		parallelism,
		errChan,
	}
	go flatMap.doStream()
	return flatMap
}

// Via streams data through the given flow
func (fm *FlatMap) Via(flow streams.Flow) streams.Flow {
	go fm.transmit(flow)
	return flow
}

// To streams data to the given sink
func (fm *FlatMap) To(sink streams.Sink) {
	fm.transmit(sink)
}

// Out returns an output channel for sending data
func (fm *FlatMap) Out() <-chan interface{} {
	return fm.out
}

// In returns an input channel for receiving data
func (fm *FlatMap) In() chan<- interface{} {
	return fm.in
}

func (fm *FlatMap) transmit(inlet streams.Inlet) {
	for elem := range fm.Out() {
		inlet.In() <- elem
	}
	close(inlet.In())
}

func (fm *FlatMap) doStream() {
	sem := make(chan struct{}, fm.parallelism)
	for elem := range fm.in {
		sem <- struct{}{}
		go func(e interface{}) {
			defer func() { <-sem }()
			trans, err := fm.FlatMapF(e)
			if err != nil {
				fm.errChan <- err
				return
			}
			for _, item := range trans {
				fm.out <- item
			}
		}(elem)
	}
	for i := 0; i < int(fm.parallelism); i++ {
		sem <- struct{}{}
	}
	close(fm.out)
}
