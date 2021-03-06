package extension

import (
	"bufio"
	"os"

	"github.com/hzw456/go-streams"
	"github.com/hzw456/go-streams/flow"
	"github.com/hzw456/go-streams/util"
)

// FileSource streams data from the file
type FileSource struct {
	fileName string
	in       chan interface{}
}

// NewFileSource returns a new FileSource instance
func NewFileSource(fileName string) *FileSource {
	source := &FileSource{fileName, make(chan interface{})}
	source.init()
	return source
}

func (fs *FileSource) init() {
	go func() {
		file, err := os.Open(fs.fileName)
		util.Check(err)
		defer file.Close()
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			fs.in <- scanner.Text()
		}
	}()
}

// Via streams data through the given flow
func (fs *FileSource) Via(_flow streams.Flow) streams.Flow {
	flow.DoStream(fs, _flow)
	return _flow
}

// Out returns an output channel for sending data
func (fs *FileSource) Out() <-chan interface{} {
	return fs.in
}

// A FileSink writes items to a file
type FileSink struct {
	fileName string
	in       chan interface{}
}

// NewFileSink returns a new FileSink instance
func NewFileSink(fileName string) *FileSink {
	sink := &FileSink{fileName, make(chan interface{})}
	sink.init()
	return sink
}

func (fs *FileSink) init() {
	go func() {
		file, err := os.OpenFile(fs.fileName, os.O_CREATE|os.O_WRONLY, 0600)
		util.Check(err)
		defer file.Close()
		for elem := range fs.in {
			_, err = file.WriteString(elem.(string) + "\n")
			util.Check(err)
		}
	}()
}

// In returns an input channel for receiving data
func (fs *FileSink) In() chan<- interface{} {
	return fs.in
}
