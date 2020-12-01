package ext

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/Shopify/sarama"
	"github.com/hzw456/go-streams"
	"github.com/hzw456/go-streams/flow"
)

// KafkaSource connector
type KafkaSource struct {
	consumerGroup sarama.ConsumerGroup
	handler       sarama.ConsumerGroupHandler
	consumer      sarama.Consumer
	partitions    []int32
	topics        []string
	out           chan interface{}
	ctx           context.Context
	cancelCtx     context.CancelFunc
	wg            *sync.WaitGroup
}

// NewKafkaSource returns a new KafkaSource instance
func NewKafkaSource(ctx context.Context, addrs []string, groupID string,
	config *sarama.Config, topics ...string) *KafkaSource {
	if len(topics) == 0 {
		streams.Check(errors.New("topic is null"))
	}
	out := make(chan interface{})
	cctx, cancel := context.WithCancel(ctx)
	var sink KafkaSource
	sink.wg = &sync.WaitGroup{}
	sink.handler = &GroupHandler{make(chan struct{}), out}
	sink.topics = topics
	sink.out = out
	sink.cancelCtx = cancel
	sink.ctx = cctx
	isLowVer := false
	if ver, _ := sarama.ParseKafkaVersion("0.10.2.0"); config.Version.IsAtLeast(ver) {
		consumerGroup, err := sarama.NewConsumerGroup(addrs, groupID, config)
		streams.Check(err)
		sink.consumerGroup = consumerGroup
	} else {
		consumer, err := sarama.NewConsumer(addrs, config)
		streams.Check(err)
		sink.consumer = consumer
		partitions, err := consumer.Partitions(topics[0])
		streams.Check(err)
		sink.partitions = partitions
		isLowVer = true
	}

	go sink.init(isLowVer)
	return &sink
}

func (ks *KafkaSource) claimLoop(isLow bool) {
	ks.wg.Add(1)
	defer func() {
		ks.wg.Done()
		log.Printf("Exiting kafka claimLoop")
	}()
	if isLow {
		sarama.Logger = log.New(os.Stderr, "[sarama]", log.LstdFlags)
		for _, partition := range ks.partitions {
			go func(partition int32) {
				partitionConsumer, err := ks.consumer.ConsumePartition(ks.topics[0], partition, sarama.OffsetNewest)
				if err != nil {
					log.Printf("Kafka consumer.Consume failed with: %v", err)
					return
				}
				defer func() {
					ks.wg.Done()
					if err := partitionConsumer.Close(); err != nil {
						log.Fatalln(err)
					}
				}()
			ConsumerLoop:
				for {
					select {
					case err := <-partitionConsumer.Errors():
						if err != nil {
							log.Fatalln("error:", err)
						}
					case msg := <-partitionConsumer.Messages():
						if msg != nil {
							ks.out <- msg
						}
					case <-ks.ctx.Done():
						log.Println("stop consuming partition:", partition)
						break ConsumerLoop
					}
				}
			}(partition)
		}
		select {
		case <-ks.ctx.Done():
			return
		default:
		}
	} else {
		for {
			handler := ks.handler.(*GroupHandler)
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := ks.consumerGroup.Consume(ks.ctx, ks.topics, handler); err != nil {
				log.Printf("Kafka consumer.Consume failed with: %v", err)
			}

			select {
			case <-ks.ctx.Done():
				return
			default:
			}
			handler.ready = make(chan struct{})
		}
	}
}

// init starts the main loop
func (ks *KafkaSource) init(isLow bool) {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	go ks.claimLoop(isLow)

	select {
	case <-sigchan:
		ks.cancelCtx()
	case <-ks.ctx.Done():
	}

	log.Printf("Closing kafka consumer")
	ks.wg.Wait()
	close(ks.out)
	ks.consumer.Close()
}

// Via streams data through the given flow
func (ks *KafkaSource) Via(_flow streams.Flow) streams.Flow {
	flow.DoStream(ks, _flow)
	return _flow
}

// Out returns an output channel for sending data
func (ks *KafkaSource) Out() <-chan interface{} {
	return ks.out
}

// GroupHandler represents a Sarama consumer group handler
type GroupHandler struct {
	ready chan struct{}
	out   chan interface{}
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (handler *GroupHandler) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(handler.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (handler *GroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (handler *GroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			if message != nil {
				log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s",
					string(message.Value), message.Timestamp, message.Topic)
				session.MarkMessage(message, "")
				handler.out <- message
			}
		case <-session.Context().Done():
			return session.Context().Err()
		}
	}
}

// KafkaSink connector
type KafkaSink struct {
	producer sarama.SyncProducer
	topic    string
	in       chan interface{}
}

// NewKafkaSink returns a new KafkaSink instance
func NewKafkaSink(addrs []string, config *sarama.Config, topic string) *KafkaSink {
	producer, err := sarama.NewSyncProducer(addrs, config)
	streams.Check(err)
	sink := &KafkaSink{
		producer,
		topic,
		make(chan interface{}),
	}
	go sink.init()
	return sink
}

// init starts the main loop
func (ks *KafkaSink) init() {
	for msg := range ks.in {
		switch m := msg.(type) {
		case *sarama.ProducerMessage:
			ks.producer.SendMessage(m)
		case *sarama.ConsumerMessage:
			sMsg := &sarama.ProducerMessage{
				Topic: ks.topic,
				Key:   sarama.StringEncoder(m.Key),
				Value: sarama.StringEncoder(m.Value),
			}
			ks.producer.SendMessage(sMsg)
		case string:
			sMsg := &sarama.ProducerMessage{
				Topic: ks.topic,
				Value: sarama.StringEncoder(m),
			}
			ks.producer.SendMessage(sMsg)
		default:
			log.Printf("Unsupported message type %v", m)
		}
	}
	log.Printf("Closing kafka producer")
	ks.producer.Close()
}

// In returns an input channel for receiving data
func (ks *KafkaSink) In() chan<- interface{} {
	return ks.in
}
