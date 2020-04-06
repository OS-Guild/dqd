package v1

import (
	"context"

	"github.com/spf13/viper"
)

type RawMessage struct {
	id   string
	data string
}

type Message interface {
	Id() string
	Data() string
	Done() error
	Retryable() bool
}

type Consumer interface {
	Iter(ctx context.Context, out chan Message)
}

type ConsumerFactory interface {
	CreateConsumer(config *viper.Viper) Consumer
}

type Producer interface {
	Produce(m Message)
}

type ProducerFactory interface {
	CreateProducer(config *viper.Viper) Producer
}

type Source struct {
	consumerFactory ConsumerFactory
	producerFactory ProducerFactory
	config          *viper.Viper
	Name            string
}

func NewSource(cf ConsumerFactory, pf ProducerFactory, config *viper.Viper, name string) Source {
	return Source{
		cf,
		pf,
		config,
		name,
	}
}

func (s Source) CreateConsumer() Consumer {
	return s.consumerFactory.CreateConsumer(s.config)
}

func (s Source) CreateProducer() Producer {
	return s.producerFactory.CreateProducer(s.config)
}
