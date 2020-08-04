package v1

import (
	"context"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

type NextMessage func(Message)

type RawMessage struct {
	Data string
}

type Message interface {
	Id() string
	Data() string
	Complete() error
	Abort(error) bool
}

type Consumer interface {
	HealthChecker
	Iter(ctx context.Context, next NextMessage) error
}

type ConsumerFactory interface {
	CreateConsumer(config *viper.Viper, logger *zerolog.Logger) Consumer
}

type Producer interface {
	HealthChecker
	Produce(context context.Context, m *RawMessage) error
}

type ProducerFactory interface {
	CreateProducer(config *viper.Viper, logger *zerolog.Logger) Producer
}

type Source struct {
	consumerFactory ConsumerFactory
	producerFactory ProducerFactory
	config          *viper.Viper
	Name            string
}

func NewSource(cf ConsumerFactory, pf ProducerFactory, config *viper.Viper, name string) *Source {
	return &Source{
		cf,
		pf,
		config,
		name,
	}
}

func (s Source) CreateConsumer() Consumer {
	l := log.With().Fields(map[string]interface{}{
		"scope":  "Consumer",
		"source": s.Name,
	}).Logger()
	return s.consumerFactory.CreateConsumer(s.config, &l)
}

func (s Source) CreateProducer() Producer {
	l := log.With().Fields(map[string]interface{}{
		"scope":  "Consumer",
		"source": s.Name,
	}).Logger()
	return s.producerFactory.CreateProducer(s.config, &l)
}
