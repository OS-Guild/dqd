package servicebus

import (
	"context"
	"strings"

	azservicebus "github.com/Azure/azure-service-bus-go"
	"github.com/rs/zerolog"
	v1 "github.com/soluto/dqd/v1"
	"github.com/spf13/viper"
)

type ServiceBusClient struct {
	topic                   *azservicebus.Topic
	subscription            *azservicebus.Subscription
	logger                  zerolog.Logger
	preFetchCount           int
	removeSerializationInfo bool
}

type ServiceBusMessage struct {
	message                 *azservicebus.Message
	removeSerializationInfo bool
}

func createServiceBusClient(cfg *viper.Viper, logger *zerolog.Logger) *ServiceBusClient {
	cfg.SetDefault("prefetchCount", 1)
	namespace, err := azservicebus.NewNamespace(azservicebus.NamespaceWithConnectionString(cfg.GetString("connectionString")))
	topicName := cfg.GetString("topic")
	subscriptionName := cfg.GetString("subscription")
	topic, err := namespace.NewTopic(topicName)
	if err != nil {
		panic("failed to initalize service bus client")
	}
	l := logger.With().Str("topic", topicName).Str("subscription", subscriptionName).Logger()
	subscription, err := topic.NewSubscription(subscriptionName)
	return &ServiceBusClient{
		topic,
		subscription,
		l,
		cfg.GetInt("prefetchCount"),
		cfg.GetBool("removeSerializationInfoInJson"),
	}
}

func (m *ServiceBusMessage) Id() string {
	return m.message.ID
}

func (m *ServiceBusMessage) Data() string {
	data := string(m.message.Data)
	println(m.removeSerializationInfo)
	if m.removeSerializationInfo {
		return strings.TrimRightFunc(strings.TrimLeftFunc(data, func(r rune) bool { return r != '{' && r != '[' }), func(r rune) bool { return r != '}' && r != ']' })
	}
	return data
}

func (m *ServiceBusMessage) Complete() error {
	return m.message.Complete(context.Background())
}

func (m *ServiceBusMessage) Abort(error) bool {
	m.message.Abandon(context.Background())
	return true
}

func (sb *ServiceBusClient) Iter(ctx context.Context, next v1.NextMessage) error {
	rec, err := sb.subscription.NewReceiver(ctx, azservicebus.ReceiverWithReceiveMode(azservicebus.PeekLockMode), azservicebus.ReceiverWithPrefetchCount(10))
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			break
		default:

		}

		err = rec.ReceiveOne(ctx, azservicebus.HandlerFunc(func(ctx context.Context, m *azservicebus.Message) error {
			message := &ServiceBusMessage{
				m,
				sb.removeSerializationInfo,
			}
			next(message)
			return nil
		}))
		if err != nil {
			return err
		}
	}
}

func (c *ServiceBusClient) Produce(ctx context.Context, m *v1.RawMessage) error {
	return c.topic.Send(ctx, azservicebus.NewMessageFromString(m.Data))
}

type ServiceBusClientFactory struct {
}

func (factory *ServiceBusClientFactory) CreateConsumer(cfg *viper.Viper, logger *zerolog.Logger) v1.Consumer {
	return createServiceBusClient(cfg, logger)
}

func (factory *ServiceBusClientFactory) CreateProducer(cfg *viper.Viper, logger *zerolog.Logger) v1.Producer {
	return createServiceBusClient(cfg, logger)
}
