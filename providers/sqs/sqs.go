package sqs

import (
	"context"
	"math"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/rs/zerolog"
	v1 "github.com/soluto/dqd/v1"
	"github.com/spf13/viper"
)

type Message struct {
	Event   string                 `json:"event"`
	Payload map[string]interface{} `json:"payload"`
}

type SQSClient struct {
	sqs    sqs.SQS
	url    string
	logger *zerolog.Logger
}

type SQSMessage struct {
	*sqs.Message
	client *SQSClient
}

func createInt64Ref(x int64) *int64 {
	return &x
}

func createSQSClient(cfg *viper.Viper, logger *zerolog.Logger) *SQSClient {
	config := aws.NewConfig().WithRegion(cfg.GetString("region"))
	endpoint := cfg.GetString("endpoint")
	if endpoint != "" {
		config.Endpoint = &endpoint
	}
	svc := sqs.New(session.New(), config)
	println(cfg.GetString("url"))
	return &SQSClient{
		*svc,
		cfg.GetString("url"),
		logger,
	}
}

func (m *SQSMessage) Id() string {
	return *m.MessageId
}

func (m *SQSMessage) Data() string {
	return *m.Body
}

func (m *SQSMessage) Done() error {
	_, err := m.client.sqs.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      &m.client.url,
		ReceiptHandle: m.ReceiptHandle,
	})
	return err
}

func (m *SQSMessage) Retryable() bool {
	return false
}

func (c *SQSClient) Iter(ctx context.Context, out chan v1.Message) error {
	backoffCount := 0
	multiplier := 0

Main:
	for {
		select {
		case <-ctx.Done():
			break Main
		default:
		}
		var maxMessages int64 = 10
		messages, err := c.sqs.ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueUrl:            &c.url,
			MaxNumberOfMessages: &maxMessages,
		})

		if err != nil {
			c.logger.Debug().Err(err).Msg("Error reading from queue")
			backoffCount++
			multiplier = 1 << int(math.Min(float64(backoffCount), 6))
			time.Sleep(time.Duration(multiplier) * 100 * time.Millisecond)
			if backoffCount >= 10 {
				return err
			}
			continue Main
		}
		messagesCount := len(messages.Messages)

		if messagesCount == 0 {
			c.logger.Debug().Msg("Reached empty queue")
			backoffCount++
			multiplier = 1 << int(math.Min(float64(backoffCount), 6))
			time.Sleep(time.Duration(multiplier) * 100 * time.Millisecond)
			backoffCount++
			continue Main
		}

		if backoffCount > 0 {

		}

		backoffCount = 0

		for _, sqsM := range messages.Messages {
			select {
			case <-ctx.Done():
				break Main
			default:
			}
			message := &SQSMessage{
				sqsM,
				c,
			}
			out <- message
		}
	}
	return nil
}

func (c *SQSClient) Produce(context context.Context, m v1.RawMessage) error {
	_, err := c.sqs.SendMessage(&sqs.SendMessageInput{
		MessageBody: &m.Data,
		QueueUrl:    &c.url,
	})
	return err
}

type SQSClientFactory struct {
}

func (factory *SQSClientFactory) CreateConsumer(cfg *viper.Viper, logger *zerolog.Logger) v1.Consumer {
	return createSQSClient(cfg, logger)
}

func (factory *SQSClientFactory) CreateProducer(cfg *viper.Viper, logger *zerolog.Logger) v1.Producer {
	return createSQSClient(cfg, logger)
}
