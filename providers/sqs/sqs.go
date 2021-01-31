package sqs

import (
	"context"
	"encoding/json"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/jpillora/backoff"
	"github.com/rs/zerolog"
	v1 "github.com/soluto/dqd/v1"
	"github.com/spf13/viper"
)

type SQSClient struct {
	sqs                        sqs.SQS
	url                        string
	visibilityTimeoutInSeconds int64
	maxNumberOfMessages        int64
	unwrapSnsMessage           bool
	logger                     *zerolog.Logger
}

type SQSMessage struct {
	*sqs.Message
	client *SQSClient
}

type SnsMessage struct {
	Message string `json:"Message"`
}

func createSQSClient(cfg *viper.Viper, logger *zerolog.Logger) *SQSClient {
	cfg.SetDefault("visibilityTimeoutInSeconds", 600)
	cfg.SetDefault("maxNumberOfMessages", 10)
	cfg.SetDefault("unwrapSnsMessage", false)

	awsConfig := aws.NewConfig().WithRegion(cfg.GetString("region"))

	endpoint := cfg.GetString("endpoint")
	if endpoint != "" {
		awsConfig.Endpoint = &endpoint
	}
	svc := sqs.New(session.New(), awsConfig)
	return &SQSClient{
		*svc,
		cfg.GetString("url"),
		cfg.GetInt64("visibilityTimeoutInSeconds"),
		cfg.GetInt64("maxNumberOfMessages"),
		cfg.GetBool("unwrapSnsMessage"),
		logger,
	}
}

func (m *SQSMessage) Id() string {
	return *m.MessageId
}

func (m *SQSMessage) Data() string {
	if !m.client.unwrapSnsMessage {
		return *m.Body
	}

	var snsMessage SnsMessage
	err := json.Unmarshal([]byte(*m.Body), &snsMessage)

	if err != nil {
		m.client.logger.Warn().Err(err).Str("Body", *m.Body).Msg("Failed deserializing SNS style message, sending along original message instead")
		return *m.Body
	}

	return snsMessage.Message
}

func (m *SQSMessage) Complete() error {
	_, err := m.client.sqs.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      &m.client.url,
		ReceiptHandle: m.ReceiptHandle,
	})
	return err
}

func (m *SQSMessage) Abort(error) bool {
	return true
}

func (c *SQSClient) Iter(ctx context.Context, next v1.NextMessage) error {
	errorBackoff := &backoff.Backoff{}
	emptyBackoff := &backoff.Backoff{}
Main:
	for {
		select {
		case <-ctx.Done():
			break Main
		default:
		}
		messages, err := c.sqs.ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueUrl:            &c.url,
			MaxNumberOfMessages: &c.maxNumberOfMessages,
			VisibilityTimeout:   &c.visibilityTimeoutInSeconds,
		})

		if err != nil {
			c.logger.Debug().Err(err).Msg("Error reading from queue")
			time.Sleep(errorBackoff.Duration())
			if errorBackoff.Attempt() >= 10 {
				return err
			}
			continue Main
		}
		errorBackoff.Reset()

		if len(messages.Messages) == 0 {
			c.logger.Debug().Msg("Reached empty queue")
			time.Sleep(emptyBackoff.Duration())
			continue Main
		}
		emptyBackoff.Reset()

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
			next(message)
		}
	}
	return nil
}

func (c *SQSClient) Produce(context context.Context, m *v1.RawMessage) error {
	backoff := &backoff.Backoff{
		Max: 10 * time.Second,
		Min: 100 * time.Millisecond,
	}
	act := func() error {
		_, err := c.sqs.SendMessage(&sqs.SendMessageInput{
			MessageBody: &m.Data,
			QueueUrl:    &c.url,
		})
		return err
	}
	err := act()
	for err != nil {
		err = act()
		if backoff.Attempt() > 4 {
			return err
		}
		time.Sleep(backoff.Duration())
	}
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

func (h *SQSClient) HealthStatus() v1.HealthStatus {
	return v1.NewHealthStatus(v1.Healthy)
}
