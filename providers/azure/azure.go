package azure

import (
	"encoding/base64"
	"fmt"
	"math"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-queue-go/azqueue"
	"github.com/rs/zerolog"

	"context"
	"net/url"

	v1 "github.com/soluto/dqd/v1"
	"github.com/spf13/viper"
)

const (
	serverTimeout = 5
)

// Message represents a message in a queue.
type AzureMessage struct {
	*azqueue.DequeuedMessage
	azureClient *azureClient
}

type azureClient struct {
	messagesURL       azqueue.MessagesURL
	MaxDequeueCount   int64
	visibilityTimeout time.Duration
	logger            *zerolog.Logger
}

func (m *AzureMessage) Data() string {
	decoded, err := base64.StdEncoding.DecodeString(m.Text)
	if err == nil {
		return string(decoded)
	}
	return m.Text
}

func (m *AzureMessage) Done() error {
	res, err := m.azureClient.messagesURL.NewMessageIDURL(azqueue.MessageID(m.Id())).Delete(context.Background(), azqueue.PopReceipt(m.PopReceipt))
	if err != nil {
		return err
	}
	if res.StatusCode() < 400 {
		return fmt.Errorf("error deleting message")
	}
	return nil
}

func (m *AzureMessage) Abort() error {
	return nil
}

func (m *AzureMessage) Id() string {
	return m.ID.String()
}

func (m *AzureMessage) Retryable() bool {
	return m.DequeueCount >= m.azureClient.MaxDequeueCount
}

type ClientOptions struct {
	StorageAccount, SasToken, Name string
	ServerTimeoutInSeconds         int
	VisibilityTimeoutInSeconds     int64
}

func (c *azureClient) Produce(m v1.RawMessage) error {
	c.messagesURL.Enqueue(context.Background(), m.Data, time.Duration(0), time.Duration(0))
	return nil
}

func (c *azureClient) Iter(ctx context.Context, out chan v1.Message) error {
	defer close(out)
	backoffCount := 0
	multiplier := 0

Main:
	for {
		select {
		case <-ctx.Done():
			break Main
		default:
		}

		var messagesCount int32 = 0
		messages, err := c.messagesURL.Dequeue(context.Background(), 32, c.visibilityTimeout)
		if err != nil {
			return err
		}
		messagesCount = messages.NumMessages()

		if messagesCount == 0 {
			c.logger.Debug().Msg("Reached empty queue")
			multiplier = 1 << int(math.Min(float64(backoffCount), 6))
			time.Sleep(time.Duration(multiplier) * 100 * time.Millisecond)
			backoffCount++
			if backoffCount >= 15 || err != nil {
				return err
			}
			continue Main
		}

		backoffCount = 0

		for i := int32(0); i < messages.NumMessages(); i++ {
			select {
			case <-ctx.Done():
				break Main
			default:
			}
			azM := messages.Message(i)
			message := &AzureMessage{
				azM,
				c,
			}
			out <- message
		}
	}
	return nil
}

type AzureQueueClientFactory struct {
	Logger *zerolog.Logger
}

func translateLogLevel(l pipeline.LogLevel) zerolog.Level {
	switch l {
	case pipeline.LogDebug:
		return zerolog.DebugLevel
	case pipeline.LogError:
		return zerolog.ErrorLevel
	case pipeline.LogFatal:
		return zerolog.FatalLevel
	case pipeline.LogInfo:
		return zerolog.InfoLevel
	case pipeline.LogNone:
		return zerolog.Disabled
	case pipeline.LogPanic:
		return zerolog.PanicLevel
	case pipeline.LogWarning:
		return zerolog.WarnLevel
	}
	return zerolog.Disabled
}

func createAuzreQueueClient(cfg *viper.Viper, logger *zerolog.Logger) *azureClient {
	cfg.SetDefault("visibilityTimeoutInSeconds", 600)
	storageAccount := cfg.GetString("storageAccount")
	queueName := cfg.GetString("queue")
	sasToken := cfg.GetString("sasToken")
	accountKey := cfg.GetString("storageAccountKey")
	visibilityTimeout := time.Duration(cfg.GetInt64("visibilityTimeoutInSeconds")) * time.Second

	credentials := azqueue.NewAnonymousCredential()
	if accountKey != "" && storageAccount != "" {
		var err error
		credentials, err = azqueue.NewSharedKeyCredential(storageAccount, accountKey)
		if err != nil {
			logger.Fatal().Msg("Error using SharedKeyCredentials")
			panic("Error using SharedKeyCredentials")
		}
	}

	pipeline := azqueue.NewPipeline(credentials, azqueue.PipelineOptions{
		Log: pipeline.LogOptions{
			Log: func(level pipeline.LogLevel, message string) {
				logger.WithLevel(translateLogLevel(level)).Msg(message)
			},
		},
		Retry: azqueue.RetryOptions{
			Policy:     azqueue.RetryPolicyExponential,
			MaxTries:   4,
			TryTimeout: 30 * time.Second,
		},
	})

	var sURL string
	if storageAccount != "" {
		sURL = fmt.Sprintf("https://%s.queue.core.windows.net", storageAccount)
	} else {
		sURL = cfg.GetString("connection")
	}
	sURL = fmt.Sprintf("%s%s", sURL, sasToken)
	u, _ := url.Parse(sURL)

	messagesURL := azqueue.NewServiceURL(*u, pipeline).NewQueueURL(queueName).NewMessagesURL()

	return &azureClient{
		messagesURL:       messagesURL,
		MaxDequeueCount:   5,
		visibilityTimeout: visibilityTimeout,
		logger:            logger,
	}
}

func (factory *AzureQueueClientFactory) CreateConsumer(cfg *viper.Viper, logger *zerolog.Logger) v1.Consumer {
	return createAuzreQueueClient(cfg, logger)
}

func (factory *AzureQueueClientFactory) CreateProducer(cfg *viper.Viper, logger *zerolog.Logger) v1.Producer {
	return createAuzreQueueClient(cfg, logger)
}
