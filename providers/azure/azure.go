package azure

import (
	"encoding/base64"
	"fmt"
	"math"
	"time"

	"github.com/Azure/azure-storage-queue-go/azqueue"

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

func (c *azureClient) Iter(ctx context.Context, out chan v1.Message) {
	defer close(out)
	emptyCount := 0
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		messages, err := c.messagesURL.Dequeue(context.Background(), 32, c.visibilityTimeout)
		if err != nil {
			println("error", err)
			panic("error reading from source")
		}

		messagesCount := messages.NumMessages()
		if messagesCount == 0 {
			multiplier := 1 << int(math.Min(float64(emptyCount), 6))
			println(multiplier)
			time.Sleep(time.Duration(multiplier) * 100 * time.Millisecond)
			emptyCount++
		} else {
			emptyCount = 0
		}

		for i := int32(0); i < messages.NumMessages(); i++ {
			select {
			case <-ctx.Done():
				return
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
}

type AzureQueueClientFactory struct{}

func createAuzreQueueClient(cfg *viper.Viper) *azureClient {
	cfg.SetDefault("visibilityTimeoutInSeconds", 600)
	storageAccount := cfg.GetString("storageAccount")
	queueName := cfg.GetString("queue")
	sasToken := cfg.GetString("sasToken")
	visibilityTimeout := time.Duration(cfg.GetInt64("visibilityTimeoutInSeconds")) * time.Second
	pipeline := azqueue.NewPipeline(azqueue.NewAnonymousCredential(), azqueue.PipelineOptions{})
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
	}
}

func (factory *AzureQueueClientFactory) CreateConsumer(cfg *viper.Viper) v1.Consumer {
	return createAuzreQueueClient(cfg)
}

func (factory *AzureQueueClientFactory) CreateProducer(cfg *viper.Viper) v1.Producer {
	return createAuzreQueueClient(cfg)
}
