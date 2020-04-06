package azure

import (
	"encoding/base64"
	"fmt"
	"time"

	"github.com/Azure/azure-storage-queue-go/azqueue"

	"context"
	"net/url"

	"github.com/soluto/dqd/utils"
	v1 "github.com/soluto/dqd/v1"
	"github.com/spf13/viper"
)

const (
	serverTimeout = 5
)

// Message represents a message in a queue.
type AzureMessage struct {
	*azqueue.DequeuedMessage
	AzureClient *AzureClient
}

type AzureClient struct {
	messagesURL       azqueue.MessagesURL
	MaxDequeueCount   int64
	visibilityTimeout int64
}

func (m *AzureMessage) Data() string {
	decoded, err := base64.StdEncoding.DecodeString(m.Text)
	if err == nil {
		return string(decoded)
	}
	return m.Text
}

func (m *AzureMessage) Done() error {
	res, err := m.AzureClient.messagesURL.NewMessageIDURL(azqueue.MessageID(m.Id())).Delete(context.Background(), azqueue.PopReceipt(m.PopReceipt))
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
	return m.DequeueCount >= m.AzureClient.MaxDequeueCount
}

type ClientOptions struct {
	StorageAccount, SasToken, Name string
	ServerTimeoutInSeconds         int
	VisibilityTimeoutInSeconds     int64
}

func (c *AzureClient) Produce(m v1.Message) {
	c.messagesURL.Enqueue(context.Background(), m.Data(), time.Duration(0), time.Duration(0))
}

func (c *AzureClient) Iter(ctx context.Context, out chan v1.Message) {
main:
	for {
		select {
		case <-ctx.Done():
			break main
		default:
		}

		messages, err := c.messagesURL.Dequeue(context.Background(), 32, time.Duration(c.visibilityTimeout))
		if err != nil {
			break main
		}
		for i := int32(0); i < messages.NumMessages(); i++ {
			select {
			case <-ctx.Done():
				break main
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
	close(out)
}

type AzureClientFactory struct{}

func createAuzreClient(cfg *viper.Viper) *AzureClient {
	viper.SetDefault("visibilityTimeoutInSeconds", int64(600))
	storageAccount := cfg.GetString("storageAccount")
	queueName := cfg.GetString("queue")
	sasToken := utils.GetenvOrFile("SAS_TOKEN", "SAS_TOKEN_FILE", true)
	visibilityTimeout := cfg.GetInt64("visibilityTimeoutInSeconds")
	pipeline := azqueue.NewPipeline(azqueue.NewAnonymousCredential(), azqueue.PipelineOptions{})
	sURL := fmt.Sprintf("https://%s.queue.core.windows.net", storageAccount)
	sURL = fmt.Sprintf("%s?%s", sURL, sasToken)
	u, _ := url.Parse(sURL)

	messagesURL := azqueue.NewServiceURL(*u, pipeline).NewQueueURL(queueName).NewMessagesURL()

	return &AzureClient{
		messagesURL:       messagesURL,
		MaxDequeueCount:   5,
		visibilityTimeout: visibilityTimeout,
	}
}

func (factory *AzureClientFactory) CreateConsumer(cfg *viper.Viper) v1.Consumer {
	return createAuzreClient(cfg)
}

func (factory *AzureClientFactory) CreateProducer(cfg *viper.Viper) v1.Producer {
	return createAuzreClient(cfg)
}
