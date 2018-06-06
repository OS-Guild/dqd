package azure

import (
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gopkg.in/eapache/go-resiliency.v1/retrier"
	"gopkg.in/h2non/gentleman.v2"
	"gopkg.in/h2non/gentleman.v2/plugins/timeout"

	"../../metrics"
	"../../queue"
	"../../utils"
)

const (
	serverTimeout = 5
)

// Message represents a message in a queue.
type AzureMessage struct {
	MessageID               string `xml:"MessageId"`
	PopReceipt, MessageText string
	DequeueCount            int64
	AzureClient             *AzureClient
}

type AzureClient struct {
	Client          *httpClient
	ErrorClient     *httpClient
	MaxDequeueCount int64
}

func (m *AzureMessage) Data() string {
	decoded, err := base64.StdEncoding.DecodeString(m.MessageText)
	if err == nil {
		return string(decoded)
	}
	return m.MessageText
}

func (m *AzureMessage) Done() {
	m.AzureClient.Client.Delete(m)
}

func (m *AzureMessage) Id() string {
	return m.MessageID
}

func (m *AzureMessage) Fail() {
	if m.DequeueCount >= m.AzureClient.MaxDequeueCount {
		m.AzureClient.Client.Delete(m)
		m.AzureClient.ErrorClient.Post(m)
	}
}

type ClientOptions struct {
	StorageAccount, SasToken, Name string
	ServerTimeoutInSeconds         int
	VisibilityTimeoutInSeconds     int64
}

type httpClient struct {
	policy                        *retrier.Retrier
	logger                        *zerolog.Logger
	client                        *gentleman.Client
	getPath, postPath, deletePath string
	queueName                     string
}

func (c *httpClient) Name() string {
	return c.queueName
}

func (c *httpClient) Get() ([]AzureMessage, error) {
	end := metrics.StartTimerWithLabels(metrics.GetMessagesSummary)
	type QueueMessagesList struct {
		QueueMessagesList xml.Name
		QueueMessages     []AzureMessage `xml:"QueueMessage"`
	}
	l := new(QueueMessagesList)

	c.logger.Debug().Msg("Start getting messages from queue")

	err := c.policy.Run(func() error {
		res, err := c.client.Get().URL(c.getPath).Send()
		if err != nil {
			return err
		}
		if !res.Ok {
			return fmt.Errorf("Invalid server response: %d", res.StatusCode)
		}
		return xml.Unmarshal(res.Bytes(), l)
	})

	if err != nil {
		c.logger.Warn().Err(err).Msg("Error getting messages from queue")
		end("false")
		return nil, err
	}

	count := len(l.QueueMessages)
	c.logger.Debug().Int("count", count).Msg("Finished getting messages from queue")
	metrics.MessageDequeueCount.Add(float64(count))
	end("true")
	return l.QueueMessages, nil
}

func (c *httpClient) Post(message *AzureMessage) error {
	end := metrics.StartTimerWithLabels(metrics.PostMessagesSummary)
	type QueueMessage struct {
		MessageText string
	}

	log := c.logger.With().Str("messageId", message.MessageID).Logger()
	log.Debug().Msg("Start posting message to queue")
	m := &QueueMessage{message.MessageText}

	output, err := xml.Marshal(m)
	if err != nil {
		log.Warn().Err(err).Msg("Error serializing message for post")
		end("false")
		return err
	}

	err = c.policy.Run(func() error {
		res, err := c.client.Post().URL(c.postPath).JSON(output).Send()
		if err != nil {
			return err
		}
		if !res.Ok {
			return fmt.Errorf("Invalid server response: %d", res.StatusCode)
		}
		return nil
	})

	if err != nil {
		log.Warn().Err(err).Msg("Error posting message to queue")
		end("false")
	} else {
		log.Debug().Msg("Finish posting message to queue")
		end("true")
	}

	return err
}

func (c *httpClient) Delete(message *AzureMessage) error {
	end := metrics.StartTimerWithLabels(metrics.DeleteMessagesSummary)
	log := c.logger.With().Str("messageId", message.MessageID).Logger()
	log.Debug().Msg("Start deleting message from queue")

	path := fmt.Sprintf(c.deletePath, message.MessageID, url.QueryEscape(message.PopReceipt))

	err := c.policy.Run(func() error {
		res, err := c.client.Delete().URL(path).Send()
		if err != nil {
			return err
		}
		if !res.Ok && res.StatusCode != 404 {
			return fmt.Errorf("Invalid server response: %d", res.StatusCode)
		}
		return nil
	})

	if err != nil {
		log.Warn().Err(err).Msg("Error deleting message from queue")
		end("false")
	} else {
		log.Debug().Msg("Finish deleting message from queue")
		end("true")
	}

	return err
}

func (c *AzureClient) Iter(out chan queue.Message, stop chan bool) {
main:
	for {
		select {
		case <-stop:
			break main
		default:
		}

		messages, err := c.Client.Get()
		if err != nil {
			break main
		}
		for _, m := range messages {
			select {
			case <-stop:
				break main
			default:
			}
			m.AzureClient = c
			out <- &m
		}
	}
	close(out)
}

// create new client
func newHttpClient(options *ClientOptions) *httpClient {
	client := gentleman.New().
		Use(timeout.Request(2 * time.Second * time.Duration(options.ServerTimeoutInSeconds)))
	logger := log.With().Str("scope", "QueueClient").Str("queueName", options.Name).Logger()

	return &httpClient{
		client:    client,
		logger:    &logger,
		queueName: options.Name,
		policy:    retrier.New(retrier.ExponentialBackoff(5, time.Second), nil),
		getPath: fmt.Sprintf("https://%s.queue.core.windows.net/%s/messages%s&numofmessages=32&visibilityTimeout=%d&timeout=%d",
			options.StorageAccount,
			options.Name,
			options.SasToken,
			options.VisibilityTimeoutInSeconds,
			options.ServerTimeoutInSeconds),
		postPath: fmt.Sprintf("https://%s.queue.core.windows.net/%s/messages%s&timeout=%d",
			options.StorageAccount,
			options.Name,
			options.SasToken,
			options.ServerTimeoutInSeconds),
		deletePath: fmt.Sprintf("https://%s.queue.core.windows.net/%s/messages/%%s%s&popreceipt=%%s&timeout=%d",
			options.StorageAccount,
			options.Name,
			strings.Replace(options.SasToken, "%", "%%", -1),
			options.ServerTimeoutInSeconds),
	}
}

type AzureClientFactory struct{}

func (factory *AzureClientFactory) Create() queue.Client {

	storageAccount := utils.GetenvRequired("STORAGE_ACCOUNT")
	queueName := utils.GetenvRequired("QUEUE_NAME")
	sasToken := utils.GetenvOrFile("SAS_TOKEN", "SAS_TOKEN_FILE", true)

	errorQueueName := os.Getenv("ERROR_QUEUE_NAME")
	if errorQueueName == "" {
		errorQueueName = queueName + "-error"
	}
	errorSasToken := utils.GetenvOrFile("ERROR_SAS_TOKEN", "ERROR_SAS_TOKEN_FILE", false)
	if errorSasToken == "" {
		errorSasToken = sasToken
	}
	visibilityTimeout := utils.GetenvInt("VISIBILITY_TIMEOUT_IN_SECONDS", 600)

	return &AzureClient{
		Client: newHttpClient(&ClientOptions{
			StorageAccount: storageAccount,
			SasToken:       sasToken,
			Name:           queueName,
			ServerTimeoutInSeconds:     serverTimeout,
			VisibilityTimeoutInSeconds: visibilityTimeout,
		}),
		ErrorClient: newHttpClient(&ClientOptions{
			StorageAccount: storageAccount,
			SasToken:       errorSasToken,
			Name:           errorQueueName,
			ServerTimeoutInSeconds:     serverTimeout,
			VisibilityTimeoutInSeconds: visibilityTimeout,
		}),
		MaxDequeueCount: utils.GetenvInt("MAX_DEQUEUE_COUNT", 5),
	}
}
