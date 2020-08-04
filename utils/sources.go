package utils

import (
	"context"
	"os"

	"github.com/rs/zerolog"
	v1 "github.com/soluto/dqd/v1"
	"github.com/spf13/viper"
)

type ioClient struct {
	file *os.File
}

type IoSourceFactory struct {
}

func (c *ioClient) HealthStatus() v1.HealthStatus {
	return v1.NewHealthStatus(v1.Healthy)
}

func (c *ioClient) Produce(context context.Context, m *v1.RawMessage) error {
	_, err := c.file.WriteString(m.Data)
	return err
}

func (*IoSourceFactory) CreateConsumer(config *viper.Viper, logger *zerolog.Logger) v1.Consumer {
	return nil
}

func (*IoSourceFactory) CreateProducer(config *viper.Viper, logger *zerolog.Logger) v1.Producer {
	s := config.GetString("file")
	var file *os.File
	if s == "" {
		file = os.Stdout
	} else {
		file, _ = os.Open(s)
	}

	return &ioClient{
		file,
	}
}
