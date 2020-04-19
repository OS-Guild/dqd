package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
	"gopkg.in/eapache/go-resiliency.v1/retrier"
	"gopkg.in/h2non/gentleman.v2"
	"gopkg.in/h2non/gentleman.v2/plugins/timeout"

	"github.com/soluto/dqd/config"
	"github.com/soluto/dqd/metrics"
)

var logger = log.With().Str("scope", "Main").Logger()

func waitForHealth() {
	healthEndpoint := os.Getenv("HEALTH_ENDPOINT")
	if healthEndpoint == "" {
		return
	}

	client := gentleman.New().
		URL(healthEndpoint).
		Use(timeout.Request(5 * time.Second))

	err := retrier.New(retrier.ConstantBackoff(120, time.Second), nil).Run(func() error {
		res, err := client.Get().Send()
		if err != nil {
			return err
		}
		if !res.Ok {
			return fmt.Errorf("Invalid server response: %d", res.StatusCode)

		}
		return nil
	})

	if err != nil {
		logger.Fatal().Err(err).Msg("Timeout while waiting for health")
	}
}

func main() {
	println("init")
	conf := viper.New()
	conf.SetConfigName("config")
	conf.SetConfigType("yaml")
	conf.AddConfigPath(".")
	conf.AddConfigPath("/config")
	conf.SetDefault("logLevel", 1)
	conf.SetDefault("metricsPort", 8888)
	err := conf.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}

	logLevel := conf.GetInt("LOG_LEVEL")
	zerolog.SetGlobalLevel(zerolog.Level(logLevel))

	metricsPort := conf.GetInt("METRICS_PORT")

	waitForHealth()

	app := config.CreateApp(conf)

	go metrics.Start(metricsPort)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	for _, worker := range app.Workers {
		go worker.Start(ctx)
	}

	for _, listeners := range app.Listeners {
		go listeners.Listen()
	}

	select {
	case <-c:
		{

		}
	}

}
