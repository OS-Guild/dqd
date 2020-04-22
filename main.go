package main

import (
	"context"
	"fmt"
	"os"
	"time"

	gofigure "github.com/NCAR/go-figure"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
	"gopkg.in/eapache/go-resiliency.v1/retrier"
	"gopkg.in/h2non/gentleman.v2"
	"gopkg.in/h2non/gentleman.v2/plugins/timeout"

	"github.com/soluto/dqd/config"
	"github.com/soluto/dqd/metrics"
	"github.com/soluto/dqd/utils"
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
	conf := viper.New()
	conf.SetConfigType("yaml")
	conf.SetDefault("logLevel", 1)
	conf.SetDefault("metricsPort", 8888)

	err := gofigure.Parse(conf, []string{"/etc/dqd", "/dqd/config"})
	if err != nil {
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}
	fmt.Println(len(conf.AllKeys()))
	logLevel := conf.GetInt("LOG_LEVEL")
	zerolog.SetGlobalLevel(zerolog.Level(logLevel))

	metricsPort := conf.GetInt("METRICS_PORT")

	waitForHealth()

	app := config.CreateApp(conf)

	go metrics.Start(metricsPort)

	ctx := utils.ContextWithSignal(context.Background())

	for _, worker := range app.Workers {
		go worker.Start(ctx)
	}

	for _, listeners := range app.Listeners {
		go listeners.Listen()
	}

	select {
	case <-ctx.Done():
		logger.Info().Msg("Shutting Down")
	}
}
