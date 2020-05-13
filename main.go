package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gopkg.in/eapache/go-resiliency.v1/retrier"
	"gopkg.in/h2non/gentleman.v2"
	"gopkg.in/h2non/gentleman.v2/plugins/timeout"

	"github.com/soluto/dqd/cmd"
	"github.com/soluto/dqd/config"
	"github.com/soluto/dqd/metrics"
	"github.com/soluto/dqd/utils"
)

var logger = log.With().Str("scope", "Main").Logger()

func waitForHealth() {
	healthEndpoint := os.Getenv("probe")
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
	conf, err := cmd.Load()
	if err != nil {
		cmd.ConfigurationError(err)
	}
	conf.SetDefault("logLevel", 1)
	conf.SetDefault("metricsPort", 8888)
	logLevel := conf.GetInt("logLevel")
	zerolog.SetGlobalLevel(zerolog.Level(logLevel))

	metricsPort := conf.GetInt("metricsPort")

	waitForHealth()

	app, err := config.CreateApp(conf)
	if err != nil {
		cmd.ConfigurationError(err)
	}

	go metrics.Start(metricsPort)

	ctx := utils.ContextWithSignal(context.Background())

	for _, worker := range app.Workers {
		go func() {
			err := worker.Start(ctx)
			if err != nil {
				panic(err)
			}
		}()
	}

	for _, listeners := range app.Listeners {
		go func() {
			err := listeners.Listen(ctx)
			if err != nil {
				panic(err)
			}
		}()
	}

	if len(app.Workers) == 0 && len(app.Sources) == 0 {
		cmd.ConfigurationError(fmt.Errorf("no workers or sources are defiend"))
	}

	select {
	case <-ctx.Done():
		logger.Info().Msg("Shutting Down")
	}
}
