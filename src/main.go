package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gopkg.in/eapache/go-resiliency.v1/retrier"
	"gopkg.in/h2non/gentleman.v2"
	"gopkg.in/h2non/gentleman.v2/plugins/timeout"

	"./metrics"
	"./providers/azure"
	"./queue"
	"./utils"
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
	logLevel := utils.GetenvInt("LOG_LEVEL", 1)
	zerolog.SetGlobalLevel(zerolog.Level(logLevel))

	metricsPort := os.Getenv("METRICS_PORT")
	if metricsPort == "" {
		metricsPort = "8888"
	}

	waitForHealth()

	queueClient := (&azure.AzureClientFactory{}).Create()
	endpoint := utils.GetenvRequired("ENDPOINT")

	offloader := queue.Offloader{
		Client:                   queueClient,
		Handler:                  queue.NewHandler(endpoint),
		FixedRate:                strings.ToLower(os.Getenv("USE_FIXED_RATE")) == "true",
		ConcurrencyStartingPoint: utils.GetenvInt("CONCURRENCY_STARTING_POINT", 10),
		MinConcurrency:           utils.GetenvInt("MIN_CONCURRENCY", 1),
	}

	go metrics.Start(metricsPort)

	offloader.Start(true)
}
