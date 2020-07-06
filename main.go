package main

import (
	"context"
	"fmt"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/soluto/dqd/api"
	"github.com/soluto/dqd/cmd"
	"github.com/soluto/dqd/config"
	"github.com/soluto/dqd/listeners"
	"github.com/soluto/dqd/pipe"
	"github.com/soluto/dqd/utils"
	v1 "github.com/soluto/dqd/v1"
)

var logger = log.With().Str("scope", "Main").Logger()

func GetHealthChecker(workers []*pipe.Worker) v1.HealthChecker {
	checkers := make(map[string]v1.HealthChecker)
	for _, w := range workers {
		checkers[w.Name] = w
	}
	return v1.CombineHealthCheckers(checkers)
}

func main() {
	conf, err := cmd.Load()
	if err != nil {
		cmd.ConfigurationError(err)
	}
	conf.SetDefault("logLevel", 1)
	conf.SetDefault("apiPort", 8888)
	logLevel := conf.GetInt("logLevel")
	zerolog.SetGlobalLevel(zerolog.Level(logLevel))

	apiPort := conf.GetInt("metricsPort")
	if apiPort == 0 {
		apiPort = conf.GetInt("apiPort")
	}

	app, err := config.CreateApp(conf)
	if err != nil {
		cmd.ConfigurationError(err)
	}

	ctx := utils.ContextWithSignal(context.Background())

	for _, worker := range app.Workers {
		go func(worker *pipe.Worker) {
			err := worker.Start(ctx)
			if err != nil {
				panic(err)
			}
		}(worker)
	}

	for _, listener := range app.Listeners {
		go func(listener listeners.Listener) {
			err := listener.Listen(ctx)
			if err != nil {
				panic(err)
			}
		}(listener)
	}

	if len(app.Workers) == 0 && len(app.Sources) == 0 {
		cmd.ConfigurationError(fmt.Errorf("no workers or sources are defiend"))
	}

	go api.Start(ctx, apiPort, GetHealthChecker(app.Workers))

	select {
	case <-ctx.Done():
		logger.Info().Msg("Shutting Down")
	}
}
