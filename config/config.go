package config

import (
	"fmt"

	"github.com/soluto/dqd/handlers"
	"github.com/soluto/dqd/listeners"
	"github.com/soluto/dqd/pipe"
	"github.com/soluto/dqd/providers/azure"
	"github.com/soluto/dqd/providers/sqs"
	"github.com/soluto/dqd/utils"
	v1 "github.com/soluto/dqd/v1"
	"github.com/spf13/viper"
)

type App struct {
	Sources   map[string]v1.Source
	Listeners []listeners.Listener
	Workers   []*pipe.Worker
}

var sourceProviders = map[string]struct {
	v1.ConsumerFactory
	v1.ProducerFactory
}{
	"azure-queue": {
		&azure.AzureQueueClientFactory{},
		&azure.AzureQueueClientFactory{},
	},
	"sqs": {
		&sqs.SQSClientFactory{},
		&sqs.SQSClientFactory{},
	},
}

func createSources(v *viper.Viper) map[string]v1.Source {
	sources := map[string]v1.Source{}
	for sourceName, subSource := range utils.ViperSubMap(v, "sources") {
		sourceType := subSource.GetString("type")
		factory, exist := sourceProviders[sourceType]
		if !exist {
			panic(fmt.Errorf("FATAL - Unkown source provider:%v", sourceType))
		}
		sources[sourceName] = v1.NewSource(factory, factory, subSource, sourceName)
	}
	return sources
}

func createWorkers(v *viper.Viper, sources map[string]v1.Source) []*pipe.Worker {
	var wList []*pipe.Worker
	pipesConfig := utils.ViperSubSlice(v, "pipes", true)
	for _, pipeConfig := range pipesConfig {
		pipeConfig.SetDefault("rate.init", 10)
		pipeConfig.SetDefault("rate.min", 1)
		pipeConfig.SetDefault("rate.static", false)
		pipeConfig.SetDefault("rate.window", "30s")
		pipeConfig.SetDefault("http.path", "/")
		pipeConfig.SetDefault("http.host", "localhost")
		pipeConfig.SetDefault("http.port", 80)
		httpEndpoint := pipeConfig.GetString("http.endpoint")
		if httpEndpoint == "" {
			httpEndpoint = fmt.Sprintf("http://%v:%v%v", pipeConfig.GetString("http.host"), pipeConfig.GetString("http.port"), pipeConfig.GetString("http.path"))
		}

		source, exists := sources[pipeConfig.GetString("source")]
		if !exists {
			panic(fmt.Sprintf("missing source definition: %v", source.Name))
		}

		wList = append(wList, pipe.NewWorker(
			source,
			handlers.NewHttpHandler(httpEndpoint),
			pipe.WorkerOptions{
				FixedRate:                pipeConfig.GetBool("rate.static"),
				ConcurrencyStartingPoint: pipeConfig.GetInt64("rate.init"),
				DynamicRateBatchWindow:   pipeConfig.GetDuration("rate.window"),
				MinConcurrency:           pipeConfig.GetInt64("rate.min"),
			},
		))
	}
	return wList
}

func createListeners(v *viper.Viper, sources map[string]v1.Source) []listeners.Listener {
	v.SetDefault("listeners.http.host", "0.0.0.0:9999")
	host := v.GetString("listeners.http.host")
	listener := listeners.Http(host)
	for _, s := range sources {
		listener.Add(s, viper.New())
	}
	return []listeners.Listener{listener}
}

func CreateApp(v *viper.Viper) App {
	sources := createSources(v)
	listeners := createListeners(v, sources)
	workers := createWorkers(v, sources)
	return App{
		sources,
		listeners,
		workers,
	}
}
