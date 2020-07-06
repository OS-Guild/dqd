package config

import (
	"fmt"

	"github.com/soluto/dqd/handlers"
	"github.com/soluto/dqd/listeners"
	"github.com/soluto/dqd/pipe"
	"github.com/soluto/dqd/providers/azure"
	"github.com/soluto/dqd/providers/servicebus"
	"github.com/soluto/dqd/providers/sqs"
	"github.com/soluto/dqd/utils"
	v1 "github.com/soluto/dqd/v1"
	"github.com/spf13/viper"
)

type App struct {
	Sources   map[string]*v1.Source
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
	"service-bus": {
		&servicebus.ServiceBusClientFactory{},
		&servicebus.ServiceBusClientFactory{},
	},
	"io": {
		&utils.IoSourceFactory{},
		&utils.IoSourceFactory{},
	},
}

func createSources(v *viper.Viper) map[string]*v1.Source {
	sources := map[string]*v1.Source{}
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

func getSource(sources map[string]*v1.Source, sourceName string) *v1.Source {
	source, exists := sources[sourceName]
	if !exists {
		panic(fmt.Sprintf("Missing source definition: %v", sourceName))
	}
	return source
}

func getPipeSources(sources map[string]*v1.Source, v *viper.Viper) (pipeSources []*v1.Source) {
	sourcesConfig := v.GetStringSlice("sources")
	for _, s := range sourcesConfig {
		pipeSources = append(pipeSources, getSource(sources, s))
	}
	if len(pipeSources) == 0 {
		pipeSources = []*v1.Source{getSource(sources, v.GetString("source"))}
	}
	return
}

func createHandler(v *viper.Viper) handlers.Handler {
	if v == nil {
		panic("no handler define for pipe, use 'none' handler if it's the desired behavior")
	}
	if v.Get("none") != nil {
		return handlers.None
	}
	v.SetDefault("http.path", "/")
	v.SetDefault("http.host", "localhost")
	v.SetDefault("http.port", 80)

	httpEndpoint := v.GetString("http.endpoint")
	if httpEndpoint == "" {
		httpEndpoint = fmt.Sprintf("http://%v:%v%v", v.GetString("http.host"), v.GetString("http.port"), v.GetString("http.path"))
	}
	return handlers.NewHttpHandler(httpEndpoint)
}

func createWorkers(v *viper.Viper, sources map[string]*v1.Source) []*pipe.Worker {
	var wList []*pipe.Worker
	pipesConfig := utils.ViperSubMap(v, "pipes")
	for name, pipeConfig := range pipesConfig {
		pipeConfig.SetDefault("rate.init", 10)
		pipeConfig.SetDefault("rate.min", 1)
		pipeConfig.SetDefault("rate.window", "30s")
		pipeConfig.SetDefault("source", "default")
		handler := createHandler(pipeConfig.Sub("handler"))

		pipeSources := getPipeSources(sources, pipeConfig)

		var opts = []pipe.WorkerOption{}
		writeToSource := pipeConfig.GetString("onError.writeTo.source")
		if writeToSource != "" {
			opts = append(opts, pipe.WithErrorSource(getSource(sources, writeToSource)))
		}

		if pipeConfig.IsSet("rate.fixed") {
			opts = append(opts, pipe.WithFixedRate(pipeConfig.GetInt("rate.fixed")))
		} else {
			opts = append(opts, pipe.WithDynamicRate(pipeConfig.GetInt("rate.init"),
				pipeConfig.GetInt("rate.min"),
				pipeConfig.GetDuration("rate.window")))
		}
		output := pipeConfig.GetString("output")
		if output != "" {
			opts = append(opts, pipe.WithOutput(getSource(sources, output)))
		} else {
			opts = append(opts, pipe.WithDynamicRate(pipeConfig.GetInt("rate.init"),
				pipeConfig.GetInt("rate.min"),
				pipeConfig.GetDuration("rate.window")))
		}

		wList = append(wList, pipe.NewWorker(
			name,
			pipeSources,
			handler,
			opts...,
		))
	}
	return wList
}

func createListeners(v *viper.Viper, sources map[string]*v1.Source) []listeners.Listener {
	v.SetDefault("listeners.http.host", "0.0.0.0:9999")
	host := v.GetString("listeners.http.host")
	listener := listeners.Http(host)
	for _, s := range sources {
		listener.Add(s, viper.New())
	}
	return []listeners.Listener{listener}
}

func CreateApp(v *viper.Viper) (_ *App, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()

	err = utils.NormalizeEntityConfig(v, "pipe", "pipes")
	if err != nil {
		return
	}
	err = utils.NormalizeEntityConfig(v, "source", "sources")
	if err != nil {
		return
	}

	sources := createSources(v)
	sources["stdout"] = v1.NewSource(&utils.IoSourceFactory{}, &utils.IoSourceFactory{}, &viper.Viper{}, "stdout")
	listeners := createListeners(v, sources)
	workers := createWorkers(v, sources)
	return &App{
		sources,
		listeners,
		workers,
	}, nil
}
