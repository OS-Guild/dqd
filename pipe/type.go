package pipe

import (
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/soluto/dqd/handlers"
	"github.com/soluto/dqd/health"
	v1 "github.com/soluto/dqd/v1"
)

type WorkerOption func(w *Worker)

type Worker struct {
	name                     string
	sources                  []*v1.Source
	output                   *v1.Source
	errorSource              *v1.Source
	handler                  handlers.Handler
	logger                   *zerolog.Logger
	maxDequeueCount          int64
	fixedRate                bool
	dynamicRateBatchWindow   time.Duration
	concurrencyStartingPoint int
	minConcurrency           int
	writeToErrorSource       bool
	probe                    *health.Probe
}

func WithDynamicRate(start, min int, windowSize time.Duration) WorkerOption {
	return WorkerOption(func(w *Worker) {
		w.fixedRate = false
		w.concurrencyStartingPoint = start
		w.minConcurrency = min
		w.dynamicRateBatchWindow = windowSize
	})
}

func WithFixedRate(rate int) WorkerOption {
	return WorkerOption(func(w *Worker) {
		w.fixedRate = true
		w.concurrencyStartingPoint = rate
	})
}

func WithErrorSource(source *v1.Source) WorkerOption {
	return WorkerOption(func(w *Worker) {
		w.writeToErrorSource = true
		w.errorSource = source
	})
}

func WithOutput(source *v1.Source) WorkerOption {
	return WorkerOption(func(w *Worker) {
		w.output = source
	})
}

func NewWorker(name string, sources []*v1.Source, handler handlers.Handler, opts ...WorkerOption) *Worker {
	l := log.With().Str("scope", "Worker").Str("pipe", name).Logger()
	w := &Worker{
		name:    name,
		sources: sources,
		handler: handler,
		logger:  &l,
		probe:   health.MakeProbe(),
	}
	for _, o := range opts {
		o(w)
	}
	return w
}
