package pipe

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/soluto/dqd/handlers"
	"github.com/soluto/dqd/metrics"
	v1 "github.com/soluto/dqd/v1"
)

var logger = log.With().Str("scope", "Worker").Logger()

type WorkerOptions struct {
	MaxDequeueCount          int64
	FixedRate                bool
	DynamicRateBatchWindow   time.Duration
	ConcurrencyStartingPoint int64
	MinConcurrency           int64
}

type Worker struct {
	source  v1.Source
	options WorkerOptions
	handler handlers.Handler
	logger  *zerolog.Logger
}

func NewWorker(source v1.Source, handler handlers.Handler, opts WorkerOptions) *Worker {
	logger = logger.With().Str("source", source.Name).Logger()
	return &Worker{
		source,
		opts,
		handler,
		&logger,
	}
}

func (w *Worker) Process(ctx context.Context, message v1.Message) {
	end := metrics.StartTimer(metrics.OffloadHistogram)
	err := w.handler.Handle(ctx, message)
	if err != nil {
		logger.Warn().Err(err).Msg("error processing message")
		if !message.Retryable() {
			message.Done()
		}
	} else {
		message.Done()
	}
	end(w.source.Name)
}

func (w *Worker) Start(ctx context.Context) {
	maxConcurrencyGauge := metrics.MaxConcurrencyGauge.WithLabelValues(w.source.Name)
	batchSizeGauge := metrics.BatchSizeGauge.WithLabelValues(w.source.Name)

	var count, lastBatch int64
	maxItems := w.options.ConcurrencyStartingPoint
	messages := make(chan v1.Message, w.options.MinConcurrency)
	defer close(messages)

	maxConcurrencyGauge.Set(float64(maxItems))

	// Handle messages
	go func() {
		for message := range messages {
			select {
			case <-ctx.Done():
				return
			default:
			}
			for count >= maxItems {
				time.Sleep(10 * time.Millisecond)
			}

			atomic.AddInt64(&count, 1)

			go func(m v1.Message) {
				w.Process(ctx, m)

				atomic.AddInt64(&count, -1)
				if !w.options.FixedRate {
					atomic.AddInt64(&lastBatch, 1)
				}
			}(message)
		}
	}()

	// Handle throughput
	if !w.options.FixedRate {
		go func() {
			var prev int64
			timer := time.NewTimer(w.options.DynamicRateBatchWindow)
			shouldUpscale := true
			logger.Debug().Int64("concurrency", maxItems).Msg("Using dynamic concurrency")
			for {
				timer.Reset(w.options.DynamicRateBatchWindow)

				select {
				case <-ctx.Done():
					return
				case <-timer.C:
				}

				curr := atomic.SwapInt64(&lastBatch, 0)
				batchSizeGauge.Set(float64(curr))

				if curr == 0 {
					continue
				}
				if curr < prev {
					shouldUpscale = !shouldUpscale
				}
				if shouldUpscale {
					atomic.AddInt64(&maxItems, 1)
				} else if maxItems > w.options.MinConcurrency {
					atomic.AddInt64(&maxItems, -1)
				}
				maxConcurrencyGauge.Set(float64(maxItems))

				prev = curr
				logger.Debug().Int64("concurrency", maxItems).Float64("rate", float64(curr)/w.options.DynamicRateBatchWindow.Seconds()).Msg("tuning concurrency")
			}
		}()
	}
	logger.Info().Msg("Init worker")
	consumer := w.source.CreateConsumer()
	err := consumer.Iter(ctx, messages)

	if err != nil {
		panic(fmt.Sprintf("error reading from source", err))
	}
}
