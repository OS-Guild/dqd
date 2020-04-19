package pipe

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/soluto/dqd/handlers"
	"github.com/soluto/dqd/metrics"
	v1 "github.com/soluto/dqd/v1"
)

var logger = log.With().Str("scope", "Worker").Logger()

type Worker struct {
	MaxDequeueCount          int64
	Source, ErrorSource      v1.Source
	Handler                  handlers.Handler
	FixedRate                bool
	ConcurrencyStartingPoint int64
	MinConcurrency           int64
}

func (o *Worker) Process(message v1.Message) {
	end := metrics.StartTimer(metrics.OffloadHistogram)
	err := o.Handler.Handle(message)
	if err != nil {
		if !message.Retryable() {
			message.Done()
		}
	} else {
		message.Done()
	}
	end(o.Source.Name)
}

func (o *Worker) Start(ctx context.Context) {
	maxConcurrencyGauge := metrics.MaxConcurrencyGauge.WithLabelValues(o.Source.Name)
	batchSizeGauge := metrics.BatchSizeGauge.WithLabelValues(o.Source.Name)

	var count, lastBatch int64
	maxItems := o.ConcurrencyStartingPoint
	messages := make(chan v1.Message, o.MinConcurrency)
	ctx, cancel := context.WithCancel(ctx)

	maxConcurrencyGauge.Set(float64(maxItems))

	// Handle messages
	go func() {
		for message := range messages {
			for count >= maxItems {
				time.Sleep(10 * time.Millisecond)
			}

			atomic.AddInt64(&count, 1)

			go func(m v1.Message) {
				o.Process(m)

				atomic.AddInt64(&count, -1)
				if !o.FixedRate {
					atomic.AddInt64(&lastBatch, 1)
				}
			}(message)
		}

		cancel()
	}()

	// Handle throughput
	if !o.FixedRate {
		go func() {
			var prev int64
			cycleDuration := 30 * time.Second
			timer := time.NewTimer(cycleDuration)
			shouldUpscale := true
			for {
				timer.Reset(cycleDuration)

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
				} else if maxItems > o.MinConcurrency {
					atomic.AddInt64(&maxItems, -1)
				}
				maxConcurrencyGauge.Set(float64(maxItems))

				prev = curr
			}
		}()
	}
	logger.Info().Msg("Init worker")
	consumer := o.Source.CreateConsumer()
	consumer.Iter(ctx, messages)
}
