package queue

import (
	"sync/atomic"
	"time"

	"../metrics"
	"github.com/rs/zerolog/log"
)

type Offloader struct {
	Client                   Client
	Handler                  Handler
	FixedRate                bool
	ConcurrencyStartingPoint int64
	MinConcurrency           int64
}

var offloadingLogger = log.With().Str("scope", "Offloader")

func (o *Offloader) Offload(message Message) {
	end := metrics.StartTimer(&metrics.OffloadSummary)
	err := o.Handler.Handle(message)
	if err != nil {
		message.Fail()
	} else {
		message.Done()
	}
	end()
}

func (o *Offloader) Start(wait bool) {
	maxConcurrencyGauge := metrics.MaxConcurrencyGauge
	batchSizeGauge := metrics.BatchSizeGauge

	var count, lastBatch int64
	maxItems := o.ConcurrencyStartingPoint
	messages := make(chan Message, o.MinConcurrency)
	stopThroughput := make(chan bool, 1)
	stopIteration := make(chan bool, 1)

	maxConcurrencyGauge.Set(float64(maxItems))

	// Handle messages
	go func() {
		for message := range messages {
			for count >= maxItems {
				time.Sleep(10 * time.Millisecond)
			}

			atomic.AddInt64(&count, 1)

			go func(m Message) {
				o.Offload(m)

				atomic.AddInt64(&count, -1)
				if !o.FixedRate {
					atomic.AddInt64(&lastBatch, 1)
				}
			}(message)
		}

		stopThroughput <- true
	}()

	// Handle throughput
	if !o.FixedRate {
		go func() {
			var prev int64
			shouldUpscale := true
			for {
				select {
				case <-stopThroughput:
					return
				case <-time.After(30 * time.Second):
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

	o.Client.Iter(messages, stopIteration)
}
