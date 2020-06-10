package pipe

import (
	"context"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/soluto/dqd/metrics"
	v1 "github.com/soluto/dqd/v1"
)

func (w *Worker) handleErrorRequest(ctx *v1.RequestContext, err error, errProducer v1.Producer) {
	m := ctx.Message()
	w.logger.Warn().Err(err).Msg("Failed to handle messge")
	if !m.Abort(err) {
		if w.writeToErrorSource && errProducer != nil {
			err = errProducer.Produce(ctx, &v1.RawMessage{Data: m.Data()})
		}
		if err != nil {
			w.logger.Error().Err(err).Msg("Failed to abort or recover message")
		}
	}
}

func (w *Worker) handleRequest(ctx *v1.RequestContext) (_ *v1.RawMessage, err error) {
	start := time.Now()
	defer func() {
		source := ctx.Source()
		t := float64(time.Since(start)) / float64(time.Second)
		metrics.HandlerProcessingHistogram.WithLabelValues(w.name, source, strconv.FormatBool(err == nil)).Observe(t)
	}()
	return w.handler.Handle(ctx, ctx.Message())
}

func (w *Worker) handleResults(ctx context.Context, results chan *v1.RequestContext) error {
	var outputP v1.Producer
	var errorP v1.Producer
	if w.output != nil {
		outputP = w.output.CreateProducer()
	}
	if w.errorSource != nil {
		errorP = w.errorSource.CreateProducer()
	}
	done := make(chan error)
	defer close(done)
	for reqCtx := range results {
		select {
		case err := <-done:
			return err
		case <-ctx.Done():
			return nil
		default:
		}
		go func(reqCtx *v1.RequestContext) {
			m, err := reqCtx.Result()
			defer func() {
				defer func() {
					t := float64(time.Since(reqCtx.DequeueTime())) / float64(time.Second)
					metrics.PipeProcessingMessagesHistogram.WithLabelValues(w.name, reqCtx.Source(), strconv.FormatBool(err == nil)).Observe(t)
				}()
				if err != nil {
					w.handleErrorRequest(reqCtx, err, errorP)
				}
			}()

			if err != nil {
				return
			}
			err = reqCtx.Complete()
			if err != nil {
				return
			}
			if m != nil && outputP != nil {
				err := outputP.Produce(reqCtx, m)
				if err != nil {
					return
				}
			}
		}(reqCtx)
	}
	return nil
}

func (w *Worker) HealthStatus() v1.HealthStatus {
	return w.probe.HealthStatus()
}

func (w *Worker) readMessages(ctx context.Context, messages chan *v1.RequestContext, results chan *v1.RequestContext) error {
	maxConcurrencyGauge := metrics.WorkerMaxConcurrencyGauge.WithLabelValues(w.name)
	batchSizeGauge := metrics.WorkerBatchSizeGauge.WithLabelValues(w.name)

	var count, lastBatch int64
	maxItems := int64(w.concurrencyStartingPoint)
	minConcurrency := int64(w.minConcurrency)

	maxConcurrencyGauge.Set(float64(maxItems))

	//TODO #15 Consider replacing this code with a goroutine pool library
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

			go func(r *v1.RequestContext) {
				result, err := w.handleRequest(r)
				atomic.AddInt64(&count, -1)
				if !w.fixedRate {
					atomic.AddInt64(&lastBatch, 1)
				}
				select {
				case <-ctx.Done():
				default:
					results <- r.WithResult(result, err)
				}

			}(message)
		}
	}()

	// Handle throughput
	if !w.fixedRate {
		go func() {
			var prev int64
			timer := time.NewTimer(w.dynamicRateBatchWindow)
			shouldUpscale := true
			w.logger.Debug().Int64("concurrency", maxItems).Msg("Using dynamic concurrency")
			for {
				timer.Reset(w.dynamicRateBatchWindow)

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
				} else if maxItems > minConcurrency {
					atomic.AddInt64(&maxItems, -1)
				}
				maxConcurrencyGauge.Set(float64(maxItems))

				prev = curr
				w.logger.Debug().Int64("concurrency", maxItems).Float64("rate", float64(curr)/w.dynamicRateBatchWindow.Seconds()).Msg("tuning concurrency")
			}
		}()
	}
	done := make(chan error)
	defer close(done)
	for _, s := range w.sources {
		go func(ss *v1.Source) {
			w.logger.Info().Str("source", ss.Name).Msg("Start reading from source")
			consumer := ss.CreateConsumer()

			err := consumer.Iter(ctx, v1.NextMessage(func(m v1.Message) {
				select {
				case <-ctx.Done():
				default:
					messages <- v1.CreateRequestContext(ctx, ss.Name, m)
				}
			}))
			select {
			case <-ctx.Done():
			default:
				done <- err
			}
		}(s)
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return nil
	}
}

func (w *Worker) Start(ctx context.Context) error {
	w.logger.Info().Msg("Starting pipe")
	messages := make(chan *v1.RequestContext, w.minConcurrency)
	defer close(messages)
	results := make(chan *v1.RequestContext, w.minConcurrency)
	defer close(results)
	done := make(chan error)

	innerContext, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		err := w.readMessages(innerContext, messages, results)
		select {
		case <-ctx.Done():
		default:
			done <- err
		}
	}()

	go func() {
		err := w.handleResults(innerContext, results)
		select {
		case <-ctx.Done():
		default:
			done <- err
		}
	}()

	select {

	case <-ctx.Done():
		return nil
	case err := <-done:
		return err
	}
}
