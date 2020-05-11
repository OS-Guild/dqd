package metrics

import (
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
)

var logger = log.With().Str("scope", "Metrics").Logger()

var WorkerMaxConcurrencyGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: "worker",
	Subsystem: "concurrent",
	Name:      "max",
	Help:      "max concurrent messages",
}, []string{"source"})
var WorkerBatchSizeGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: "worker",
	Subsystem: "concurrent",
	Name:      "size",
	Help:      "concurrent message handling",
}, []string{"source"})

var HandlerProcessingHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "worker",
	Name:      "handler_processing",
	Help:      "handler processing time",
}, []string{"pipe", "source", "success"})

var PipeProcessingMessagesHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "worker",
	Name:      "pipe_processing",
	Help:      "Pipe processing messages",
}, []string{"pipe", "source", "success"})

func StartTimer(h *prometheus.HistogramVec) func(...string) {
	start := time.Now()
	return func(labels ...string) {
		total := time.Since(start)
		h.WithLabelValues(labels...).Observe(float64(total) / float64(time.Second))
	}
}

func Start(metricsPort int) {
	prometheus.MustRegister(
		HandlerProcessingHistogram,
		PipeProcessingMessagesHistogram,
		WorkerBatchSizeGauge,
		WorkerMaxConcurrencyGauge,
	)

	http.Handle("/metrics", promhttp.Handler())
	logger.Info().Msgf("listening port for metrics: %v", metricsPort)
	err := http.ListenAndServe(fmt.Sprintf(":%v", metricsPort), nil)
	if err != nil {
		logger.Error().Err(err).Str("scope", "Metrics").Msg("Failed starting prometheus service")
	}
}
