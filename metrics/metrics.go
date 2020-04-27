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

var WorkerProcessesMessagesHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "worker",
	Name:      "handle",
	Help:      "total processed messages",
}, []string{"source", "success"})

func StartTimer(h *prometheus.HistogramVec) func(...string) {
	start := time.Now()
	return func(labels ...string) {
		total := time.Since(start)
		h.WithLabelValues(labels...).Observe(float64(total) / float64(time.Second))
	}
}

func Start(metricsPort int) {
	prometheus.MustRegister(
		WorkerProcessesMessagesHistogram,
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
