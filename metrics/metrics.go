package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
)

var queueLabels = []string{"queueName"}
var MaxConcurrencyGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: "offload",
	Subsystem: "concurrent",
	Name:      "max",
	Help:      "max concurrent messages",
}, queueLabels)
var BatchSizeGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: "offload",
	Subsystem: "concurrent",
	Name:      "size",
	Help:      "concurrent message handling",
}, queueLabels)
var OffloadHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "offload",
	Name:      "handle",
	Help:      "offloading total handling histogram",
}, queueLabels)

var messageHistogramLabels = []string{"queueName", "success"}
var GetMessagesHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "queue",
	Subsystem: "message",
	Name:      "get",
	Help:      "histogram for get queue messages",
}, messageHistogramLabels)
var DeleteMessagesHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "queue",
	Subsystem: "message",
	Name:      "delete",
	Help:      "histogram for delete queue messages",
}, messageHistogramLabels)
var PostMessagesHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "queue",
	Subsystem: "message",
	Name:      "post",
	Help:      "histogram for post queue messages",
}, messageHistogramLabels)
var HandleMessagesHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "queue",
	Subsystem: "message",
	Name:      "handle",
	Help:      "histogram for handle queue messages",
}, messageHistogramLabels)
var MessageDequeueCount = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "queue",
	Subsystem: "message",
	Name:      "dequeue_count",
	Help:      "message dequeue counter",
}, queueLabels)

func StartTimer(h *prometheus.HistogramVec) func(...string) {
	start := time.Now()
	return func(labels ...string) {
		total := time.Since(start)
		h.WithLabelValues(labels...).Observe(float64(total) / float64(time.Second))
	}
}

func Start(metricsPort string) {
	prometheus.MustRegister(
		MaxConcurrencyGauge,
		BatchSizeGauge,
		OffloadHistogram,
		GetMessagesHistogram,
		DeleteMessagesHistogram,
		PostMessagesHistogram,
		HandleMessagesHistogram,
		MessageDequeueCount)

	http.Handle("/metrics", promhttp.Handler())
	err := http.ListenAndServe(":"+metricsPort, nil)
	if err != nil {
		log.Error().Err(err).Str("scope", "Metrics").Msg("Failed starting prometheus service")
	}
}
