package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
)

var MaxConcurrencyGauge = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "offload",
	Subsystem: "concurrent",
	Name:      "max",
	Help:      "max concurrent messages",
})
var BatchSizeGauge = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "offload",
	Subsystem: "concurrent",
	Name:      "size",
	Help:      "concurrent message handling",
})
var OffloadSummary = prometheus.NewSummary(prometheus.SummaryOpts{
	Namespace: "offload",
	Name:      "handle",
	Help:      "offloading total handling summary",
})
var MessageDequeueCount = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "queue",
	Subsystem: "message",
	Name:      "dequeue_count",
	Help:      "message dequeue counter",
})

var messageSummaryLabels = []string{"success"}
var GetMessagesSummary = prometheus.NewSummaryVec(prometheus.SummaryOpts{
	Namespace: "queue",
	Subsystem: "message",
	Name:      "get",
	Help:      "summary for get queue messages",
}, messageSummaryLabels)
var DeleteMessagesSummary = prometheus.NewSummaryVec(prometheus.SummaryOpts{
	Namespace: "queue",
	Subsystem: "message",
	Name:      "delete",
	Help:      "summary for delete queue messages",
}, messageSummaryLabels)
var PostMessagesSummary = prometheus.NewSummaryVec(prometheus.SummaryOpts{
	Namespace: "queue",
	Subsystem: "message",
	Name:      "post",
	Help:      "summary for post queue messages",
}, messageSummaryLabels)
var HandleMessagesSummary = prometheus.NewSummaryVec(prometheus.SummaryOpts{
	Namespace: "queue",
	Subsystem: "message",
	Name:      "handle",
	Help:      "summary for handle queue messages",
}, messageSummaryLabels)

func StartTimerWithLabels(s *prometheus.SummaryVec) func(...string) {
	start := time.Now()
	return func(labels ...string) {
		total := time.Since(start)
		s.WithLabelValues(labels...).Observe(float64(total))
	}
}

func StartTimer(s *prometheus.Summary) func() {
	start := time.Now()
	return func() {
		total := time.Since(start)
		obs := *s
		obs.Observe(float64(total))
	}
}

func Start(metricsPort string) {
	prometheus.MustRegister(
		MaxConcurrencyGauge,
		BatchSizeGauge,
		OffloadSummary,
		GetMessagesSummary,
		DeleteMessagesSummary,
		PostMessagesSummary,
		HandleMessagesSummary,
		MessageDequeueCount)

	http.Handle("/metrics", promhttp.Handler())
	err := http.ListenAndServe(":"+metricsPort, nil)
	if err != nil {
		log.Error().Err(err).Str("scope", "Metrics").Msg("Failed starting prometheus service")
	}
}
