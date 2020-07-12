package metrics

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/soluto/dqd/metrics"
)

func CreateMetricsHandler() httprouter.Handle {
	prometheus.MustRegister(
		metrics.HandlerProcessingHistogram,
		metrics.PipeProcessingMessagesHistogram,
		metrics.WorkerBatchSizeGauge,
		metrics.WorkerMaxConcurrencyGauge,
	)
	handler := promhttp.Handler()
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		handler.ServeHTTP(w, r)
	}
}
