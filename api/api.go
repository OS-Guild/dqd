package api

import (
	"context"
	"fmt"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/soluto/dqd/api/health"
	"github.com/soluto/dqd/api/metrics"
)

func Start(ctx context.Context, port int) error {
	router := httprouter.New()
	router.GET("metrics", metrics.CreateMetricsHandler())
	router.GET("health", health.CreateHealthHandler())
	srv := &http.Server{Addr: fmt.Sprintf(":%v", port)}
	e := make(chan error, 1)
	go func() {
		srv.Handler = router
		e <- srv.ListenAndServe()
	}()

	select {
	case err := <-e:
		return err
	case <-ctx.Done():
		return nil
	}
}
