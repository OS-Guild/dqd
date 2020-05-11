package listeners

import (
	"context"
	"io/ioutil"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/rs/zerolog/log"
	v1 "github.com/soluto/dqd/v1"
	"github.com/spf13/viper"
)

var logger = log.With().Str("scope", "HttpListener").Logger()

type HttpListener struct {
	address string
	router  *mux.Router
	context context.Context
}

func Http(address string) Listener {
	return &HttpListener{
		address: address,
		router:  mux.NewRouter(),
	}
}

func (h *HttpListener) Add(source v1.Source, options *viper.Viper) {
	p := source.CreateProducer()
	logger.Info().Str("source", source.Name).Msg("adding source route")
	h.router.Methods("post").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		msg, err := ioutil.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(500)
			return
		}
		err = p.Produce(h.context, &v1.RawMessage{
			Data: string(msg),
		})
		if err != nil {
			logger.Warn().Err(err).Msg("Error producing item")
			w.WriteHeader(500)
			return
		}
	})
}

func (h *HttpListener) Listen(ctx context.Context) error {
	srv := &http.Server{Addr: h.address}
	h.context = ctx
	e := make(chan error, 1)
	go func() {
		srv.Handler = h.router
		e <- srv.ListenAndServe()
	}()

	select {
	case err := <-e:
		return err
	case <-ctx.Done():
		return nil
	}
}
