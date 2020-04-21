package listeners

import (
	"io/ioutil"
	"net/http"

	"github.com/gorilla/mux"
	v1 "github.com/soluto/dqd/v1"
	"github.com/spf13/viper"
)

type Dispose func() error

type Listener interface {
	Add(source v1.Source, options *viper.Viper)
	Listen() (Dispose, error)
}

type HttpListener struct {
	address string
	router  *mux.Router
}

func Http(address string) Listener {
	return &HttpListener{
		address: address,
		router:  mux.NewRouter(),
	}
}

func (h *HttpListener) Add(source v1.Source, options *viper.Viper) {
	p := source.CreateProducer()
	println("adding listener source %s", source.Name)
	h.router.Methods("post").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		msg, err := ioutil.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(500)
			return
		}
		err = p.Produce(v1.RawMessage{
			Data: string(msg),
		})
		if err != nil {
			w.WriteHeader(500)
			return
		}
	})
}

func (h *HttpListener) Listen() (Dispose, error) {
	srv := &http.Server{Addr: h.address}
	go func() {
		srv.Handler = h.router
		srv.ListenAndServe()
	}()
	return func() error {
		err := srv.Close()
		return err
	}, nil
}
