package health

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
)

func CreateHealthHandler() httprouter.Handle {
	//handler := promhttp.Handler()
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {

	} //httprouter.Handle. promhttp.Handler()
}
