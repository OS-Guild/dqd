package health

import (
	"encoding/json"
	"net/http"

	"github.com/julienschmidt/httprouter"
	v1 "github.com/soluto/dqd/v1"
)

func CreateHealthHandler(healthChecker v1.HealthChecker) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		s := healthChecker.HealthStatus()
		if !s.IsHealthy() {
			w.WriteHeader(500)
		}
		json.NewEncoder(w).Encode(s)
	}
}
