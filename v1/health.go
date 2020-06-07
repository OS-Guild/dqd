package v1

import "fmt"

type healthStatus string

const (
	Healthy = healthStatus("Healthy")
	Init    = healthStatus("Init")
)

func Error(err error) healthStatus { return healthStatus(fmt.Sprintf("Error - %v", err.Error())) }

type HealthStatus map[string]healthStatus

type HealthChecker interface {
	HealthStatus() HealthStatus
}

func (h HealthStatus) IsHealthy() bool {
	for _, v := range h {
		if v != Healthy {
			return false
		}
	}
	return true
}

func (h HealthStatus) Add(h2 HealthStatus, prefix string) HealthStatus {
	for k, v := range h2 {
		h[fmt.Sprintf("%v.%v", prefix, k)] = v
	}
	return h
}
