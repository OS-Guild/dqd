package v1

import "fmt"

type healthStatusValue string

const (
	Healthy = healthStatusValue("Healthy")
	Init    = healthStatusValue("Init")
)

func Error(err error) healthStatusValue {
	return healthStatusValue(fmt.Sprintf("Error - %v", err.Error()))
}

type HealthStatus map[string]healthStatusValue

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

type CompositeHealthChecker struct {
	checkers map[string]HealthChecker
}

func (c *CompositeHealthChecker) HealthStatus() HealthStatus {
	s := HealthStatus{}
	for k, c := range c.checkers {
		s.Add(c.HealthStatus(), k)
	}
	return s
}

func CombineHealthCheckers(checkers map[string]HealthChecker) HealthChecker {
	return &CompositeHealthChecker{
		checkers,
	}
}
