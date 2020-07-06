package health

import v1 "github.com/soluto/dqd/v1"

type Probe struct {
	inputs chan struct {
		v1.HealthStatus
		string
	}
	checks chan struct {
		v1.HealthChecker
		string
	}
	current v1.HealthStatus
}

func (p *Probe) UpdateStatus(status v1.HealthStatus, prefix string) {
	p.inputs <- struct {
		v1.HealthStatus
		string
	}{status, prefix}
}

func (p *Probe) SendCheck(checker v1.HealthChecker, prefix string) {
	p.checks <- struct {
		v1.HealthChecker
		string
	}{checker, prefix}
}

func (p *Probe) HealthStatus() v1.HealthStatus {
	return p.current
}

func (p *Probe) run() {
	for {
		select {
		case m := <-p.checks:
			p.current.Add(m.HealthChecker.HealthStatus(), m.string)
		case m := <-p.inputs:
			p.current.Add(m.HealthStatus, m.string)
		}
	}
}

func MakeProbe() *Probe {
	p := &Probe{
		inputs: make(chan struct {
			v1.HealthStatus
			string
		}),
		checks: make(chan struct {
			v1.HealthChecker
			string
		}),
		current: v1.HealthStatus{},
	}
	go p.run()
	return p
}
