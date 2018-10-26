package util

import "time"

const (
	stateCreated = iota
	stateRunning
	stateFinished
	stateMeasuring
	stateNilProfiler
)

type SimpleProfiler struct {
	state int

	runStart            time.Time
	currentMeasureStart time.Time

	totalMeasuredDuration time.Duration
	totalRunningDuration  time.Duration
}

func NewSimpleProfiler() *SimpleProfiler {
	return &SimpleProfiler{
		state: stateCreated,
	}
}

var nilSimpleProfiler = SimpleProfiler{state: stateNilProfiler}

func NewNilSimpleProfiler() *SimpleProfiler {
	return &nilSimpleProfiler
}

func (p *SimpleProfiler) Start() {
	if p.state != stateCreated {
		panic("Invalid state")
	}

	p.state = stateRunning
	p.runStart = time.Now()
}

func (p *SimpleProfiler) StartMeasuring() {
	if p.state == stateNilProfiler {
		return
	}

	if p.state != stateRunning {
		panic("Invalid state")
	}

	p.state = stateMeasuring
	p.currentMeasureStart = time.Now()
}

func (p *SimpleProfiler) FinishMeasuring() {
	measuredDuration := time.Since(p.currentMeasureStart)

	if p.state == stateNilProfiler {
		return
	}

	if p.state != stateMeasuring {
		panic("Invalid state")
	}

	p.state = stateRunning
	p.totalMeasuredDuration += measuredDuration
}

func (p *SimpleProfiler) Finish() {
	measuredDuration := time.Since(p.runStart)

	if p.state != stateRunning {
		panic("Invalid state")
	}

	p.state = stateFinished
	p.totalRunningDuration = measuredDuration
}

func (p *SimpleProfiler) GetTotalMeasuredDuration() time.Duration {
	if p.state != stateRunning && p.state != stateFinished {
		panic("Invalid state")
	}

	return p.totalMeasuredDuration
}

func (p *SimpleProfiler) GetTotalRunningDuration() time.Duration {
	if p.state != stateFinished {
		panic("Invalid state")
	}

	return p.totalRunningDuration
}

func (p *SimpleProfiler) GetMeasuredDurationRatio() float64 {
	if p.state != stateFinished {
		panic("Invalid state")
	}

	return float64(p.totalMeasuredDuration.Nanoseconds()) / float64(p.totalRunningDuration.Nanoseconds())
}
