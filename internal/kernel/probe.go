package kernel

import (
	"sort"
	"sync"
	"time"
)

// Sample 是一次请求的时间点。Sent 到 Ack 是线上往返，Start 到 Ack 含排队与退避。
type Sample struct {
	Op       string
	ClientID string
	Start    time.Time
	Sent     time.Time
	Ack      time.Time
	Err      error
}

func (s Sample) Wire() time.Duration { return s.Ack.Sub(s.Sent) }
func (s Sample) End() time.Duration  { return s.Ack.Sub(s.Start) }

type Stats struct {
	N       int
	Errors  int
	P50     time.Duration
	P90     time.Duration
	P99     time.Duration
	Max     time.Duration
	Mean    time.Duration
	WireP50 time.Duration
}

type Probe struct {
	mu      sync.Mutex
	samples []Sample
}

func NewProbe() *Probe { return &Probe{} }

func (p *Probe) Record(s Sample) {
	if s.Ack.IsZero() {
		s.Ack = time.Now()
	}
	p.mu.Lock()
	p.samples = append(p.samples, s)
	p.mu.Unlock()
}

// Stats 按操作类型统计，op 为空表示全部。
func (p *Probe) Stats(op string) Stats {
	p.mu.Lock()
	defer p.mu.Unlock()

	var pick []Sample
	for _, s := range p.samples {
		if op == "" || s.Op == op {
			pick = append(pick, s)
		}
	}
	return summarize(pick)
}

func summarize(pick []Sample) Stats {
	st := Stats{N: len(pick)}
	if len(pick) == 0 {
		return st
	}
	var end []time.Duration
	var wire []time.Duration
	var total time.Duration
	for _, s := range pick {
		if s.Err != nil {
			st.Errors++
		}
		d := s.End()
		end = append(end, d)
		total += d
		wire = append(wire, s.Wire())
	}
	sort.Slice(end, func(i, j int) bool { return end[i] < end[j] })
	sort.Slice(wire, func(i, j int) bool { return wire[i] < wire[j] })

	st.Mean = total / time.Duration(len(pick))
	st.P50 = quantile(end, 0.50)
	st.P90 = quantile(end, 0.90)
	st.P99 = quantile(end, 0.99)
	st.Max = end[len(end)-1]
	st.WireP50 = quantile(wire, 0.50)
	return st
}

func quantile(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * q)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
