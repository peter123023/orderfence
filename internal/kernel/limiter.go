package kernel

import (
	"context"
	"sync"
	"time"
)

// Bucket 是速率可自适应的令牌桶：撞 429 就减半，连续成功再小幅提速。
type Bucket struct {
	mu       sync.Mutex
	rate     float64
	maxRate  float64
	minRate  float64
	burst    float64
	tokens   float64
	last     time.Time
	streak   int
	penaltyN int
}

func NewBucket(rate, burst float64) *Bucket {
	if rate <= 0 {
		rate = 1
	}
	if burst < 1 {
		burst = 1
	}
	return &Bucket{
		rate:    rate,
		maxRate: rate * 4,
		minRate: rate / 16,
		burst:   burst,
		tokens:  burst,
		last:    time.Now(),
	}
}

func (b *Bucket) Wait(ctx context.Context) error {
	for {
		b.mu.Lock()
		now := time.Now()
		b.tokens += now.Sub(b.last).Seconds() * b.rate
		if b.tokens > b.burst {
			b.tokens = b.burst
		}
		b.last = now
		if b.tokens >= 1 {
			b.tokens--
			b.mu.Unlock()
			return nil
		}
		need := (1 - b.tokens) / b.rate
		b.mu.Unlock()

		if need <= 0 {
			need = 0.001
		}
		timer := time.NewTimer(time.Duration(need * float64(time.Second)))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (b *Bucket) Penalize() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.rate /= 2
	if b.rate < b.minRate {
		b.rate = b.minRate
	}
	b.streak = 0
	b.penaltyN++
}

func (b *Bucket) Reward() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.streak++
	if b.streak < 8 {
		return
	}
	b.streak = 0
	b.rate *= 1.05
	if b.rate > b.maxRate {
		b.rate = b.maxRate
	}
}

func (b *Bucket) Rate() float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.rate
}

func (b *Bucket) Penalties() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.penaltyN
}
