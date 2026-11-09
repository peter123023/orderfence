// Package sim 是内存版的订单场所，用来复现限流、回执丢失、post-only 拒单、部分成交。
// 同一个种子跑出一模一样的故障序列。
package sim

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/peter123023/orderfence/internal/venue"
)

// Config 是模拟场所的行为参数。
type Config struct {
	Name        string
	Latency     time.Duration
	Jitter      time.Duration
	LimitPerSec int     // 0 表示不限
	TimeoutRate float64 // 订单已生效但回执丢失的概率
	Seed        int64
}

type Sim struct {
	cfg Config

	mu     sync.Mutex
	seq    int
	orders map[string]*venue.Order
	hits   []time.Time
	rnd    *rand.Rand
	calls  int
}

func New(cfg Config) *Sim {
	if cfg.Name == "" {
		cfg.Name = "sim"
	}
	if cfg.Latency == 0 {
		cfg.Latency = 2 * time.Millisecond
	}
	return &Sim{
		cfg:    cfg,
		orders: make(map[string]*venue.Order),
		rnd:    rand.New(rand.NewSource(cfg.Seed)),
	}
}

func (s *Sim) Name() string { return s.cfg.Name }

func (s *Sim) Calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (s *Sim) Submit(ctx context.Context, req venue.SubmitReq) (venue.Ack, error) {
	s.sleep(ctx)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++

	if err := s.rateLocked(); err != nil {
		return venue.Ack{}, err
	}
	if _, ok := s.orders[req.ClientID]; ok {
		return venue.Ack{}, venue.ErrDuplicate
	}
	if req.PostOnly && s.wouldCrossLocked(req.Symbol, req.Side, req.Price) {
		return venue.Ack{}, venue.ErrWouldCross
	}

	lost := s.rnd.Float64() < s.cfg.TimeoutRate
	o := s.placeLocked(req)
	if lost {
		return venue.Ack{}, venue.ErrTimeout
	}
	return ackOf(o), nil
}

// SubmitBatch 只付一次网络时延。
func (s *Sim) SubmitBatch(ctx context.Context, reqs []venue.SubmitReq) ([]venue.Ack, error) {
	s.sleep(ctx)

	acks := make([]venue.Ack, 0, len(reqs))
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++

	if err := s.rateLocked(); err != nil {
		return nil, err
	}
	for _, req := range reqs {
		if o, ok := s.orders[req.ClientID]; ok {
			acks = append(acks, ackOf(o))
			continue
		}
		if req.PostOnly && s.wouldCrossLocked(req.Symbol, req.Side, req.Price) {
			acks = append(acks, venue.Ack{ClientID: req.ClientID, State: venue.Rejected})
			continue
		}
		acks = append(acks, ackOf(s.placeLocked(req)))
	}
	return acks, nil
}

func (s *Sim) Cancel(ctx context.Context, req venue.CancelReq) (venue.Ack, error) {
	s.sleep(ctx)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++

	if err := s.rateLocked(); err != nil {
		return venue.Ack{}, err
	}
	o, ok := s.orders[req.ClientID]
	if !ok || o.State.Terminal() {
		return venue.Ack{}, venue.ErrUnknownOrder
	}
	o.State = venue.Canceled
	o.UpdatedAt = time.Now()
	if s.rnd.Float64() < s.cfg.TimeoutRate {
		return venue.Ack{}, venue.ErrTimeout
	}
	return ackOf(o), nil
}

func (s *Sim) CancelBatch(ctx context.Context, reqs []venue.CancelReq) ([]venue.Ack, error) {
	s.sleep(ctx)

	out := make([]venue.Ack, 0, len(reqs))
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++

	if err := s.rateLocked(); err != nil {
		return nil, err
	}
	for _, req := range reqs {
		o, ok := s.orders[req.ClientID]
		if !ok {
			out = append(out, venue.Ack{ClientID: req.ClientID, State: venue.Unknown})
			continue
		}
		if !o.State.Terminal() {
			o.State = venue.Canceled
			o.UpdatedAt = time.Now()
		}
		out = append(out, ackOf(o))
	}
	return out, nil
}

func (s *Sim) Order(ctx context.Context, clientID string) (venue.Order, error) {
	s.sleep(ctx)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++

	o, ok := s.orders[clientID]
	if !ok {
		return venue.Order{}, venue.ErrUnknownOrder
	}
	return *o, nil
}

// OpenOrders 返回账本上仍可成交的订单。
func (s *Sim) OpenOrders(ctx context.Context, symbol string) ([]venue.Order, error) {
	s.sleep(ctx)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++

	var out []venue.Order
	for _, o := range s.orders {
		if symbol != "" && o.Symbol != symbol {
			continue
		}
		if o.State.Open() {
			out = append(out, *o)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ClientID < out[j].ClientID })
	return out, nil
}

// Cross 模拟外部主动单吃掉账本上的挂单，返回实际成交量。
func (s *Sim) Cross(symbol string, side venue.Side, price, qty int64) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	resting := s.restingLocked(symbol, opposite(side))
	filled := int64(0)
	for _, o := range resting {
		if qty <= 0 {
			break
		}
		if side == venue.Buy && o.Price > price {
			continue
		}
		if side == venue.Sell && o.Price < price {
			continue
		}
		take := o.Remaining()
		if take > qty {
			take = qty
		}
		o.Filled += take
		if o.Filled >= o.Qty {
			o.State = venue.Filled
		} else {
			o.State = venue.PartiallyFilled
		}
		o.UpdatedAt = time.Now()
		qty -= take
		filled += take
	}
	return filled
}

// Resting 返回账本上仍可成交的订单数量。
func (s *Sim) Resting(symbol string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.restingLocked(symbol, venue.Side(0)))
}

func (s *Sim) restingLocked(symbol string, side venue.Side) []*venue.Order {
	var out []*venue.Order
	for _, o := range s.orders {
		if symbol != "" && o.Symbol != symbol {
			continue
		}
		if side != 0 && o.Side != side {
			continue
		}
		if o.State.Open() {
			out = append(out, o)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Price != out[j].Price {
			if out[i].Side == venue.Buy {
				return out[i].Price > out[j].Price
			}
			return out[i].Price < out[j].Price
		}
		return out[i].UpdatedAt.Before(out[j].UpdatedAt)
	})
	return out
}

func (s *Sim) placeLocked(req venue.SubmitReq) *venue.Order {
	s.seq++
	o := &venue.Order{
		ClientID:  req.ClientID,
		VenueID:   fmt.Sprintf("V%06d", s.seq),
		Symbol:    req.Symbol,
		Side:      req.Side,
		Price:     req.Price,
		Qty:       req.Qty,
		PostOnly:  req.PostOnly,
		State:     venue.Open,
		UpdatedAt: time.Now(),
	}
	if !req.PostOnly {
		s.matchLocked(o)
	}
	s.orders[o.ClientID] = o
	return o
}

// matchLocked 让主动单吃对手盘，价格优先、时间优先。
func (s *Sim) matchLocked(o *venue.Order) {
	for _, r := range s.restingLocked(o.Symbol, opposite(o.Side)) {
		if o.Remaining() <= 0 {
			break
		}
		if o.Side == venue.Buy && r.Price > o.Price {
			continue
		}
		if o.Side == venue.Sell && r.Price < o.Price {
			continue
		}
		take := r.Remaining()
		if take > o.Remaining() {
			take = o.Remaining()
		}
		r.Filled += take
		if r.Filled >= r.Qty {
			r.State = venue.Filled
		} else {
			r.State = venue.PartiallyFilled
		}
		r.UpdatedAt = time.Now()

		o.Filled += take
	}
	if o.Filled >= o.Qty {
		o.State = venue.Filled
	} else if o.Filled > 0 {
		o.State = venue.PartiallyFilled
	}
}

func (s *Sim) wouldCrossLocked(symbol string, side venue.Side, price int64) bool {
	resting := s.restingLocked(symbol, opposite(side))
	if len(resting) == 0 {
		return false
	}
	best := resting[0].Price
	if side == venue.Buy {
		return price >= best
	}
	return price <= best
}

func (s *Sim) rateLocked() error {
	if s.cfg.LimitPerSec <= 0 {
		return nil
	}
	now := time.Now()
	cut := now.Add(-time.Second)

	keep := s.hits[:0]
	for _, t := range s.hits {
		if t.After(cut) {
			keep = append(keep, t)
		}
	}
	s.hits = keep

	if len(s.hits) >= s.cfg.LimitPerSec {
		return venue.ErrRateLimited
	}
	s.hits = append(s.hits, now)
	return nil
}

func (s *Sim) sleep(ctx context.Context) {
	d := s.cfg.Latency
	if s.cfg.Jitter > 0 {
		s.mu.Lock()
		j := time.Duration(s.rnd.Int63n(int64(s.cfg.Jitter)))
		s.mu.Unlock()
		d += j
	}
	if d <= 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

func ackOf(o *venue.Order) venue.Ack {
	return venue.Ack{
		ClientID: o.ClientID,
		VenueID:  o.VenueID,
		State:    o.State,
		Filled:   o.Filled,
	}
}

func opposite(s venue.Side) venue.Side {
	if s == venue.Buy {
		return venue.Sell
	}
	return venue.Buy
}
