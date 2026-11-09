package kernel

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/peter123023/orderfence/internal/venue"
)

// Result 是一次下单或撤单的结果。
type Result struct {
	ClientID string
	State    venue.State
	Err      error
	Deduped  bool
	Unknown  bool
	Attempts int
	Wire     time.Duration
}

// Executor 并发地把请求送到场所，负责限速、幂等和结果收敛。
type Executor struct {
	V        venue.Venue
	Submit   *Bucket
	Cancel   *Bucket
	Idem     *Idem
	Probe    *Probe
	Journal  *Journal
	Parallel int
	Attempts int
	Backoff  time.Duration
}

// NewExecutor rate 是每秒请求数的初始估计值。
func NewExecutor(v venue.Venue, rate float64) *Executor {
	if rate <= 0 {
		rate = 50
	}
	return &Executor{
		V:        v,
		Submit:   NewBucket(rate, rate/4+1),
		Cancel:   NewBucket(rate, rate/4+1),
		Idem:     NewIdem(),
		Probe:    NewProbe(),
		Journal:  NewJournal(),
		Parallel: 16,
		Attempts: 4,
		Backoff:  2 * time.Millisecond,
	}
}

// SubmitAll 并发提交一批订单，返回顺序与入参一致。
func (e *Executor) SubmitAll(ctx context.Context, reqs []venue.SubmitReq) []Result {
	out := make([]Result, len(reqs))
	e.fanout(ctx, len(reqs), func(i int) {
		out[i] = e.submitOne(ctx, reqs[i])
	})
	return out
}

// CancelAll 并发撤销一批订单。
func (e *Executor) CancelAll(ctx context.Context, ids []string) []Result {
	out := make([]Result, len(ids))
	e.fanout(ctx, len(ids), func(i int) {
		out[i] = e.cancelOne(ctx, ids[i])
	})
	return out
}

func (e *Executor) fanout(ctx context.Context, n int, fn func(int)) {
	parallel := e.Parallel
	if parallel <= 0 {
		parallel = 1
	}
	sem := make(chan struct{}, parallel)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				return
			}
			fn(i)
		}(i)
	}
	wg.Wait()
}

func (e *Executor) submitOne(ctx context.Context, req venue.SubmitReq) Result {
	start := time.Now()

	if o, ok := e.Idem.Get(req.ClientID); ok {
		return Result{ClientID: req.ClientID, State: o.State, Deduped: true}
	}
	if !e.Idem.Claim(req.ClientID) {
		return Result{ClientID: req.ClientID, State: venue.PendingNew, Deduped: true}
	}

	base := venue.Order{
		ClientID: req.ClientID,
		Symbol:   req.Symbol,
		Side:     req.Side,
		Price:    req.Price,
		Qty:      req.Qty,
		PostOnly: req.PostOnly,
		State:    venue.PendingNew,
	}
	e.Journal.Track(base)

	attempts := 0
	for {
		attempts++
		if err := e.Submit.Wait(ctx); err != nil {
			e.Idem.Release(req.ClientID)
			return Result{ClientID: req.ClientID, Err: err, Attempts: attempts}
		}

		sent := time.Now()
		ack, err := e.V.Submit(ctx, req)
		ackAt := time.Now()
		e.Probe.Record(Sample{
			Op: "submit", ClientID: req.ClientID,
			Start: start, Sent: sent, Ack: ackAt, Err: err,
		})

		switch {
		case err == nil:
			o := base
			if aerr := Apply(&o, ack); aerr != nil {
				e.Journal.Append(Event{Kind: "state_drift", ClientID: req.ClientID, State: ack.State, Err: aerr.Error()})
				o.State = ack.State
			}
			o.UpdatedAt = ackAt
			e.Idem.Put(o)
			e.Journal.Append(Event{Kind: "submit_ack", ClientID: o.ClientID, State: o.State, Filled: o.Filled})
			e.Submit.Reward()
			return Result{ClientID: o.ClientID, State: o.State, Attempts: attempts, Wire: ackAt.Sub(sent)}

		case errors.Is(err, venue.ErrWouldCross):
			o := base
			o.State = venue.Rejected
			o.UpdatedAt = ackAt
			e.Idem.Put(o)
			e.Journal.Append(Event{Kind: "submit_reject", ClientID: o.ClientID, State: venue.Rejected, Err: err.Error()})
			return Result{ClientID: o.ClientID, State: venue.Rejected, Err: err, Attempts: attempts, Wire: ackAt.Sub(sent)}

		case errors.Is(err, venue.ErrRateLimited):
			e.Submit.Penalize()
			if attempts >= e.Attempts {
				e.Idem.Release(req.ClientID)
				return Result{ClientID: req.ClientID, Err: err, Attempts: attempts, Wire: ackAt.Sub(sent)}
			}
			if !sleepCtx(ctx, e.backoffFor(attempts)) {
				e.Idem.Release(req.ClientID)
				return Result{ClientID: req.ClientID, Err: ctx.Err(), Attempts: attempts}
			}

		case errors.Is(err, venue.ErrDuplicate):
			// 上一笔其实成功了，只是回执丢了，查一次就够。
			if o, qerr := e.V.Order(ctx, req.ClientID); qerr == nil {
				e.Idem.Put(o)
				e.Journal.Append(Event{Kind: "submit_dedup", ClientID: o.ClientID, State: o.State, Filled: o.Filled})
				return Result{ClientID: o.ClientID, State: o.State, Deduped: true, Attempts: attempts}
			}
			return Result{ClientID: req.ClientID, Err: err, Attempts: attempts}

		case errors.Is(err, venue.ErrTimeout):
			// 请求可能已经生效。重发会凭空多一张单，只能标未知，交给对账收敛。
			e.Journal.Append(Event{Kind: "submit_unknown", ClientID: req.ClientID, State: venue.Unknown, Err: err.Error()})
			return Result{ClientID: req.ClientID, State: venue.PendingNew, Err: err, Unknown: true, Attempts: attempts, Wire: ackAt.Sub(sent)}

		default:
			e.Idem.Release(req.ClientID)
			return Result{ClientID: req.ClientID, Err: err, Attempts: attempts, Wire: ackAt.Sub(sent)}
		}
	}
}

func (e *Executor) cancelOne(ctx context.Context, id string) Result {
	start := time.Now()

	cur := e.snapshot(id)
	if cur.State.Terminal() {
		return Result{ClientID: id, State: cur.State, Deduped: true}
	}

	attempts := 0
	for {
		attempts++
		if err := e.Cancel.Wait(ctx); err != nil {
			return Result{ClientID: id, Err: err, Attempts: attempts}
		}

		sent := time.Now()
		ack, err := e.V.Cancel(ctx, venue.CancelReq{ClientID: id, VenueID: cur.VenueID})
		ackAt := time.Now()
		e.Probe.Record(Sample{
			Op: "cancel", ClientID: id,
			Start: start, Sent: sent, Ack: ackAt, Err: err,
		})

		switch {
		case err == nil:
			o := cur
			if ack.State != o.State {
				if aerr := Apply(&o, ack); aerr != nil {
					e.Journal.Append(Event{Kind: "state_drift", ClientID: id, State: ack.State, Err: aerr.Error()})
					o.State = ack.State
				}
			}
			if ack.Filled > o.Filled {
				o.Filled = ack.Filled
			}
			o.UpdatedAt = ackAt
			e.Idem.Put(o)
			e.Journal.Append(Event{Kind: "cancel_ack", ClientID: id, State: o.State, Filled: o.Filled})
			e.Cancel.Reward()
			return Result{ClientID: id, State: o.State, Attempts: attempts, Wire: ackAt.Sub(sent)}

		case errors.Is(err, venue.ErrUnknownOrder):
			// 场所没有这张单，撤单已经是终态。
			o := cur
			o.State = venue.Canceled
			o.UpdatedAt = ackAt
			e.Idem.Put(o)
			e.Journal.Append(Event{Kind: "cancel_absent", ClientID: id, State: venue.Canceled})
			return Result{ClientID: id, State: venue.Canceled, Attempts: attempts, Wire: ackAt.Sub(sent)}

		case errors.Is(err, venue.ErrRateLimited):
			e.Cancel.Penalize()
			if attempts >= e.Attempts {
				return Result{ClientID: id, Err: err, Attempts: attempts}
			}
			if !sleepCtx(ctx, e.backoffFor(attempts)) {
				return Result{ClientID: id, Err: ctx.Err(), Attempts: attempts}
			}

		case errors.Is(err, venue.ErrTimeout):
			e.Journal.Append(Event{Kind: "cancel_unknown", ClientID: id, State: venue.PendingCancel, Err: err.Error()})
			return Result{ClientID: id, State: venue.PendingCancel, Err: err, Unknown: true, Attempts: attempts}

		default:
			return Result{ClientID: id, Err: err, Attempts: attempts, Wire: ackAt.Sub(sent)}
		}
	}
}

// Resolve 对结果未知的订单做一次定点查询。
func (e *Executor) Resolve(ctx context.Context, ids []string) int {
	fixed := 0
	for _, id := range ids {
		o, err := e.V.Order(ctx, id)
		switch {
		case err == nil:
			e.Idem.Put(o)
			e.Journal.Append(Event{Kind: "resolve", ClientID: id, State: o.State, Filled: o.Filled})
			fixed++
		case errors.Is(err, venue.ErrUnknownOrder):
			cur := e.snapshot(id)
			cur.State = venue.Canceled
			e.Idem.Put(cur)
			e.Journal.Append(Event{Kind: "resolve_absent", ClientID: id, State: venue.Canceled})
			fixed++
		}
	}
	return fixed
}

func (e *Executor) snapshot(id string) venue.Order {
	if o, ok := e.Journal.View(id); ok {
		if v, ok2 := e.Idem.Get(id); ok2 && v.State != venue.Unknown {
			o.State = v.State
			o.Filled = v.Filled
			o.VenueID = v.VenueID
		}
		return o
	}
	if o, ok := e.Idem.Get(id); ok {
		return o
	}
	return venue.Order{ClientID: id, State: venue.Unknown}
}

// backoffFor 指数退避，上限 200ms。限流窗口一般是一秒，等得比窗口还久说明该降速。
func (e *Executor) backoffFor(attempts int) time.Duration {
	const cap = 200 * time.Millisecond
	d := e.Backoff
	if d <= 0 {
		d = time.Millisecond
	}
	for i := 1; i < attempts; i++ {
		d *= 2
		if d >= cap {
			return cap
		}
	}
	return d
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
