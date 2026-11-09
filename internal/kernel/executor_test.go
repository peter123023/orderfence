package kernel

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/peter123023/orderfence/internal/sim"
	"github.com/peter123023/orderfence/internal/venue"
)

func newTestExec(s venue.Venue, rate float64) *Executor {
	e := NewExecutor(s, rate)
	e.Parallel = 16
	e.Backoff = time.Millisecond
	return e
}

func reqs(n int) []venue.SubmitReq {
	out := make([]venue.SubmitReq, n)
	for i := range out {
		out[i] = venue.SubmitReq{
			ClientID: NewClientID("t"),
			Symbol:   "X",
			Side:     venue.Buy,
			Price:    40,
			Qty:      1,
			PostOnly: true,
		}
	}
	return out
}

// 回执丢失时订单已生效，内核必须标未知且不重发。
func TestTimeoutDoesNotResend(t *testing.T) {
	s := sim.New(sim.Config{Latency: time.Microsecond, TimeoutRate: 1.0, Seed: 1})
	e := newTestExec(s, 1000)

	res := e.SubmitAll(context.Background(), reqs(10))

	unknown := 0
	for _, r := range res {
		if r.Unknown {
			unknown++
		}
	}
	if unknown != 10 {
		t.Fatalf("期望 10 次结果未知，实际 %d", unknown)
	}
	if got := s.Resting("X"); got != 10 {
		t.Fatalf("场所应有 10 张单，实际 %d（重发会导致翻倍）", got)
	}
}

// 对账要发现场所挂着、本地不知道的孤儿单并撤掉。
func TestReconcileCancelsOrphans(t *testing.T) {
	ctx := context.Background()
	s := sim.New(sim.Config{Latency: time.Microsecond})

	// 绕过执行内核直接下单，模拟进程重启后丢失内存状态。
	for _, cid := range []string{"lost-1", "lost-2", "lost-3"} {
		if _, err := s.Submit(ctx, venue.SubmitReq{
			ClientID: cid, Symbol: "X", Side: venue.Buy, Price: 40, Qty: 1, PostOnly: true,
		}); err != nil {
			t.Fatal(err)
		}
	}

	j := NewJournal()
	r := &Reconciler{V: s, Journal: j}
	st, err := r.Run(ctx, "X")
	if err != nil {
		t.Fatal(err)
	}
	if st.Orphans != 3 {
		t.Fatalf("期望发现 3 张孤儿单，实际 %d", st.Orphans)
	}
	if got := s.Resting("X"); got != 0 {
		t.Fatalf("孤儿单应被撤掉，实际仍挂着 %d 张", got)
	}
}

// 终态订单重复撤销不应再打到场所。
func TestCancelTerminalIsNoop(t *testing.T) {
	ctx := context.Background()
	s := sim.New(sim.Config{Latency: time.Microsecond})
	e := newTestExec(s, 100)

	r := e.SubmitAll(ctx, reqs(1))[0]
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	before := s.Calls()

	res := e.CancelAll(ctx, []string{r.ClientID})[0]
	if res.Err != nil {
		t.Fatalf("首次撤单失败: %v", res.Err)
	}
	after := s.Calls()

	res = e.CancelAll(ctx, []string{r.ClientID})[0]
	if !res.Deduped {
		t.Fatal("重复撤单应被识别为已完成")
	}
	if s.Calls() != after {
		t.Fatalf("重复撤单不该再打到场所，多发了 %d 次", s.Calls()-after)
	}
	if after == before {
		t.Fatal("首次撤单应当真实发出请求")
	}
}

// 限流时应重试而不是直接失败。
func TestRetriesOnRateLimit(t *testing.T) {
	s := sim.New(sim.Config{Latency: time.Microsecond, LimitPerSec: 1})
	e := newTestExec(s, 50)
	e.Attempts = 20

	res := e.SubmitAll(context.Background(), reqs(3))
	failed := 0
	for _, r := range res {
		if errors.Is(r.Err, venue.ErrRateLimited) {
			failed++
		}
	}
	if failed != 0 {
		t.Fatalf("重试之后不应再有被限流失败，实际 %d", failed)
	}
	if got := s.Resting("X"); got != 3 {
		t.Fatalf("期望 3 张单落地，实际 %d", got)
	}
	if e.Submit.Penalties() == 0 {
		t.Fatal("应当收到过限流反馈")
	}
}
