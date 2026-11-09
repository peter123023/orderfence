// Package scenarios 把执行层容易出问题的几类情形固化成可重复运行的场景。
package scenarios

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/peter123023/orderfence/internal/kernel"
	"github.com/peter123023/orderfence/internal/report"
	"github.com/peter123023/orderfence/internal/sim"
	"github.com/peter123023/orderfence/internal/venue"
)

const symbol = "TEST-PAIR"

// Scenario 是一个可运行场景。
type Scenario struct {
	Name string
	Desc string
	Run  func(ctx context.Context) (report.Section, error)
}

func All() []Scenario {
	return []Scenario{
		{"burst", "并发批量挂撤：延迟分布与残留检查", runBurst},
		{"concurrency", "并发度对批量挂单总耗时的影响", runConcurrency},
		{"ratelimit", "限流下朴素重试与自适应限速的差别", runRateLimit},
		{"ackloss", "回执丢失：结果未知如何收敛", runAckLoss},
		{"postonly", "post-only 越价被拒与合规挂单的对比", runPostOnly},
	}
}

func Find(name string) (Scenario, bool) {
	for _, s := range All() {
		if s.Name == name {
			return s, true
		}
	}
	return Scenario{}, false
}

func books() *sim.Sim {
	return sim.New(sim.Config{Name: "sim-burst"})
}

func runBurst(ctx context.Context) (report.Section, error) {
	const n = 64

	s := books()
	e := kernel.NewExecutor(s, 400)
	e.Parallel = n

	reqs := make([]venue.SubmitReq, n)
	for i := range reqs {
		reqs[i] = venue.SubmitReq{
			ClientID: kernel.NewClientID("burst"),
			Symbol:   symbol,
			Side:     venue.Buy,
			Price:    int64(40 + i%7),
			Qty:      5,
			PostOnly: true,
		}
	}

	start := time.Now()
	res := e.SubmitAll(ctx, reqs)
	submitWall := time.Since(start)

	ids := make([]string, 0, n)
	filled := 0
	rejected := 0
	failed := 0
	for _, r := range res {
		ids = append(ids, r.ClientID)
		switch {
		case r.State == venue.Filled || r.State == venue.PartiallyFilled:
			filled++
		case r.State == venue.Rejected:
			rejected++
		case r.Err != nil:
			failed++
		}
	}

	start = time.Now()
	cres := e.CancelAll(ctx, ids)
	cancelWall := time.Since(start)

	cancelFailed := 0
	for _, r := range cres {
		if r.Err != nil {
			cancelFailed++
		}
	}

	recon := &kernel.Reconciler{V: s, E: e, Journal: e.Journal}
	st, err := recon.Run(ctx, symbol)
	if err != nil {
		return report.Section{}, err
	}

	sub := e.Probe.Stats("submit")
	can := e.Probe.Stats("cancel")

	t := &report.Table{
		Headers: []string{"操作", "样本", "p50", "p90", "p99", "最大", "总耗时"},
		Rows: [][]string{
			{"挂单", fmt.Sprint(sub.N), report.Dur(sub.P50), report.Dur(sub.P90), report.Dur(sub.P99), report.Dur(sub.Max), report.Dur(submitWall)},
			{"撤单", fmt.Sprint(can.N), report.Dur(can.P50), report.Dur(can.P90), report.Dur(can.P99), report.Dur(can.Max), report.Dur(cancelWall)},
		},
	}

	resting := s.Resting(symbol)

	return report.Section{
		Title: "并发批量挂撤",
		Body:  fmt.Sprintf("%d 张 post-only 报价一次性并发挂出，再整体撤销，最后做一次全量对账。", n),
		Table: t,
		Notes: []string{
			report.Check(filled == 0, fmt.Sprintf("成交张数为 0（实际 %d）", filled)),
			report.Check(rejected == 0, fmt.Sprintf("无被拒订单（实际 %d）", rejected)),
			report.Check(failed == 0, fmt.Sprintf("无失败请求（实际 %d）", failed)),
			report.Check(cancelFailed == 0, fmt.Sprintf("撤单全部确认（失败 %d）", cancelFailed)),
			report.Check(resting == 0, fmt.Sprintf("账本无残留挂单（实际 %d）", resting)),
			report.Check(st.Orphans == 0, fmt.Sprintf("对账未发现孤儿单（实际 %d）", st.Orphans)),
		},
	}, nil
}

func runConcurrency(ctx context.Context) (report.Section, error) {
	const n = 64
	levels := []int{1, 2, 4, 8, 16, 32, 64}

	t := &report.Table{Headers: []string{"并发度", "总耗时", "单均耗时", "p99", "失败"}}

	for _, p := range levels {
		s := books()
		e := kernel.NewExecutor(s, 10000)
		e.Parallel = p

		reqs := make([]venue.SubmitReq, n)
		for i := range reqs {
			reqs[i] = venue.SubmitReq{
				ClientID: kernel.NewClientID("cnc"),
				Symbol:   symbol,
				Side:     venue.Buy,
				Price:    int64(30 + i%9),
				Qty:      1,
				PostOnly: true,
			}
		}

		start := time.Now()
		res := e.SubmitAll(ctx, reqs)
		wall := time.Since(start)

		failed := 0
		for _, r := range res {
			if r.Err != nil {
				failed++
			}
		}
		st := e.Probe.Stats("submit")
		t.Rows = append(t.Rows, []string{
			fmt.Sprint(p), report.Dur(wall),
			report.Dur(wall / time.Duration(n)),
			report.Dur(st.P99),
			fmt.Sprint(failed),
		})
	}

	return report.Section{
		Title: "并发度扫描",
		Body:  fmt.Sprintf("同样 %d 张单，只改客户端并发度。单次往返时延固定，差别全部来自是否串行等待。", n),
		Table: t,
		Notes: []string{
			"总耗时基本等于 ⌈N/并发度⌉ 乘以单次往返。并发度越高越接近一次往返，但场所侧的连接数与限流会先成为瓶颈。",
		},
	}, nil
}

func runRateLimit(ctx context.Context) (report.Section, error) {
	const n = 120
	const limit = 40

	build := func(initialRate float64, attempts int) (*sim.Sim, *kernel.Executor, []venue.SubmitReq) {
		s := sim.New(sim.Config{Name: "sim-limited", Latency: 500 * time.Microsecond, LimitPerSec: limit})
		e := kernel.NewExecutor(s, initialRate)
		e.Parallel = 24
		e.Attempts = attempts
		e.Backoff = time.Millisecond

		reqs := make([]venue.SubmitReq, n)
		for i := range reqs {
			reqs[i] = venue.SubmitReq{
				ClientID: kernel.NewClientID("rl"),
				Symbol:   symbol,
				Side:     venue.Buy,
				Price:    int64(20 + i%10),
				Qty:      1,
				PostOnly: true,
			}
		}
		return s, e, reqs
	}

	t := &report.Table{Headers: []string{"客户端策略", "成功", "被限流失败", "限流次数", "总耗时", "当前速率"}}

	s1, e1, r1 := build(4000, 1)
	start := time.Now()
	res1 := e1.SubmitAll(ctx, r1)
	wall1 := time.Since(start)
	ok1, rl1 := count(res1, venue.ErrRateLimited)
	t.Rows = append(t.Rows, []string{
		"不限速、不重试", fmt.Sprint(ok1), fmt.Sprint(rl1), "0",
		report.Dur(wall1), "—",
	})

	s2, e2, r2 := build(400, 12)
	start = time.Now()
	res2 := e2.SubmitAll(ctx, r2)
	wall2 := time.Since(start)
	ok2, rl2 := count(res2, venue.ErrRateLimited)
	t.Rows = append(t.Rows, []string{
		"自适应限速 + 退避重试", fmt.Sprint(ok2), fmt.Sprint(rl2), fmt.Sprint(e2.Submit.Penalties()),
		report.Dur(wall2), fmt.Sprintf("%.0f/s", e2.Submit.Rate()),
	})

	return report.Section{
		Title: "限流与自适应速率",
		Body:  fmt.Sprintf("场所硬限 %d 请求/秒，客户端一次性打 %d 张单。两组唯一的差别是客户端有没有把 429 当作反馈信号。", limit, n),
		Table: t,
		Notes: []string{
			report.Check(ok2 == n, fmt.Sprintf("自适应客户端 %d/%d 全部落地", ok2, n)),
			"不限速那组并没有更快，只是把失败转移给了上游：真实业务里 429 之后还得有人补单。",
			fmt.Sprintf("自适应组最终速率 %.0f/s，贴着场所上限来回震荡，说明 AIMD 找到了实际可达的速率。", e2.Submit.Rate()),
			fmt.Sprintf("两组账本挂单数分别为 %d 与 %d。", s1.Resting(symbol), s2.Resting(symbol)),
		},
	}, nil
}

func runAckLoss(ctx context.Context) (report.Section, error) {
	const n = 48

	s := sim.New(sim.Config{Name: "sim-lossy", Latency: time.Millisecond, TimeoutRate: 0.3, Seed: 7})
	e := kernel.NewExecutor(s, 200)
	e.Parallel = 16

	reqs := make([]venue.SubmitReq, n)
	for i := range reqs {
		reqs[i] = venue.SubmitReq{
			ClientID: kernel.NewClientID("loss"),
			Symbol:   symbol,
			Side:     venue.Buy,
			Price:    int64(50 + i%5),
			Qty:      2,
			PostOnly: true,
		}
	}

	res := e.SubmitAll(ctx, reqs)
	unknown := 0
	ids := make([]string, 0, n)
	var unknownIDs []string
	for _, r := range res {
		ids = append(ids, r.ClientID)
		if r.Unknown {
			unknown++
			unknownIDs = append(unknownIDs, r.ClientID)
		}
	}

	placed := s.Resting(symbol)

	fixed := e.Resolve(ctx, unknownIDs)
	afterResolve := s.Resting(symbol)

	recon := &kernel.Reconciler{V: s, E: e, Journal: e.Journal}
	st, err := recon.Run(ctx, symbol)
	if err != nil {
		return report.Section{}, err
	}

	afterRecon := s.Resting(symbol)

	cres := e.CancelAll(ctx, ids)
	cancelFailed, cancelUnknown := 0, 0
	for _, r := range cres {
		switch {
		case r.Unknown:
			cancelUnknown++
		case r.Err != nil:
			cancelFailed++
		}
	}
	left := s.Resting(symbol)

	st2, err := recon.Run(ctx, symbol)
	if err != nil {
		return report.Section{}, err
	}
	leftAfter := s.Resting(symbol)

	t := &report.Table{
		Headers: []string{"阶段", "场所挂单", "未知/差异", "说明"},
		Rows: [][]string{
			{"提交后", fmt.Sprint(placed), fmt.Sprint(unknown), "回执丢了的单其实已经挂在场所上"},
			{"逐笔查询后", fmt.Sprint(afterResolve), fmt.Sprint(fixed), "拿 ClientID 查一次就够了，不用重发"},
			{"全量对账后", fmt.Sprint(afterRecon), fmt.Sprint(st.Mismatched), "兜底扫描，此时应当没有差异"},
			{"撤销后", fmt.Sprint(left), fmt.Sprint(cancelUnknown), "撤单回执同样可能丢"},
			{"再次对账", fmt.Sprint(leftAfter), fmt.Sprint(st2.Mismatched), "未知的撤单结果被收敛为终态"},
		},
	}

	return report.Section{
		Title: "回执丢失与结果收敛",
		Body:  fmt.Sprintf("场所每三次请求丢一次回执，%d 张单。关键约束是：收到超时后绝不能重发，否则账本上会出现重复订单。", n),
		Table: t,
		Notes: []string{
			report.Check(placed == n, fmt.Sprintf("场所实际挂单数等于请求数 %d（无重复下单）", n)),
			report.Check(unknown > 0, fmt.Sprintf("确实触发了结果未知分支（%d 次）", unknown)),
			report.Check(fixed == unknown, fmt.Sprintf("逐笔查询修正了全部未知（%d/%d）", fixed, unknown)),
			report.Check(countKind(e.Journal.Events(), "submit_unknown") == unknown, "日志里每次未知都有对应记录"),
			report.Check(st.Mismatched == 0, fmt.Sprintf("查询之后对账不再发现差异（%d）", st.Mismatched)),
			report.Check(cancelUnknown > 0, fmt.Sprintf("撤单也出现了结果未知（%d 次）", cancelUnknown)),
			report.Check(left == 0 && leftAfter == 0, fmt.Sprintf("最终账本清空（残留 %d / %d）", left, leftAfter)),
			report.Check(cancelFailed == 0, fmt.Sprintf("撤单无确定性失败（%d）", cancelFailed)),
		},
	}, nil
}

func runPostOnly(ctx context.Context) (report.Section, error) {
	const crossed = 20
	const safe = 20

	s := books()
	e := kernel.NewExecutor(s, 400)
	e.Parallel = 16

	// 先挂一张卖单，制造对手盘。
	makerPrice := int64(60)
	seed := kernel.NewClientID("seed")
	if _, err := s.Submit(ctx, venue.SubmitReq{
		ClientID: seed, Symbol: symbol, Side: venue.Sell, Price: makerPrice, Qty: 1000,
	}); err != nil {
		return report.Section{}, err
	}

	var reqs []venue.SubmitReq
	for i := 0; i < crossed; i++ {
		reqs = append(reqs, venue.SubmitReq{
			ClientID: kernel.NewClientID("cross"),
			Symbol:   symbol, Side: venue.Buy,
			Price: makerPrice, Qty: 1, PostOnly: true,
		})
	}
	for i := 0; i < safe; i++ {
		reqs = append(reqs, venue.SubmitReq{
			ClientID: kernel.NewClientID("safe"),
			Symbol:   symbol, Side: venue.Buy,
			Price: makerPrice - 2, Qty: 1, PostOnly: true,
		})
	}

	res := e.SubmitAll(ctx, reqs)
	rejected, open, filled := 0, 0, 0
	for _, r := range res {
		switch r.State {
		case venue.Rejected:
			rejected++
		case venue.Open:
			open++
		case venue.Filled, venue.PartiallyFilled:
			filled++
		}
	}

	t := &report.Table{
		Headers: []string{"分组", "数量", "被拒", "挂上", "成交"},
		Rows: [][]string{
			{"价格等于卖一", fmt.Sprint(crossed), fmt.Sprint(rejected), "0", "0"},
			{"价格低于卖一", fmt.Sprint(safe), "0", fmt.Sprint(open), fmt.Sprint(filled)},
		},
	}

	return report.Section{
		Title: "post-only 的两种去向",
		Body:  fmt.Sprintf("同一批买单分两组：一组报价等于卖一（会立即成交），一组低两个价位。账本里另有 1 张 %d 的卖单作为对手盘。", makerPrice),
		Table: t,
		Notes: []string{
			report.Check(rejected == crossed, fmt.Sprintf("越价组全部被拒（%d/%d）", rejected, crossed)),
			report.Check(filled == 0, "越价组一张都没成交，说明场所是拒单而不是帮我们吃单"),
			report.Check(open == safe, fmt.Sprintf("合规组全部挂上（%d/%d）", open, safe)),
		},
	}, nil
}

func count(res []kernel.Result, target error) (ok int, hit int) {
	for _, r := range res {
		switch {
		case r.Err == nil:
			ok++
		case errors.Is(r.Err, target):
			hit++
		}
	}
	return ok, hit
}

func countKind(ev []kernel.Event, kind string) int {
	n := 0
	for _, e := range ev {
		if e.Kind == kind {
			n++
		}
	}
	return n
}
