package sim

import (
	"context"
	"testing"
	"time"

	"github.com/peter123023/orderfence/internal/venue"
)

func newTestSim() *Sim {
	return New(Config{Latency: time.Microsecond})
}

// 外部主动单吃掉挂单后，场所里的状态应当从 open 变成部分成交或全部成交。
func TestCrossPartiallyFills(t *testing.T) {
	ctx := context.Background()
	s := newTestSim()

	for _, cid := range []string{"a", "b"} {
		if _, err := s.Submit(ctx, venue.SubmitReq{
			ClientID: cid, Symbol: "X", Side: venue.Buy, Price: 40, Qty: 10, PostOnly: true,
		}); err != nil {
			t.Fatal(err)
		}
	}

	if got := s.Cross("X", venue.Sell, 40, 15); got != 15 {
		t.Fatalf("成交数量应为 15，实际 %d", got)
	}

	first, err := s.Order(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	if first.State != venue.Filled {
		t.Fatalf("先挂的单应全部成交，实际 %s", first.State)
	}

	second, err := s.Order(ctx, "b")
	if err != nil {
		t.Fatal(err)
	}
	if second.State != venue.PartiallyFilled || second.Filled != 5 {
		t.Fatalf("后挂的单应剩 5 手，实际 %s / %d", second.State, second.Filled)
	}
	if got := s.Resting("X"); got != 1 {
		t.Fatalf("账本应只剩 1 张未成交完的单，实际 %d", got)
	}
}
