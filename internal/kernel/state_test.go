package kernel

import (
	"testing"

	"github.com/peter123023/orderfence/internal/venue"
)

func TestStateMachineRejectsIllegalTransitions(t *testing.T) {
	cases := []struct {
		from, to venue.State
		ok       bool
	}{
		{venue.PendingNew, venue.Open, true},
		{venue.PendingNew, venue.Rejected, true},
		{venue.Open, venue.PendingCancel, true},
		{venue.PendingCancel, venue.Canceled, true},
		{venue.PartiallyFilled, venue.PartiallyFilled, true},
		{venue.Filled, venue.Filled, false},
		{venue.Canceled, venue.Open, false},
		{venue.Filled, venue.Canceled, false},
		{venue.Open, venue.Rejected, false},
	}

	for _, c := range cases {
		if got := CanTransition(c.from, c.to); got != c.ok {
			t.Fatalf("%s -> %s: 期望 %v，实际 %v", c.from, c.to, c.ok, got)
		}
	}
}

func TestApplyRejectsOutOfOrderAck(t *testing.T) {
	o := venue.Order{ClientID: "x1", State: venue.Filled, Qty: 10, Filled: 10}
	if err := Apply(&o, venue.Ack{ClientID: "x1", State: venue.Open}); err == nil {
		t.Fatal("已经成交的订单不该被改回挂单中")
	}
	if o.State != venue.Filled {
		t.Fatalf("状态被污染: %s", o.State)
	}
}

func TestApplyKeepsMaxFilled(t *testing.T) {
	o := venue.Order{ClientID: "x2", State: venue.PartiallyFilled, Qty: 10, Filled: 4}
	if err := Apply(&o, venue.Ack{ClientID: "x2", State: venue.PartiallyFilled, Filled: 3}); err != nil {
		t.Fatalf("部分成交的原地更新应当合法: %v", err)
	}
	if o.Filled != 4 {
		t.Fatalf("成交量不应回退，实际 %d", o.Filled)
	}
}
