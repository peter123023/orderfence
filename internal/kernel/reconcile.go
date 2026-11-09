package kernel

import (
	"context"
	"errors"

	"github.com/peter123023/orderfence/internal/venue"
)

// ReconcileStats 是一次对账的结果。
type ReconcileStats struct {
	LocalOpen  int
	VenueOpen  int
	Orphans    int // 场所挂着、本地不知道
	Ghosts     int // 本地以为挂着、场所已无
	Mismatched int // 成交数量不一致
}

// Reconciler 把本地视图和场所账本对齐。进程重启后只信场所。
type Reconciler struct {
	V       venue.Venue
	E       *Executor
	Journal *Journal
}

func (r *Reconciler) Run(ctx context.Context, symbol string) (ReconcileStats, error) {
	var st ReconcileStats

	local := r.Journal.LocalOpen()
	remote, err := r.V.OpenOrders(ctx, symbol)
	if err != nil {
		return st, err
	}
	st.LocalOpen = len(local)
	st.VenueOpen = len(remote)

	remoteByClient := make(map[string]venue.Order, len(remote))
	for _, o := range remote {
		remoteByClient[o.ClientID] = o
	}

	for id, lo := range local {
		ro, ok := remoteByClient[id]
		if !ok {
			cur, qerr := r.V.Order(ctx, id)
			switch {
			case errors.Is(qerr, venue.ErrUnknownOrder):
				r.Journal.Append(Event{Kind: "reconcile_ghost", ClientID: id, State: venue.Canceled})
				st.Ghosts++
			case qerr == nil:
				r.Journal.Append(Event{Kind: "reconcile_ghost", ClientID: id, State: cur.State, Filled: cur.Filled})
				if cur.Filled != lo.Filled {
					st.Mismatched++
				}
				st.Ghosts++
			}
			continue
		}
		if ro.State != lo.State || ro.Filled != lo.Filled {
			r.Journal.Append(Event{Kind: "reconcile_sync", ClientID: id, State: ro.State, Filled: ro.Filled})
			st.Mismatched++
		}
	}

	for id, ro := range remoteByClient {
		if _, ok := local[id]; ok {
			continue
		}
		_, cerr := r.V.Cancel(ctx, venue.CancelReq{ClientID: id, VenueID: ro.VenueID})
		if cerr == nil || errors.Is(cerr, venue.ErrUnknownOrder) {
			r.Journal.Append(Event{Kind: "reconcile_orphan", ClientID: id, State: venue.Canceled})
			st.Orphans++
		}
	}

	return st, nil
}
