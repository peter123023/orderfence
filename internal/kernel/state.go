package kernel

import (
	"fmt"

	"github.com/peter123023/orderfence/internal/venue"
)

// allowed 是合法的状态迁移。乱序或重复的回报会在这里被挡住。
var allowed = map[venue.State][]venue.State{
	venue.Unknown:         {venue.PendingNew},
	venue.PendingNew:      {venue.Open, venue.PartiallyFilled, venue.Filled, venue.Rejected, venue.Canceled},
	venue.Open:            {venue.PartiallyFilled, venue.Filled, venue.PendingCancel, venue.Canceled},
	venue.PartiallyFilled: {venue.PartiallyFilled, venue.Filled, venue.PendingCancel, venue.Canceled},
	venue.PendingCancel:   {venue.Canceled, venue.Open, venue.PartiallyFilled, venue.Filled},
	venue.Filled:          {},
	venue.Canceled:        {},
	venue.Rejected:        {},
}

func CanTransition(from, to venue.State) bool {
	if from == to {
		return from == venue.PartiallyFilled
	}
	for _, s := range allowed[from] {
		if s == to {
			return true
		}
	}
	return false
}

// Apply 把回执落到订单上。返回错误时订单已被改动，调用方应记下漂移，交给对账修。
func Apply(o *venue.Order, a venue.Ack) error {
	if a.ClientID != "" && o.ClientID != "" && a.ClientID != o.ClientID {
		return fmt.Errorf("ack 属于 %s，却应用到了 %s", a.ClientID, o.ClientID)
	}
	if !CanTransition(o.State, a.State) {
		return fmt.Errorf("非法状态迁移 %s -> %s (order %s)", o.State, a.State, o.ClientID)
	}
	o.State = a.State
	if a.VenueID != "" {
		o.VenueID = a.VenueID
	}
	if a.Filled > o.Filled {
		o.Filled = a.Filled
	}
	return nil
}
