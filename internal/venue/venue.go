// Package venue 定义订单网关的最小契约：下单、撤单、查询，以及客户端视角的订单模型。
package venue

import (
	"context"
	"errors"
	"time"
)

type Side uint8

const (
	Buy Side = iota + 1
	Sell
)

func (s Side) String() string {
	switch s {
	case Buy:
		return "buy"
	case Sell:
		return "sell"
	default:
		return "unknown"
	}
}

// State 只由场所回报驱动，客户端不自己发明状态。
type State uint8

const (
	Unknown State = iota
	PendingNew
	PendingCancel
	Open
	PartiallyFilled
	Filled
	Canceled
	Rejected
)

func (s State) String() string {
	switch s {
	case PendingNew:
		return "pending_new"
	case PendingCancel:
		return "pending_cancel"
	case Open:
		return "open"
	case PartiallyFilled:
		return "partially_filled"
	case Filled:
		return "filled"
	case Canceled:
		return "canceled"
	case Rejected:
		return "rejected"
	default:
		return "unknown"
	}
}

func (s State) Terminal() bool {
	return s == Filled || s == Canceled || s == Rejected
}

func (s State) Open() bool {
	return s == Open || s == PartiallyFilled
}

// Order 是订单在本地的一份快照。Price 以最小价位为单位，避免浮点误差。
type Order struct {
	ClientID  string
	VenueID   string
	Symbol    string
	Side      Side
	Price     int64
	Qty       int64
	Filled    int64
	PostOnly  bool
	State     State
	UpdatedAt time.Time
}

func (o Order) Remaining() int64 {
	if o.Qty <= o.Filled {
		return 0
	}
	return o.Qty - o.Filled
}

// SubmitReq 是一次下单请求。ClientID 由客户端生成，重试时必须复用同一个值。
type SubmitReq struct {
	ClientID string
	Symbol   string
	Side     Side
	Price    int64
	Qty      int64
	PostOnly bool
}

type CancelReq struct {
	ClientID string
	VenueID  string
}

type Ack struct {
	ClientID string
	VenueID  string
	State    State
	Filled   int64
}

// 错误分两类：可判定失败说明请求一定没生效，可以重试；ErrTimeout 属于
// 结果未知，只能查询或对账收敛，绝不能盲目重发。
var (
	ErrRateLimited  = errors.New("venue: rate limited")
	ErrWouldCross   = errors.New("venue: post-only order would cross")
	ErrDuplicate    = errors.New("venue: duplicate client id")
	ErrUnknownOrder = errors.New("venue: unknown order")
	ErrTimeout      = errors.New("venue: request outcome unknown")
)

type Venue interface {
	Name() string
	Submit(ctx context.Context, req SubmitReq) (Ack, error)
	SubmitBatch(ctx context.Context, reqs []SubmitReq) ([]Ack, error)
	Cancel(ctx context.Context, req CancelReq) (Ack, error)
	CancelBatch(ctx context.Context, reqs []CancelReq) ([]Ack, error)
	Order(ctx context.Context, clientID string) (Order, error)
	OpenOrders(ctx context.Context, symbol string) ([]Order, error)
}
