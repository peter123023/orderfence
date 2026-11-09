package kernel

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/peter123023/orderfence/internal/venue"
)

var seq atomic.Uint64

// NewClientID 生成客户端订单号。前缀用来在日志里区分来源。
func NewClientID(prefix string) string {
	if prefix == "" {
		prefix = "xl"
	}
	return fmt.Sprintf("%s-%d-%05d", prefix, time.Now().UnixNano(), seq.Add(1)%100000)
}

// Idem 记录下过的 ClientID，重试前先查这里。
type Idem struct {
	mu     sync.Mutex
	orders map[string]venue.Order
	claims map[string]bool
}

func NewIdem() *Idem {
	return &Idem{
		orders: make(map[string]venue.Order),
		claims: make(map[string]bool),
	}
}

// Claim 发请求前占位。返回 false 表示同一个 ClientID 正在处理中。
func (i *Idem) Claim(clientID string) bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.claims[clientID] {
		return false
	}
	i.claims[clientID] = true
	return true
}

// Release 归还占位，只在请求确定没有发出时调用。
func (i *Idem) Release(clientID string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	delete(i.claims, clientID)
}

func (i *Idem) Get(clientID string) (venue.Order, bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	o, ok := i.orders[clientID]
	return o, ok
}

// Put 记录订单快照，同时结束占位。
func (i *Idem) Put(o venue.Order) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.orders[o.ClientID] = o
	delete(i.claims, o.ClientID)
}
