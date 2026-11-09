package kernel

import (
	"sync"
	"time"

	"github.com/peter123023/orderfence/internal/venue"
)

// Event 是一条回报事实，只记收到什么，不记推断。
type Event struct {
	At       time.Time   `json:"at"`
	Kind     string      `json:"kind"`
	ClientID string      `json:"client_id"`
	State    venue.State `json:"state"`
	Filled   int64       `json:"filled"`
	Err      string      `json:"err,omitempty"`
}

// Journal 是追加写的事件流，同时维护一份本地订单视图。
type Journal struct {
	mu   sync.Mutex
	ev   []Event
	view map[string]venue.Order
}

func NewJournal() *Journal {
	return &Journal{view: make(map[string]venue.Order)}
}

func (j *Journal) Append(e Event) {
	if e.At.IsZero() {
		e.At = time.Now()
	}
	j.mu.Lock()
	defer j.mu.Unlock()

	j.ev = append(j.ev, e)

	if e.ClientID == "" {
		return
	}
	o := j.view[e.ClientID]
	o.ClientID = e.ClientID
	if e.State != venue.Unknown {
		o.State = e.State
	}
	if e.Filled > o.Filled {
		o.Filled = e.Filled
	}
	o.UpdatedAt = e.At
	j.view[e.ClientID] = o
}

// Track 记录订单的静态字段。已有记录时不覆盖，避免把状态打回原形。
func (j *Journal) Track(o venue.Order) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if _, ok := j.view[o.ClientID]; ok {
		return
	}
	j.view[o.ClientID] = o
}

// LocalOpen 返回本地认为还可能有敞口的订单。
func (j *Journal) LocalOpen() map[string]venue.Order {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make(map[string]venue.Order)
	for id, o := range j.view {
		if o.State.Open() || o.State == venue.PendingNew || o.State == venue.PendingCancel {
			out[id] = o
		}
	}
	return out
}

func (j *Journal) View(clientID string) (venue.Order, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	o, ok := j.view[clientID]
	return o, ok
}

func (j *Journal) Events() []Event {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make([]Event, len(j.ev))
	copy(out, j.ev)
	return out
}
