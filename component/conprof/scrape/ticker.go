package scrape

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/ng-monitoring/utils"
)

type Ticker struct {
	sync.Mutex
	interval    time.Duration
	subscribers map[int]chan time.Time
	idAlloc     int
	cancel      context.CancelFunc
	lastTime    time.Time
}

func NewTicker(d time.Duration) *Ticker {
	if d == 0 {
		panic("should never happen")
	}
	ctx, cancel := context.WithCancel(context.Background())
	t := &Ticker{
		interval:    d,
		subscribers: make(map[int]chan time.Time),
		cancel:      cancel,
	}
	go utils.GoWithRecovery(func() {
		t.run(ctx)
	}, nil)
	return t
}

type TickerChan struct {
	id     int
	ch     chan time.Time
	ticker *Ticker
}

func (tc *TickerChan) Stop() {
	tc.ticker.Lock()
	defer tc.ticker.Unlock()
	delete(tc.ticker.subscribers, tc.id)
}

func (t *Ticker) Subscribe() *TickerChan {
	ch := make(chan time.Time, 1)
	t.Lock()
	defer t.Unlock()

	t.idAlloc += 1
	id := t.idAlloc
	t.subscribers[id] = ch
	return &TickerChan{
		id:     id,
		ch:     ch,
		ticker: t,
	}
}

func (t *Ticker) Reset(d time.Duration) {
	if t.interval == d {
		return
	}
	t.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	t.cancel = cancel
	t.interval = d
	go utils.GoWithRecovery(func() {
		t.run(ctx)
	}, nil)
}

func (t *Ticker) run(ctx context.Context) {
	nextStart := int64(t.interval) - time.Now().UnixNano()%int64(t.interval)
	select {
	case <-time.After(time.Duration(nextStart)):
		// Continue after the scraping offset.
	case <-ctx.Done():
		return
	}

	t.notify(time.Now())

	ticker := time.NewTicker(t.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			t.notify(now)
		}
	}
}

func (t *Ticker) notify(now time.Time) {
	t.Lock()
	defer t.Unlock()
	t.lastTime = now
	for _, ch := range t.subscribers {
		select {
		case ch <- now:
		default:
		}
	}
}

func (t *Ticker) Stop() {
	if t.cancel != nil {
		t.cancel()
	}
}
