package store

import (
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

type capture struct {
	mu     sync.Mutex
	writes [][]Metric
	err    error
}

func (c *capture) write(ms []Metric) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writes = append(c.writes, ms)
	return c.err
}

// points returns every written (timestamp, value) of the aggregate record series
// for sqlDigest, in write order.
func (c *capture) points(name, instanceType, sqlDigest string) (ts, vs []uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, w := range c.writes {
		for _, m := range w {
			t, ok := m.Metric.(recordTags)
			if !ok || t.Name != name || t.Instance != AggregateInstance || t.InstanceType != instanceType || t.SQLDigest != sqlDigest {
				continue
			}
			ts = append(ts, m.TimestampMs...)
			vs = append(vs, m.Values...)
		}
	}
	return
}

func (c *capture) markers() map[string][]uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[string][]uint64{}
	for _, w := range c.writes {
		for _, m := range w {
			if t, ok := m.Metric.(instanceTags); ok {
				out[t.Instance+"/"+t.InstanceType] = append(out[t.Instance+"/"+t.InstanceType], m.TimestampMs...)
			}
		}
	}
	return out
}

func (c *capture) dataWrites() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, w := range c.writes {
		for _, m := range w {
			if _, ok := m.Metric.(instanceTags); !ok {
				n++
				break
			}
		}
	}
	return n
}

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (f *fakeClock) now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.t
}

func (f *fakeClock) advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.t = f.t.Add(d)
}

func newTestAggregator(t *testing.T, horizon time.Duration, maxPoints int) (*aggregator, *capture, *fakeClock) {
	c := &capture{}
	clk := &fakeClock{t: time.Unix(10000, 0)}
	a := newAggregator(AggregatorConfig{FlushHorizon: horizon, MaxPoints: maxPoints, FlushInterval: time.Hour}, c.write)
	a.now = clk.now
	a.start()
	t.Cleanup(a.close)
	return a, c, clk
}

// tickAndWait runs one flush tick and waits for the writer to finish it.
func tickAndWait(a *aggregator) {
	a.tick()
	a.pending.Wait()
}

func rec(name, instance, instanceType, sqlDigest string, tsSec []uint64, values []uint64) Metric {
	m := Metric{Metric: recordTags{Name: name, Instance: instance, InstanceType: instanceType, SQLDigest: sqlDigest, PlanDigest: "p"}}
	for i := range tsSec {
		m.TimestampMs = append(m.TimestampMs, tsSec[i]*1000)
		m.Values = append(m.Values, values[i])
	}
	return m
}

func sortedPairs(ts, vs []uint64) ([]uint64, []uint64) {
	idx := make([]int, len(ts))
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(a, b int) bool { return ts[idx[a]] < ts[idx[b]] })
	ots, ovs := make([]uint64, len(idx)), make([]uint64, len(idx))
	for i, j := range idx {
		ots[i], ovs[i] = ts[j], vs[j]
	}
	return ots, ovs
}

func droppedPoints(reason string) float64 {
	return testutil.ToFloat64(aggDroppedPoints.WithLabelValues(reason))
}

func TestAggregatorSumsAcrossInstances(t *testing.T) {
	a, c, clk := newTestAggregator(t, 180*time.Second, 1000)

	a.add([]Metric{rec(MetricNameCPUTime, "tidb-1", "tidb", "sql-a", []uint64{9900, 9901}, []uint64{10, 20})})
	a.add([]Metric{rec(MetricNameCPUTime, "tidb-2", "tidb", "sql-a", []uint64{9900, 9901}, []uint64{1, 2})})
	a.add([]Metric{rec(MetricNameCPUTime, "tidb-3", "tidb", "sql-b", []uint64{9901}, []uint64{7})})
	// An evicted ("other") row has an empty digest and aggregates too.
	a.add([]Metric{rec(MetricNameCPUTime, "tidb-1", "tidb", "", []uint64{9900}, []uint64{100})})
	a.add([]Metric{rec(MetricNameCPUTime, "tidb-2", "tidb", "", []uint64{9900}, []uint64{50})})

	// Nothing has waited a full horizon yet.
	tickAndWait(a)
	require.Zero(t, c.dataWrites())

	clk.advance(180 * time.Second)
	tickAndWait(a)

	ts, vs := sortedPairs(c.points(MetricNameCPUTime, "tidb", "sql-a"))
	require.Equal(t, []uint64{9900000, 9901000}, ts)
	require.Equal(t, []uint64{11, 22}, vs)
	_, vs = c.points(MetricNameCPUTime, "tidb", "sql-b")
	require.Equal(t, []uint64{7}, vs)
	_, vs = c.points(MetricNameCPUTime, "tidb", "")
	require.Equal(t, []uint64{150}, vs)

	a.mu.Lock()
	require.Zero(t, a.points)
	require.Empty(t, a.open)
	require.Empty(t, a.fifo)
	a.mu.Unlock()
}

func TestAggregatorKeepsTypesAndTablesApartAndSkipsRegions(t *testing.T) {
	a, c, clk := newTestAggregator(t, time.Second, 1000)

	tidb := rec(MetricNameCPUTime, "tidb-1", "tidb", "sql-a", []uint64{9990}, []uint64{5})
	tikvT1 := Metric{Metric: recordTags{Name: MetricNameCPUTime, Instance: "tikv-1", InstanceType: "tikv", SQLDigest: "sql-a", PlanDigest: "p", DB: "db", Table: "t1"},
		TimestampMs: []uint64{9990000}, Values: []uint64{3}}
	tikvT2 := Metric{Metric: recordTags{Name: MetricNameCPUTime, Instance: "tikv-2", InstanceType: "tikv", SQLDigest: "sql-a", PlanDigest: "p", DB: "db", Table: "t2"},
		TimestampMs: []uint64{9990000}, Values: []uint64{4}}
	tikvT1b := Metric{Metric: recordTags{Name: MetricNameCPUTime, Instance: "tikv-2", InstanceType: "tikv", SQLDigest: "sql-a", PlanDigest: "p", DB: "db", Table: "t1"},
		TimestampMs: []uint64{9990000}, Values: []uint64{6}}
	region := Metric{Metric: regionTags{Name: MetricNameRegionCPUTime, Instance: "tikv-1", InstanceType: "tikv", RegionID: "42"},
		TimestampMs: []uint64{9990000}, Values: []uint64{9}}
	marker := Metric{Metric: instanceTags{Name: MetricNameInstance, Instance: "tidb-1", InstanceType: "tidb"}, TimestampMs: []uint64{9990000}, Values: []uint64{1}}

	a.add([]Metric{tidb, tikvT1, tikvT2, tikvT1b, region, marker})
	clk.advance(time.Second)
	tickAndWait(a)

	var tables = map[string][]uint64{}
	c.mu.Lock()
	for _, w := range c.writes {
		for _, m := range w {
			switch tg := m.Metric.(type) {
			case recordTags:
				tables[tg.InstanceType+"/"+tg.Table] = m.Values
			case regionTags:
				t.Fatalf("region series must not be aggregated: %+v", tg)
			case instanceTags:
				require.Equal(t, AggregateInstance, tg.Instance)
			}
		}
	}
	c.mu.Unlock()
	require.Equal(t, map[string][]uint64{
		"tidb/":   {5},
		"tikv/t1": {9}, // 3 + 6 across two TiKV
		"tikv/t2": {4},
	}, tables)
}

// A contribution for a second whose sum was already written is written as one more
// sample. The embedded VictoriaMetrics keeps duplicate-timestamp samples and every
// read path sums them, so the stored total stays correct (Artem #1, #2).
func TestAggregatorWritesLateContributionsAsExtraSamples(t *testing.T) {
	a, c, clk := newTestAggregator(t, 100*time.Second, 1000)
	reopenedBefore := testutil.ToFloat64(aggReopenedSlots)

	a.add([]Metric{rec(MetricNameCPUTime, "tidb-1", "tidb", "sql-a", []uint64{9950}, []uint64{10})})
	clk.advance(100 * time.Second)
	tickAndWait(a)

	// tidb-2 reports the same second after it was flushed (slow instance, or a
	// restart of ng-monitoring in between).
	a.add([]Metric{rec(MetricNameCPUTime, "tidb-2", "tidb", "sql-a", []uint64{9950}, []uint64{20})})
	require.Equal(t, reopenedBefore+1, testutil.ToFloat64(aggReopenedSlots))
	tickAndWait(a)
	ts, _ := c.points(MetricNameCPUTime, "tidb", "sql-a")
	require.Len(t, ts, 1, "the reopened second waits its own horizon")

	clk.advance(100 * time.Second)
	tickAndWait(a)
	ts, vs := c.points(MetricNameCPUTime, "tidb", "sql-a")
	require.Equal(t, []uint64{9950000, 9950000}, ts)
	require.Equal(t, []uint64{10, 20}, vs)
}

// Seconds are held for the horizon measured from when they first arrived, not from
// their record timestamp, so instance clock skew neither drops nor stalls data.
func TestAggregatorFlushesByArrivalTime(t *testing.T) {
	a, c, clk := newTestAggregator(t, 60*time.Second, 1000)

	a.add([]Metric{rec(MetricNameCPUTime, "tidb-1", "tidb", "old", []uint64{1000}, []uint64{1})})     // far in the past
	a.add([]Metric{rec(MetricNameCPUTime, "tidb-1", "tidb", "future", []uint64{99999}, []uint64{2})}) // far in the future
	clk.advance(59 * time.Second)
	tickAndWait(a)
	require.Zero(t, c.dataWrites())

	clk.advance(time.Second)
	tickAndWait(a)
	_, vs := c.points(MetricNameCPUTime, "tidb", "old")
	require.Equal(t, []uint64{1}, vs)
	_, vs = c.points(MetricNameCPUTime, "tidb", "future")
	require.Equal(t, []uint64{2}, vs)
}

// At the cap the oldest open second is written early instead of new data being
// dropped for the rest of the horizon, and rejected points never allocate buffer
// entries (Artem #4, #8).
func TestAggregatorCapFlushesOldestEarly(t *testing.T) {
	a, c, clk := newTestAggregator(t, 100*time.Second, 2)
	capBefore := droppedPoints(dropReasonCap)
	earlyBefore := testutil.ToFloat64(aggEarlyFlushes)

	a.add([]Metric{rec(MetricNameCPUTime, "tidb-1", "tidb", "sql-a", []uint64{9950}, []uint64{2})})
	clk.advance(time.Second)
	a.add([]Metric{rec(MetricNameCPUTime, "tidb-1", "tidb", "sql-a", []uint64{9951}, []uint64{3})})
	// Adding to an existing point needs no capacity.
	a.add([]Metric{rec(MetricNameCPUTime, "tidb-2", "tidb", "sql-a", []uint64{9950}, []uint64{40})})
	require.Equal(t, capBefore, droppedPoints(dropReasonCap))

	// A third distinct point is over the cap: it is dropped and second 9950 (the
	// oldest arrival) is detached for writing right away.
	a.add([]Metric{rec(MetricNameCPUTime, "tidb-1", "tidb", "sql-new", []uint64{9952}, []uint64{4})})
	require.Equal(t, capBefore+1, droppedPoints(dropReasonCap))
	require.Equal(t, earlyBefore+1, testutil.ToFloat64(aggEarlyFlushes))
	a.mu.Lock()
	_, hasNew := a.open[9952000]
	a.mu.Unlock()
	require.False(t, hasNew, "a rejected point must not open a slot")

	a.pending.Wait()
	ts, vs := c.points(MetricNameCPUTime, "tidb", "sql-a")
	require.Equal(t, []uint64{9950000}, ts)
	require.Equal(t, []uint64{42}, vs)

	// Capacity is released once the early write completes.
	a.add([]Metric{rec(MetricNameCPUTime, "tidb-1", "tidb", "sql-new", []uint64{9952}, []uint64{4})})
	require.Equal(t, capBefore+1, droppedPoints(dropReasonCap))
	a.mu.Lock()
	require.Equal(t, 2, a.points)
	a.mu.Unlock()
}

// A failed import is counted and not retried: VictoriaMetrics may have stored part
// of the batch, so a retry could double-count (Artem #1).
func TestAggregatorCountsWriteErrorsWithoutRetry(t *testing.T) {
	a, c, clk := newTestAggregator(t, 10*time.Second, 1000)
	c.err = errors.New("vminsert: 503")
	errsBefore := testutil.ToFloat64(aggWriteErrors)
	droppedBefore := droppedPoints(dropReasonWriteError)

	a.add([]Metric{rec(MetricNameCPUTime, "tidb-1", "tidb", "sql-a", []uint64{9950, 9951}, []uint64{1, 2})})
	clk.advance(10 * time.Second)
	tickAndWait(a)

	require.Equal(t, errsBefore+1, testutil.ToFloat64(aggWriteErrors))
	require.Equal(t, droppedBefore+2, droppedPoints(dropReasonWriteError))
	require.Equal(t, 1, c.dataWrites())
	a.mu.Lock()
	require.Zero(t, a.points)
	a.mu.Unlock()

	tickAndWait(a)
	require.Equal(t, 1, c.dataWrites(), "no retry")
}

// The "*" marker is written on every tick at the current time, like the real
// instances' markers, so short time ranges and idle periods still list "*"; it
// stops two horizons after the last contribution of that type (Artem #3).
func TestAggregatorMarkerEveryTickAtNow(t *testing.T) {
	a, c, clk := newTestAggregator(t, 100*time.Second, 1000)

	tickAndWait(a)
	require.Empty(t, c.markers(), "no marker before any data")

	a.add([]Metric{rec(MetricNameCPUTime, "tidb-1", "tidb", "sql-a", []uint64{9950}, []uint64{1})})
	tickAndWait(a)
	clk.advance(150 * time.Second)
	tickAndWait(a) // idle tick after the data was flushed
	require.Equal(t, []uint64{10000000, 10150000}, c.markers()["*/tidb"])

	clk.advance(51 * time.Second) // > 2 horizons since the last contribution
	tickAndWait(a)
	require.Equal(t, []uint64{10000000, 10150000}, c.markers()["*/tidb"])
}

// When the writer falls behind, a full queue drops the detached seconds and counts
// them instead of blocking the scrapers.
func TestAggregatorDropsWhenQueueFull(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	c := &capture{}
	clk := &fakeClock{t: time.Unix(10000, 0)}
	blocking := func(ms []Metric) error {
		<-release
		return c.write(ms)
	}
	a := newAggregator(AggregatorConfig{FlushHorizon: time.Second, MaxPoints: 1000, FlushInterval: time.Hour, QueueSlots: 1}, blocking)
	a.now = clk.now
	a.start()
	t.Cleanup(func() {
		once.Do(func() { close(release) })
		a.close()
	})
	before := droppedPoints(dropReasonQueueFull)

	// Job 1 is taken by the writer and blocks; job 2 fills the queue; job 3 is dropped.
	for i := uint64(0); i < 3; i++ {
		a.add([]Metric{rec(MetricNameCPUTime, "tidb-1", "tidb", "sql-a", []uint64{9950 + i}, []uint64{1})})
		clk.advance(time.Second)
		a.tick()
		if i == 0 {
			require.Eventually(t, func() bool { return len(a.queue) == 0 }, time.Second, time.Millisecond)
		}
	}
	require.Equal(t, before+1, droppedPoints(dropReasonQueueFull))

	once.Do(func() { close(release) })
	a.pending.Wait()
	ts, _ := c.points(MetricNameCPUTime, "tidb", "sql-a")
	require.ElementsMatch(t, []uint64{9950000, 9951000}, ts)
}

func TestAggregatorCloseWritesEverything(t *testing.T) {
	c := &capture{}
	a := newAggregator(AggregatorConfig{FlushHorizon: time.Hour, MaxPoints: 1000, FlushInterval: time.Hour}, c.write)
	a.start()
	a.add([]Metric{rec(MetricNameCPUTime, "tidb-1", "tidb", "sql-a", []uint64{9950}, []uint64{1})})
	a.close()
	_, vs := c.points(MetricNameCPUTime, "tidb", "sql-a")
	require.Equal(t, []uint64{1}, vs)

	// Records that race with shutdown are ignored rather than panicking.
	a.add([]Metric{rec(MetricNameCPUTime, "tidb-1", "tidb", "sql-a", []uint64{9951}, []uint64{1})})
	a.tick()
}
