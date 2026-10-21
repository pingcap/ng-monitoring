package store

import (
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

// AggregateInstance is the value of the `instance` label under which cross-instance
// sums are stored. Query paths treat it like any other instance, so
// `/topsql/v1/summary?instance=*&instance_type=tidb` returns the cluster-wide view.
//
// The sums are best effort: "*" is the sum of the records the aggregator accepted.
// Points dropped at the buffer cap, on a full write queue or by a failed write make
// it incomplete; every such drop is counted in ng_monitoring_topsql_aggregator_*.
// Buffered points are lost if the process is killed.
const AggregateInstance = "*"

const defaultQueueSlots = 1024

// maxWrittenSeconds bounds the record of recently written seconds that only feeds
// the reopened-seconds counter.
const maxWrittenSeconds = 1 << 16

const (
	dropReasonCap        = "cap"
	dropReasonQueueFull  = "queue_full"
	dropReasonWriteError = "write_error"
)

var (
	aggBufferedPoints = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ng_monitoring_topsql_aggregator_buffered_points",
		Help: "Cross-instance TopSQL points held in memory, open or waiting to be written.",
	})
	aggWrittenPoints = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ng_monitoring_topsql_aggregator_written_points_total",
		Help: "Cross-instance TopSQL points written to the TSDB.",
	})
	aggDroppedPoints = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ng_monitoring_topsql_aggregator_dropped_points_total",
		Help: "Cross-instance TopSQL points lost, by reason (cap, queue_full, write_error).",
	}, []string{"reason"})
	aggWriteErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ng_monitoring_topsql_aggregator_write_errors_total",
		Help: "Failed TSDB imports of cross-instance TopSQL series. Failed imports are not retried.",
	})
	aggEarlyFlushes = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ng_monitoring_topsql_aggregator_early_flushes_total",
		Help: "Seconds written before the flush horizon because the buffer cap was reached.",
	})
	aggReopenedSlots = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ng_monitoring_topsql_aggregator_reopened_seconds_total",
		Help: "Seconds that received contributions after their sum was already written; the late part is written as an extra sample.",
	})
	aggQueuedJobs = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ng_monitoring_topsql_aggregator_queued_jobs",
		Help: "Write jobs waiting for the cross-instance TopSQL writer.",
	})
)

// AggregatorConfig controls the write-side aggregator. See config.TopSQL.
type AggregatorConfig struct {
	// FlushHorizon is how long a second stays open for other instances' reports,
	// measured from when its first contribution arrived.
	FlushHorizon time.Duration
	// MaxPoints caps the (series, second) points held in memory, open or queued.
	MaxPoints int
	// FlushInterval is how often due seconds are written and the "*" marker is
	// refreshed. Zero picks a default derived from FlushHorizon.
	FlushInterval time.Duration
	// QueueSlots bounds the write jobs waiting for the writer. Zero picks a default.
	QueueSlots int
}

// seriesKey identifies one aggregated series. DB/Table are only set for TiKV SQL
// metrics. Instance is intentionally absent.
type seriesKey struct {
	Name         string
	InstanceType string
	SQLDigest    string
	PlanDigest   string
	DB           string
	Table        string
}

// slot holds the cross-instance sums for one record second. It is opened by the
// first contribution for that second and written FlushHorizon later.
type slot struct {
	tsMs     uint64
	openedAt time.Time
	sums     map[seriesKey]uint64
}

type writeJob struct {
	slots      []*slot
	points     int
	markers    []string // instance types whose "*" marker is refreshed
	markerTsMs uint64
}

// aggregator sums TopSQL samples across instances per (series, second) and writes
// the sums as instance="*" series.
//
// The embedded VictoriaMetrics keeps samples with duplicate timestamps and every
// TopSQL read path uses sum_over_time, so a second can be written more than once:
// contributions that arrive after its sum was written open a new slot and are
// written as one more sample. Writes are never retried, because a failed
// /api/v1/import may have stored part of the batch.
type aggregator struct {
	cfg   AggregatorConfig
	write func([]Metric) error
	now   func() time.Time

	mu        sync.Mutex
	open      map[uint64]*slot // record second (ms) -> open slot
	fifo      []*slot          // open slots in arrival order
	points    int              // open + queued points, bounded by cfg.MaxPoints
	lastSeen  map[string]time.Time
	written   map[uint64]time.Time // seconds detached for writing within the last two horizons
	closed    bool
	queue     chan writeJob
	pending   sync.WaitGroup // queued or in-flight jobs
	closeCh   chan struct{}
	closeOnce sync.Once
	loopWg    sync.WaitGroup
	writerWg  sync.WaitGroup
}

func newAggregator(cfg AggregatorConfig, write func([]Metric) error) *aggregator {
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = cfg.FlushHorizon / 4
		if cfg.FlushInterval > 10*time.Second {
			cfg.FlushInterval = 10 * time.Second
		}
		if cfg.FlushInterval < 100*time.Millisecond {
			cfg.FlushInterval = 100 * time.Millisecond
		}
	}
	if cfg.QueueSlots <= 0 {
		cfg.QueueSlots = defaultQueueSlots
	}
	return &aggregator{
		cfg:      cfg,
		write:    write,
		now:      time.Now,
		open:     make(map[uint64]*slot),
		lastSeen: make(map[string]time.Time),
		written:  make(map[uint64]time.Time),
		queue:    make(chan writeJob, cfg.QueueSlots),
		closeCh:  make(chan struct{}),
	}
}

func (a *aggregator) start() {
	a.writerWg.Add(1)
	go a.writer()
	a.loopWg.Add(1)
	go a.loop()
}

func (a *aggregator) loop() {
	defer a.loopWg.Done()
	t := time.NewTicker(a.cfg.FlushInterval)
	defer t.Stop()
	for {
		select {
		case <-a.closeCh:
			return
		case <-t.C:
			a.tick()
		}
	}
}

// close stops the flush loop and writes everything that is still buffered.
func (a *aggregator) close() {
	a.closeOnce.Do(func() {
		close(a.closeCh)
		a.loopWg.Wait()

		a.mu.Lock()
		a.closed = true
		job := writeJob{slots: a.fifo}
		for _, s := range a.fifo {
			job.points += len(s.sums)
		}
		a.fifo = nil
		a.open = make(map[uint64]*slot)
		a.mu.Unlock()

		if job.points > 0 {
			a.pending.Add(1)
			aggQueuedJobs.Inc()
			a.queue <- job // the writer is still draining, so this cannot block forever
		}
		close(a.queue)
		a.writerWg.Wait()
	})
}

// add folds per-instance metrics into the open slots. It never does I/O.
func (a *aggregator) add(ms []Metric) {
	a.mu.Lock()
	defer a.mu.Unlock()
	nowT := a.now()
	if a.closed {
		return
	}
	earlyFlushed := false
	for i := range ms {
		key, ok := keyOf(ms[i].Metric)
		if !ok {
			continue
		}
		a.lastSeen[key.InstanceType] = nowT
		for j, ts := range ms[i].TimestampMs {
			if j >= len(ms[i].Values) {
				break
			}
			s := a.open[ts]
			if s != nil {
				if _, exists := s.sums[key]; exists {
					s.sums[key] += ms[i].Values[j]
					continue
				}
			}
			if a.points >= a.cfg.MaxPoints {
				aggDroppedPoints.WithLabelValues(dropReasonCap).Inc()
				if !earlyFlushed {
					earlyFlushed = true
					a.flushOldestLocked(nowT)
				}
				continue
			}
			if s == nil {
				s = &slot{tsMs: ts, openedAt: nowT, sums: make(map[seriesKey]uint64)}
				a.open[ts] = s
				a.fifo = append(a.fifo, s)
				if _, ok := a.written[ts]; ok {
					aggReopenedSlots.Inc()
				}
			}
			s.sums[key] = ms[i].Values[j]
			a.points++
		}
	}
	aggBufferedPoints.Set(float64(a.points))
}

// flushOldestLocked detaches the oldest open slot so the writer frees its points.
func (a *aggregator) flushOldestLocked(nowT time.Time) {
	if len(a.fifo) == 0 {
		return
	}
	s := a.fifo[0]
	a.fifo[0] = nil
	a.fifo = a.fifo[1:]
	delete(a.open, s.tsMs)
	a.markWrittenLocked(s.tsMs, nowT)
	aggEarlyFlushes.Inc()
	a.enqueueLocked(writeJob{slots: []*slot{s}, points: len(s.sums)})
}

func (a *aggregator) markWrittenLocked(tsMs uint64, nowT time.Time) {
	if len(a.written) < maxWrittenSeconds {
		a.written[tsMs] = nowT
	}
}

// tick detaches every slot that has been open for a full horizon and refreshes the
// "*" marker for each instance type that contributed within the last two horizons.
func (a *aggregator) tick() {
	a.mu.Lock()
	defer a.mu.Unlock()
	nowT := a.now()
	if a.closed {
		return
	}

	var job writeJob
	n := 0
	for n < len(a.fifo) && nowT.Sub(a.fifo[n].openedAt) >= a.cfg.FlushHorizon {
		s := a.fifo[n]
		a.fifo[n] = nil
		delete(a.open, s.tsMs)
		a.markWrittenLocked(s.tsMs, nowT)
		job.slots = append(job.slots, s)
		job.points += len(s.sums)
		n++
	}
	a.fifo = a.fifo[n:]
	for ts, at := range a.written {
		if nowT.Sub(at) > 2*a.cfg.FlushHorizon {
			delete(a.written, ts)
		}
	}

	for instanceType, seen := range a.lastSeen {
		if nowT.Sub(seen) > 2*a.cfg.FlushHorizon {
			delete(a.lastSeen, instanceType)
			continue
		}
		job.markers = append(job.markers, instanceType)
	}
	job.markerTsMs = uint64(nowT.UnixMilli())

	if job.points == 0 && len(job.markers) == 0 {
		return
	}
	a.enqueueLocked(job)
}

// enqueueLocked hands a job to the writer without blocking. On a full queue the
// job's points are dropped and released.
func (a *aggregator) enqueueLocked(job writeJob) {
	a.pending.Add(1)
	aggQueuedJobs.Inc()
	select {
	case a.queue <- job:
	default:
		aggQueuedJobs.Dec()
		a.pending.Done()
		if job.points > 0 {
			aggDroppedPoints.WithLabelValues(dropReasonQueueFull).Add(float64(job.points))
			a.points -= job.points
			aggBufferedPoints.Set(float64(a.points))
			log.Warn("topsql aggregator write queue is full, dropping aggregated points",
				zap.Int("points", job.points))
		}
	}
}

func (a *aggregator) writer() {
	defer a.writerWg.Done()
	for job := range a.queue {
		aggQueuedJobs.Dec()
		a.writeJob(job)
		if job.points > 0 {
			a.mu.Lock()
			a.points -= job.points
			aggBufferedPoints.Set(float64(a.points))
			a.mu.Unlock()
		}
		a.pending.Done()
	}
}

func (a *aggregator) writeJob(job writeJob) {
	out := buildMetrics(job)
	if len(out) == 0 {
		return
	}
	if err := a.write(out); err != nil {
		aggWriteErrors.Inc()
		aggDroppedPoints.WithLabelValues(dropReasonWriteError).Add(float64(job.points))
		log.Warn("failed to write aggregated topsql series",
			zap.Error(err), zap.Int("series", len(out)), zap.Int("points", job.points))
		return
	}
	aggWrittenPoints.Add(float64(job.points))
}

// buildMetrics turns a job into one Metric per aggregated series. Two slots for the
// same second (a reopened one) are merged, since they are written together.
func buildMetrics(job writeJob) []Metric {
	bySeries := make(map[seriesKey]map[uint64]uint64)
	for _, s := range job.slots {
		for key, v := range s.sums {
			pts := bySeries[key]
			if pts == nil {
				pts = make(map[uint64]uint64)
				bySeries[key] = pts
			}
			pts[s.tsMs] += v
		}
	}
	out := make([]Metric, 0, len(bySeries)+len(job.markers))
	for key, pts := range bySeries {
		m := Metric{Metric: tagsOf(key)}
		for ts, v := range pts {
			m.TimestampMs = append(m.TimestampMs, ts)
			m.Values = append(m.Values, v)
		}
		out = append(out, m)
	}
	for _, instanceType := range job.markers {
		out = append(out, Metric{
			Metric:      instanceTags{Name: MetricNameInstance, Instance: AggregateInstance, InstanceType: instanceType},
			TimestampMs: []uint64{job.markerTsMs},
			Values:      []uint64{1},
		})
	}
	return out
}

// keyOf returns the aggregation key of a per-instance series. Region series are
// not aggregated: their cardinality is per region, not per statement, and the
// query service rejects region grouping for the aggregate instance.
func keyOf(tags interface{}) (seriesKey, bool) {
	t, ok := tags.(recordTags)
	if !ok {
		return seriesKey{}, false
	}
	return seriesKey{Name: t.Name, InstanceType: t.InstanceType, SQLDigest: t.SQLDigest, PlanDigest: t.PlanDigest, DB: t.DB, Table: t.Table}, true
}

func tagsOf(k seriesKey) interface{} {
	return recordTags{Name: k.Name, Instance: AggregateInstance, InstanceType: k.InstanceType, SQLDigest: k.SQLDigest, PlanDigest: k.PlanDigest, DB: k.DB, Table: k.Table}
}
