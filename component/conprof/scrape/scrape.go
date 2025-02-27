package scrape

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/pingcap/ng-monitoring/component/conprof/jeprof"
	"github.com/pingcap/ng-monitoring/component/conprof/meta"
	"github.com/pingcap/ng-monitoring/component/conprof/store"
	"github.com/pingcap/ng-monitoring/component/topology"
	"github.com/pingcap/ng-monitoring/config"

	"github.com/pingcap/log"
	"github.com/pkg/errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/net/context/ctxhttp"
)

type ScrapeSuite struct {
	scraper          Scraper
	lastScrape       time.Time
	lastScrapeStatus atomic.Int64
	lastScrapeSize   int
	store            *store.ProfileStorage
	ctx              context.Context
	cancel           func()
}

func newScrapeSuite(ctx context.Context, sc Scraper, store *store.ProfileStorage) *ScrapeSuite {
	sl := &ScrapeSuite{
		scraper: sc,
		store:   store,
	}
	sl.ctx, sl.cancel = context.WithCancel(ctx)
	return sl
}

func (sl *ScrapeSuite) run(ticker *TickerChan) {
	target := sl.scraper.target

	defer func() {
		ticker.Stop()
		log.Debug("scraper stop running",
			zap.String("component", target.Component),
			zap.String("address", target.Address),
			zap.String("kind", target.Kind))
	}()

	log.Debug("scraper start to run",
		zap.String("component", target.Component),
		zap.String("address", target.Address),
		zap.String("kind", target.Kind))

	buf := bytes.NewBuffer(make([]byte, 0, 1024))
	sl.lastScrapeSize = 0
	var start time.Time
	for {
		select {
		case <-sl.ctx.Done():
			return
		case start = <-ticker.ch:
			sl.lastScrape = start
			sl.lastScrapeStatus.Store(int64(meta.ProfileStatusRunning))
		}

		if sl.lastScrapeSize > 0 && buf.Cap() > 2*sl.lastScrapeSize {
			// shrink the buffer size.
			buf = bytes.NewBuffer(make([]byte, 0, sl.lastScrapeSize))
		}

		buf.Reset()
		scrapeCtx, cancel := context.WithTimeout(sl.ctx, time.Second*time.Duration(config.GetGlobalConfig().ContinueProfiling.TimeoutSeconds))
		scrapeErr := sl.scraper.scrape(scrapeCtx, buf)
		cancel()
		if scrapeErr != nil {
			if scrapeErr != context.Canceled {
				log.Error("scrape failed",
					zap.String("component", target.Component),
					zap.String("address", target.Address),
					zap.String("kind", target.Kind),
					zap.Error(scrapeErr))
			}
		}
		sl.lastScrapeSize = buf.Len()

		err := sl.store.AddProfile(meta.ProfileTarget{
			Kind:      sl.scraper.target.Kind,
			Component: sl.scraper.target.Component,
			Address:   sl.scraper.target.Address,
		}, start, buf.Bytes(), scrapeErr)
		if err != nil {
			log.Error("save scrape data failed",
				zap.String("component", target.Component),
				zap.String("address", target.Address),
				zap.String("kind", target.Kind),
				zap.Time("start", start),
				zap.Error(err))
		}

		if scrapeErr != nil || err != nil {
			sl.lastScrapeStatus.Store(int64(meta.ProfileStatusFailed))
		} else {
			sl.lastScrapeStatus.Store(int64(meta.ProfileStatusFinished))
		}
	}
}

// Stop the scraping. May still write data and stale markers after it has
// returned. Cancel the context to stop all writes.
func (sl *ScrapeSuite) stop() {
	sl.cancel()
}

type Scraper struct {
	target *Target
	client *http.Client
	req    *http.Request
}

func newScraper(target *Target, client *http.Client) Scraper {
	return Scraper{
		target: target,
		client: client,
	}
}

func (s *Scraper) scrape(ctx context.Context, w io.Writer) error {
	cfg := config.GetGlobalConfig()
	if !cfg.ContinueProfiling.Enable {
		return nil
	}

	if s.target.Component == topology.ComponentTiKV && s.target.Kind == meta.ProfileKindHeap {
		// use jeprof to fetch tikv heap profile
		data, err := jeprof.FetchRaw(s.target.GetURLString(), cfg.Security.GetHTTPClientConfig())
		if err != nil {
			return err
		}
		_, err = w.Write(data)
		return err
	}

	if s.req == nil {
		req, err := http.NewRequest("GET", s.target.GetURLString(), nil)
		if err != nil {
			return err
		}
		if header := s.target.header; len(header) > 0 {
			for k, v := range header {
				req.Header.Set(k, v)
			}
		}

		s.req = req
	}

	resp, err := ctxhttp.Do(ctx, s.client, s.req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned HTTP status %s", resp.Status)
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return errors.Wrap(err, "failed to read body")
	}

	_, err = w.Write(b)
	return err
}

// Target refers to a singular HTTP or HTTPS endpoint.
type Target struct {
	meta.ProfileTarget
	header map[string]string
	*url.URL
}

func NewTarget(component, address, scrapeAddress, kind, schema string, cfg *config.PprofProfilingConfig) *Target {
	t := &Target{
		ProfileTarget: meta.ProfileTarget{
			Kind:      kind,
			Component: component,
			Address:   address,
		},
	}
	vs := url.Values{}
	for k, v := range cfg.Params {
		vs.Set(k, v)
	}
	if cfg.Seconds > 0 {
		vs.Add("seconds", strconv.Itoa(cfg.Seconds))
	}

	t.header = cfg.Header
	t.URL = &url.URL{
		Scheme:   schema,
		Host:     scrapeAddress,
		Path:     cfg.Path,
		RawQuery: vs.Encode(),
	}
	return t
}

func (t *Target) GetURLString() string {
	return t.URL.String()
}
