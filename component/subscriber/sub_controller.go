package subscriber

import (
	"context"
	"net/http"
	"sync"

	"github.com/pingcap/ng-monitoring/component/topology"
	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/ng-monitoring/config/pdvariable"
)

type SubscribeController interface {
	ScraperFactory

	Name() string
	IsEnabled() bool
	UpdatePDVariable(pdvariable.PDVariable)
	UpdateConfig(config.Config)
	UpdateTopology([]topology.Component)
	NewHTTPClient() *http.Client
}

type ScraperFactory interface {
	NewScraper(ctx context.Context, component topology.Component, schemaInfo *sync.Map) Scraper
}

type Scraper interface {
	Run()
	IsDown() bool
	Close()
}
