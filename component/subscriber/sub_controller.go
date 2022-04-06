package subscriber

import (
	"context"

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
}

type ScraperFactory interface {
	NewScraper(ctx context.Context, component topology.Component) Scraper
}

type Scraper interface {
	Run()
	IsDown() bool
	Close()
}
