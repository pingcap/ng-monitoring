package subscriber_test

import (
	"testing"

	"github.com/pingcap/ng-monitoring/component/subscriber"
	"github.com/pingcap/ng-monitoring/component/topology"
	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/ng-monitoring/config/pdvariable"

	"github.com/golang/mock/gomock"
)

type testSuite struct {
	subscriber     *subscriber.Subscriber
	varSubscriber  pdvariable.Subscriber
	topoSubscriber topology.Subscriber
	cfgSubscriber  config.Subscriber
}

func newTestSuite(controller subscriber.SubscribeController) *testSuite {
	ts := &testSuite{
		varSubscriber:  make(pdvariable.Subscriber),
		topoSubscriber: make(topology.Subscriber),
		cfgSubscriber:  make(config.Subscriber),
	}
	ts.subscriber = subscriber.NewSubscriber(nil, ts.topoSubscriber, ts.varSubscriber, ts.cfgSubscriber, controller)
	return ts
}

func (ts *testSuite) close() {
	ts.subscriber.Close()
}

func TestSubscriberBasic(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	routine := func(controller subscriber.SubscribeController) {
		ts := newTestSuite(controller)
		defer ts.close()

		ts.varSubscriber <- varGetter(pdvariable.PDVariable{})
		ts.cfgSubscriber <- cfgGetter(config.Config{})
		ts.topoSubscriber <- topoGetter([]topology.Component{{Name: "foo"}})
		ts.topoSubscriber <- topoGetter([]topology.Component{{Name: "bar"}})
		ts.cfgSubscriber <- cfgGetter(config.Config{})
	}

	controller := NewMockSubscribeController(ctrl)

	// always enabled
	controller.EXPECT().IsEnabled().Return(true).AnyTimes()
	controller.EXPECT().Name().Return("test-subscriber-basic").AnyTimes()

	c := controller.EXPECT().UpdatePDVariable(gomock.Eq(pdvariable.PDVariable{})).Times(1)
	c = controller.EXPECT().UpdateConfig(gomock.Eq(config.Config{})).Times(1).After(c)

	// topology changed: a `foo` component is found
	// * related scraper is created
	c = controller.EXPECT().UpdateTopology(gomock.Eq([]topology.Component{{Name: "foo"}})).Times(1).After(c)
	scraperFoo := NewMockScraper(ctrl)
	scraperFoo.EXPECT().IsDown().Return(false).AnyTimes()
	scraperFoo.EXPECT().Run().Times(1)
	c = controller.EXPECT().NewScraper(gomock.Any(), gomock.Eq(topology.Component{Name: "foo"})).Return(scraperFoo).Times(1).After(c)

	// topology changed: `foo` is removed and `bar` is added
	// * `foo` related scraper is closed
	// * `bar` related scraper is created
	c = controller.EXPECT().UpdateTopology(gomock.Eq([]topology.Component{{Name: "bar"}})).Times(1).After(c)
	c = scraperFoo.EXPECT().Close().Times(1).After(c)
	scraperBar := NewMockScraper(ctrl)
	scraperBar.EXPECT().IsDown().Return(false).AnyTimes()
	scraperBar.EXPECT().Run().Times(1)
	c = controller.EXPECT().NewScraper(gomock.Any(), gomock.Eq(topology.Component{Name: "bar"})).Return(scraperBar).Times(1).After(c)

	// trigger a config update to see what scrapers are closed finally
	c = controller.EXPECT().UpdateConfig(gomock.Eq(config.Config{})).Times(1).After(c)
	scraperBar.EXPECT().Close().Times(1).After(c)

	routine(controller)
}

func TestSubscriberFixedComponent(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	routine := func(controller subscriber.SubscribeController) {
		ts := newTestSuite(controller)
		defer ts.close()

		ts.topoSubscriber <- topoGetter([]topology.Component{{Name: "foo"}})
		ts.topoSubscriber <- topoGetter([]topology.Component{{Name: "foo"}})
		ts.cfgSubscriber <- cfgGetter(config.Config{})
	}

	controller := NewMockSubscribeController(ctrl)

	// always enabled
	controller.EXPECT().IsEnabled().Return(true).AnyTimes()
	controller.EXPECT().Name().Return("test-subscriber-fixed-component").AnyTimes()

	// topology changed: a `foo` component is found
	// * related scraper is created
	c := controller.EXPECT().UpdateTopology(gomock.Eq([]topology.Component{{Name: "foo"}})).Times(1)
	scraperFoo := NewMockScraper(ctrl)
	scraperFoo.EXPECT().Run().Times(1)
	scraperFoo.EXPECT().IsDown().Return(false).AnyTimes()
	c = controller.EXPECT().NewScraper(gomock.Any(), gomock.Eq(topology.Component{Name: "foo"})).Return(scraperFoo).Times(1).After(c)

	// topology isn't changed, no scraper changed
	c = controller.EXPECT().UpdateTopology(gomock.Eq([]topology.Component{{Name: "foo"}})).Times(1).After(c)

	// trigger a config update to see what scrapers are closed finally
	c = controller.EXPECT().UpdateConfig(gomock.Eq(config.Config{})).Times(1).After(c)
	scraperFoo.EXPECT().Close().Times(1).After(c)

	routine(controller)
}

func TestSubscriberAddComponent(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	routine := func(controller subscriber.SubscribeController) {
		ts := newTestSuite(controller)
		defer ts.close()

		ts.topoSubscriber <- topoGetter([]topology.Component{{Name: "foo"}})
		ts.topoSubscriber <- topoGetter([]topology.Component{{Name: "foo"}, {Name: "bar"}})
		ts.cfgSubscriber <- cfgGetter(config.Config{})
	}

	controller := NewMockSubscribeController(ctrl)

	// always enabled
	controller.EXPECT().IsEnabled().Return(true).AnyTimes()
	controller.EXPECT().Name().Return("test-subscriber-add-component").AnyTimes()

	// topology changed: a `foo` component is found
	// * related scraper is created
	c := controller.EXPECT().UpdateTopology(gomock.Eq([]topology.Component{{Name: "foo"}})).Times(1)
	scraperFoo := NewMockScraper(ctrl)
	scraperFoo.EXPECT().Run().Times(1)
	scraperFoo.EXPECT().IsDown().Return(false).AnyTimes()
	c = controller.EXPECT().NewScraper(gomock.Any(), gomock.Eq(topology.Component{Name: "foo"})).Return(scraperFoo).Times(1).After(c)

	// topology changed: a `bar` component is added
	// * `bar` related scraper is created
	c = controller.EXPECT().UpdateTopology(gomock.Eq([]topology.Component{{Name: "foo"}, {Name: "bar"}})).Times(1).After(c)
	scraperBar := NewMockScraper(ctrl)
	scraperBar.EXPECT().Run().Times(1)
	scraperBar.EXPECT().IsDown().Return(false).AnyTimes()
	c = controller.EXPECT().NewScraper(gomock.Any(), gomock.Eq(topology.Component{Name: "bar"})).Return(scraperBar).Times(1).After(c)

	// trigger a config update to see what scrapers are closed finally
	c = controller.EXPECT().UpdateConfig(gomock.Eq(config.Config{})).Times(1).After(c)
	scraperFoo.EXPECT().Close().Times(1).After(c)
	scraperBar.EXPECT().Close().Times(1).After(c)

	routine(controller)
}

func TestSubscriberRemoveComponent(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	routine := func(controller subscriber.SubscribeController) {
		ts := newTestSuite(controller)
		defer ts.close()

		ts.topoSubscriber <- topoGetter([]topology.Component{{Name: "foo"}})
		ts.topoSubscriber <- topoGetter([]topology.Component{})
		ts.cfgSubscriber <- cfgGetter(config.Config{})
	}

	controller := NewMockSubscribeController(ctrl)

	// always enabled
	controller.EXPECT().IsEnabled().Return(true).AnyTimes()
	controller.EXPECT().Name().Return("test-subscriber-remove-component").AnyTimes()

	// topology changed: a `foo` component is found
	// * related scraper is created
	c := controller.EXPECT().UpdateTopology(gomock.Eq([]topology.Component{{Name: "foo"}})).Times(1)
	scraperFoo := NewMockScraper(ctrl)
	scraperFoo.EXPECT().Run().Times(1)
	scraperFoo.EXPECT().IsDown().Return(false).AnyTimes()
	c = controller.EXPECT().NewScraper(gomock.Any(), gomock.Eq(topology.Component{Name: "foo"})).Return(scraperFoo).Times(1).After(c)

	// topology changed: a `foo` component is removed
	// * related scraper is closed
	c = controller.EXPECT().UpdateTopology(gomock.Eq([]topology.Component{})).Times(1).After(c)
	c = scraperFoo.EXPECT().Close().Times(1).After(c)

	// trigger a config update to see what scrapers are closed finally
	controller.EXPECT().UpdateConfig(gomock.Eq(config.Config{})).Times(1).After(c)

	routine(controller)
}

func TestSubscriberSwitch(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	routine := func(controller subscriber.SubscribeController) {
		ts := newTestSuite(controller)
		defer ts.close()

		ts.topoSubscriber <- topoGetter([]topology.Component{{Name: "foo"}})
		ts.varSubscriber <- varGetter(pdvariable.PDVariable{})
		ts.varSubscriber <- varGetter(pdvariable.PDVariable{})
		ts.cfgSubscriber <- cfgGetter(config.Config{})
	}

	controller := NewMockSubscribeController(ctrl)
	controller.EXPECT().Name().Return("test-subscriber-switch").AnyTimes()

	// enabled at the beginning
	c := controller.EXPECT().IsEnabled().Return(true).Times(1)

	// topology changed: a `foo` component is found
	// switch is not changed, still on
	// * related scraper is created
	c = controller.EXPECT().UpdateTopology(gomock.Eq([]topology.Component{{Name: "foo"}})).Times(1).After(c)
	c = controller.EXPECT().IsEnabled().Return(true).Times(1).After(c)
	scraperFoo1 := NewMockScraper(ctrl)
	scraperFoo1.EXPECT().Run().Times(1)
	scraperFoo1.EXPECT().IsDown().Return(false).AnyTimes()
	c = controller.EXPECT().NewScraper(gomock.Any(), gomock.Eq(topology.Component{Name: "foo"})).Return(scraperFoo1).Times(1).After(c)

	// topology is not changed, but switch is changed: on -> off
	// * related scraper is closed and removed
	c = controller.EXPECT().UpdatePDVariable(gomock.Eq(pdvariable.PDVariable{})).Times(1).After(c)
	c = controller.EXPECT().IsEnabled().Return(false).Times(1).After(c)
	c = scraperFoo1.EXPECT().Close().Times(1).After(c)

	// topology is not changed, but switch is changed: off -> on
	// * related scraper is created again
	c = controller.EXPECT().UpdatePDVariable(gomock.Eq(pdvariable.PDVariable{})).Times(1).After(c)
	c = controller.EXPECT().IsEnabled().Return(true).Times(1).After(c)
	scraperFoo2 := NewMockScraper(ctrl)
	scraperFoo2.EXPECT().Run().Times(1)
	scraperFoo2.EXPECT().IsDown().Return(false).AnyTimes()
	c = controller.EXPECT().NewScraper(gomock.Any(), gomock.Eq(topology.Component{Name: "foo"})).Return(scraperFoo2).Times(1).After(c)

	// trigger a config update to see what scrapers are closed finally
	c = controller.EXPECT().UpdateConfig(gomock.Eq(config.Config{})).Times(1).After(c)
	c = controller.EXPECT().IsEnabled().Return(true).Times(1).After(c)
	scraperFoo2.EXPECT().Close().Times(1).After(c)

	routine(controller)
}

func TestSubscriberScraperDown(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	routine := func(controller subscriber.SubscribeController) {
		ts := newTestSuite(controller)
		defer ts.close()

		ts.topoSubscriber <- topoGetter([]topology.Component{{Name: "foo"}})
		ts.cfgSubscriber <- cfgGetter(config.Config{})
		ts.cfgSubscriber <- cfgGetter(config.Config{})
	}

	controller := NewMockSubscribeController(ctrl)

	// always enabled
	controller.EXPECT().IsEnabled().Return(true).AnyTimes()
	controller.EXPECT().Name().Return("test-subscriber-scraper-down").AnyTimes()

	// topology changed: a `foo` component is found
	// * related scraper is created
	c := controller.EXPECT().UpdateTopology(gomock.Eq([]topology.Component{{Name: "foo"}})).Times(1)
	scraperFoo1 := NewMockScraper(ctrl)
	scraperFoo1.EXPECT().Run().Times(1)
	c = controller.EXPECT().NewScraper(gomock.Any(), gomock.Eq(topology.Component{Name: "foo"})).Return(scraperFoo1).Times(1).After(c)

	// old scraper is down, so a new one is created
	c = controller.EXPECT().UpdateConfig(gomock.Eq(config.Config{})).Times(1).After(c)
	c = scraperFoo1.EXPECT().IsDown().Return(true).Times(1).After(c)
	c = scraperFoo1.EXPECT().Close().Times(1).After(c)
	scraperFoo2 := NewMockScraper(ctrl)
	scraperFoo2.EXPECT().Run().Times(1)
	scraperFoo2.EXPECT().IsDown().Return(false).AnyTimes()
	c = controller.EXPECT().NewScraper(gomock.Any(), gomock.Eq(topology.Component{Name: "foo"})).Return(scraperFoo2).Times(1).After(c)

	// trigger a config update to see what scrapers are closed finally
	c = controller.EXPECT().UpdateConfig(gomock.Eq(config.Config{})).Times(1).After(c)
	scraperFoo2.EXPECT().Close().Times(1).After(c)

	routine(controller)
}

func varGetter(variable pdvariable.PDVariable) pdvariable.GetLatestPDVariable {
	return func() pdvariable.PDVariable {
		return variable
	}
}

func topoGetter(value []topology.Component) topology.GetLatestTopology {
	return func() []topology.Component {
		return value
	}
}

func cfgGetter(value config.Config) config.GetLatestConfig {
	return func() config.Config {
		return value
	}
}
