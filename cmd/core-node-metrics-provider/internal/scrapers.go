package internal

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// scrapeInterval is the interval between two scrapes of an endpoint.
const scrapeInterval = 5 * time.Second

// scraperSet runs one goroutine per scrape target, keyed by endpoint URL.
//
// A goroutine is started when its endpoint appears and stopped when it
// disappears. A running goroutine reads its target on every tick, so the
// metrics wanted from its endpoint, and the policies and pods that receive
// them, change without restarting it.
//
// sync and stop must be called from a single goroutine.
type scraperSet struct {
	interval time.Duration
	// scrape scrapes the endpoint of a target once and sends what its
	// exporter returns.
	scrape func(ctx context.Context, t *target)

	running map[string]*runningScraper
	wg      sync.WaitGroup
}

type runningScraper struct {
	cancel context.CancelFunc
	target atomic.Pointer[target]
}

func newScraperSet(interval time.Duration, scrape func(ctx context.Context, t *target)) *scraperSet {
	return &scraperSet{
		interval: interval,
		scrape:   scrape,
		running:  make(map[string]*runningScraper),
	}
}

// sync makes the running scrapers match the targets: a goroutine is started
// for each new endpoint and stopped for each endpoint no longer scraped. The
// goroutines of the other endpoints get their updated target.
func (s *scraperSet) sync(ctx context.Context, targets []*target) {
	desired := make(map[string]*target, len(targets))
	for _, t := range targets {
		desired[t.url] = t
	}

	for url, r := range s.running {
		if t, ok := desired[url]; ok {
			r.target.Store(t)
			continue
		}
		slog.Info("Stopping scraper", "url", url)
		r.cancel()
		delete(s.running, url)
	}

	for url, t := range desired {
		if _, ok := s.running[url]; ok {
			continue
		}
		slog.Info("Starting scraper", "url", url)
		s.start(ctx, t)
	}
}

func (s *scraperSet) start(ctx context.Context, t *target) {
	ctx, cancel := context.WithCancel(ctx)
	r := &runningScraper{cancel: cancel}
	r.target.Store(t)
	s.running[t.url] = r

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.run(ctx, r)
	}()
}

// run scrapes the endpoint of a target right away, then every interval, until
// the context is cancelled.
func (s *scraperSet) run(ctx context.Context, r *runningScraper) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		s.scrape(ctx, r.target.Load())
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// stop stops all the scrapers and waits for their goroutines to return.
func (s *scraperSet) stop() {
	for url, r := range s.running {
		r.cancel()
		delete(s.running, url)
	}
	s.wg.Wait()
}
