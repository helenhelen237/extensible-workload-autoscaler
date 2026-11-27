package internal

import (
	"context"
	"sync"
	"testing"
	"time"
)

// recordingScrapes records the scrapes of a scraperSet.
type recordingScrapes struct {
	mu sync.Mutex
	// scrapes counts the scrapes of each endpoint.
	scrapes map[string]int
	// last is the last target scraped for each endpoint.
	last map[string]*target
	// goroutines counts the distinct goroutine contexts that scraped each
	// endpoint, i.e. how many times its scraper was started.
	goroutines map[string]map[context.Context]bool
}

func newRecordingScrapes() *recordingScrapes {
	return &recordingScrapes{
		scrapes:    make(map[string]int),
		last:       make(map[string]*target),
		goroutines: make(map[string]map[context.Context]bool),
	}
}

func (r *recordingScrapes) scrape(ctx context.Context, t *target) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.scrapes[t.url]++
	r.last[t.url] = t
	if r.goroutines[t.url] == nil {
		r.goroutines[t.url] = make(map[context.Context]bool)
	}
	r.goroutines[t.url][ctx] = true
}

func (r *recordingScrapes) snapshot(url string) (scrapes, goroutines int, last *target) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.scrapes[url], len(r.goroutines[url]), r.last[url]
}

// waitFor polls cond until it holds or the test times out.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestScraperSet(t *testing.T) {
	rec := newRecordingScrapes()
	s := newScraperSet(time.Millisecond, rec.scrape)
	ctx := context.Background()

	// Two endpoints: one goroutine each, scraping repeatedly.
	a1 := &target{url: "http://a"}
	b1 := &target{url: "http://b"}
	s.sync(ctx, []*target{a1, b1})
	waitFor(t, "both endpoints to be scraped repeatedly", func() bool {
		na, _, _ := rec.snapshot("http://a")
		nb, _, _ := rec.snapshot("http://b")
		return na >= 3 && nb >= 3
	})

	// An updated target for a running endpoint is picked up by the same
	// goroutine; a removed endpoint stops being scraped.
	a2 := &target{url: "http://a", token: "new"}
	s.sync(ctx, []*target{a2})
	waitFor(t, "the updated target to be scraped", func() bool {
		_, _, last := rec.snapshot("http://a")
		return last == a2
	})
	if _, g, _ := rec.snapshot("http://a"); g != 1 {
		t.Errorf("endpoint a was scraped by %d goroutines, want 1: an updated target must not restart its scraper", g)
	}
	if _, ok := s.running["http://b"]; ok {
		t.Errorf("the scraper of removed endpoint b is still running")
	}
	nb, _, _ := rec.snapshot("http://b")
	time.Sleep(20 * time.Millisecond)
	if nb2, _, _ := rec.snapshot("http://b"); nb2 > nb+1 {
		t.Errorf("removed endpoint b was still scraped: %d scrapes after removal", nb2-nb)
	}

	// stop stops every goroutine.
	s.stop()
	na, _, _ := rec.snapshot("http://a")
	time.Sleep(20 * time.Millisecond)
	if na2, _, _ := rec.snapshot("http://a"); na2 != na {
		t.Errorf("endpoint a was scraped %d times after stop", na2-na)
	}
	if len(s.running) != 0 {
		t.Errorf("%d scrapers still registered after stop", len(s.running))
	}
}
