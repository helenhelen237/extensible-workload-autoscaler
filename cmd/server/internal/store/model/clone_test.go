package model

import (
	"testing"
	"time"
)

func TestCloneIsolation(t *testing.T) {
	if (*Distribution)(nil).Clone() != nil {
		t.Fatal("nil Distribution.Clone() != nil")
	}
	if (*DecayingHistogram)(nil).Clone() != nil {
		t.Fatal("nil DecayingHistogram.Clone() != nil")
	}
	if (*Series)(nil).Clone() != nil {
		t.Fatal("nil Series.Clone() != nil")
	}

	now := time.Unix(1000, 0)
	hist, err := NewDecayingHistogram(now, time.Hour, "0.5")
	if err != nil {
		t.Fatalf("NewDecayingHistogram() error = %v", err)
	}
	hist.Add(1.2, now)

	dist := &Distribution{Buckets: map[string]float64{"1.0": 3, "+Inf": 5}}
	orig := &Series{
		PodName:       "pod-1",
		ContainerName: "app",
		ResourceName:  "cpu",
		Labels:        map[string]string{"env": "prod"},
		Type:          SampleTypeGauge,
		LastRaw: Sample{
			Timestamp:         now,
			Value:             2.5,
			Distribution:      dist,
			DecayingHistogram: hist,
		},
		LastRawWindow: 15 * time.Second,
		LastIngested:  now.Add(time.Second),
	}

	cloned := orig.Clone()
	if cloned.TimeWindow != nil {
		t.Errorf("cloned.TimeWindow = %v, want nil", cloned.TimeWindow)
	}

	// Mutate original maps and fields in place.
	orig.Labels["env"] = "staging"
	orig.Labels["extra"] = "1"
	orig.LastRaw.Value = 99.0
	orig.LastRaw.Distribution.Buckets["1.0"] = 999
	orig.LastRaw.DecayingHistogram.Add(4.5, now.Add(time.Minute))

	if got := cloned.Labels["env"]; got != "prod" {
		t.Errorf("cloned.Labels[env] = %q, want %q", got, "prod")
	}
	if _, exists := cloned.Labels["extra"]; exists {
		t.Error("cloned.Labels mutated when original Labels changed")
	}
	if cloned.LastRaw.Value != 2.5 {
		t.Errorf("cloned.LastRaw.Value = %v, want 2.5", cloned.LastRaw.Value)
	}
	if got := cloned.LastRaw.Distribution.Buckets["1.0"]; got != 3 {
		t.Errorf("cloned Distribution bucket[1.0] = %v, want 3", got)
	}
	if cloned.LastRaw.DecayingHistogram.TotalWeight != 1.0 {
		t.Errorf("cloned DecayingHistogram.TotalWeight = %v, want 1.0", cloned.LastRaw.DecayingHistogram.TotalWeight)
	}
}
