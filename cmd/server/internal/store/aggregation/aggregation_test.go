package aggregation

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/protobuf/types/known/durationpb"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
	"github.com/gke-labs/extensible-workload-autoscaler/cmd/server/internal/store/model"
)

// t0 is the reference time of the tests.
var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// at returns the time d after t0.
func at(d time.Duration) time.Time {
	return t0.Add(d)
}

// add feeds a scalar sample measured at ts to w.
func add(w model.TimeWindow, ts time.Time, value float64) {
	w.AddSample(model.Sample{Timestamp: ts, Value: value})
}

// applyValue returns the scalar aggregate of w, failing if there is none.
func applyValue(t *testing.T, w model.TimeWindow) float64 {
	t.Helper()
	s, ok := w.Apply()
	if !ok {
		t.Fatalf("Apply() reported no value")
	}
	return s.Value
}

func TestRateAggregation_InstantaneousRate(t *testing.T) {
	// A zero window computes the rate between the last two samples.
	r := NewRateAggregation(0, model.SampleTypeGauge)
	if _, ok := r.Apply(); ok {
		t.Errorf("Apply() reported a value for an empty window")
	}

	add(r, t0, 0)
	if _, ok := r.Apply(); ok {
		t.Errorf("Apply() reported a value for a single sample")
	}

	add(r, at(10*time.Second), 100)
	if got, want := applyValue(t, r), 10.0; got != want {
		t.Errorf("rate = %v, want %v", got, want)
	}

	// (500 - 100) / 20s, rather than (500 - 0) / 30s.
	add(r, at(30*time.Second), 500)
	if got, want := applyValue(t, r), 20.0; got != want {
		t.Errorf("rate = %v, want %v", got, want)
	}
	if got, want := r.Len(), 2; got != want {
		t.Errorf("Len() = %v, want %v", got, want)
	}
}

func TestRateAggregation_InstantaneousRateCounterReset(t *testing.T) {
	r := NewRateAggregation(0, model.SampleTypeCounter)
	add(r, t0, 100)
	add(r, at(10*time.Second), 150)
	// The counter resets: it grew by 20 from zero over the last 10s.
	add(r, at(20*time.Second), 20)
	if got, want := applyValue(t, r), 2.0; got != want {
		t.Errorf("rate = %v, want %v", got, want)
	}
}

func TestRateAggregation_WindowSpansItsSamples(t *testing.T) {
	// A positive window computes the rate between its oldest and its newest
	// samples.
	r := NewRateAggregation(time.Minute, model.SampleTypeGauge)
	add(r, t0, 0)
	add(r, at(10*time.Second), 100)
	add(r, at(30*time.Second), 500)
	if got, want := applyValue(t, r), 500.0/30; !cmp.Equal(got, want, cmpopts.EquateApprox(0, 1e-9)) {
		t.Errorf("rate = %v, want %v", got, want)
	}

	// The first sample falls out of the window.
	add(r, at(70*time.Second), 900)
	if got, want := applyValue(t, r), 800.0/60; !cmp.Equal(got, want, cmpopts.EquateApprox(0, 1e-9)) {
		t.Errorf("rate = %v, want %v", got, want)
	}
	if got, want := r.Len(), 3; got != want {
		t.Errorf("Len() = %v, want %v", got, want)
	}
}

func TestDecayingDistributionAggregation_InstantaneousRate(t *testing.T) {
	// A zero rate window feeds the histogram with the instantaneous rates.
	def := pb.DecayingDistribution_builder{
		HalfLife:   durationpb.New(time.Hour),
		BucketSize: new("1"),
		Rate:       durationpb.New(0),
	}.Build()
	d := NewDecayingDistributionAggregation(def, model.SampleTypeCounter)
	if d.rate == nil {
		t.Fatalf("no rate window for a zero rate")
	}

	add(d, t0, 0)
	if _, ok := d.Apply(); ok {
		t.Errorf("Apply() reported a value before any rate could be computed")
	}
	// +100 in 10s: a rate of 10/s is observed.
	add(d, at(10*time.Second), 100)
	// +500 in 20s: a rate of 25/s is observed, rather than 600 / 30s = 20/s.
	add(d, at(30*time.Second), 600)

	s, ok := d.Apply()
	if !ok {
		t.Fatalf("Apply() reported no value")
	}
	hist := s.DecayingHistogram
	if got, want := hist.TotalWeight, 2.0; !cmp.Equal(got, want, cmpopts.EquateApprox(0, 0.01)) {
		t.Errorf("TotalWeight = %v, want ~%v", got, want)
	}
	// The p100 falls in the bucket of the highest observation, [25, 26).
	if got, want := hist.Percentile(1, hist.ReferenceTime), 26.0; got != want {
		t.Errorf("p100 = %v, want %v", got, want)
	}
}

func TestNewDecayingDistributionAggregation_NoRate(t *testing.T) {
	def := pb.DecayingDistribution_builder{
		HalfLife:   durationpb.New(time.Hour),
		BucketSize: new("1"),
	}.Build()
	if d := NewDecayingDistributionAggregation(def, model.SampleTypeGauge); d.rate != nil {
		t.Errorf("got a rate window without a rate configured")
	}
}

func TestDecayingDistributionAggregation_InvalidBucketSizeDoesNotPanic(t *testing.T) {
	for _, bucketSize := range []string{"invalid", "100Mi", "0", "-1", "NaN", "+Inf"} {
		t.Run("gauge/"+bucketSize, func(t *testing.T) {
			def := pb.DecayingDistribution_builder{
				HalfLife:   durationpb.New(time.Hour),
				BucketSize: new(bucketSize),
				Percentile: new("p95"),
			}.Build()
			d := NewDecayingDistributionAggregation(def, model.SampleTypeGauge)
			add(d, t0, 10)
			add(d, at(10*time.Second), 20)
			if _, ok := d.Apply(); ok {
				t.Errorf("Apply() reported a value for invalid bucket size %q", bucketSize)
			}
		})

		t.Run("rate/"+bucketSize, func(t *testing.T) {
			def := pb.DecayingDistribution_builder{
				HalfLife:   durationpb.New(time.Hour),
				BucketSize: new(bucketSize),
				Percentile: new("p95"),
				Rate:       durationpb.New(0),
			}.Build()
			d := NewDecayingDistributionAggregation(def, model.SampleTypeCounter)
			add(d, t0, 0)
			add(d, at(10*time.Second), 100)
			if _, ok := d.Apply(); ok {
				t.Errorf("Apply() reported a value for invalid bucket size %q", bucketSize)
			}
		})
	}
}

func TestGlobalDecayingDistribution_IsolatesPerSeriesRateWindows(t *testing.T) {
	def := pb.MetricDefinition_builder{
		Name: new("cpu_distribution"),
		DecayingDistribution: pb.DecayingDistribution_builder{
			HalfLife:   durationpb.New(time.Hour),
			BucketSize: new("0.1"),
			Percentile: new("p100"),
			Rate:       durationpb.New(0),
		}.Build(),
	}.Build()

	agg := CreateSeriesAggregation(def, model.SampleTypeCounter, "cpu", "pod-a", "main")
	workload := map[string]*pb.PodState{
		"pod-a": pb.PodState_builder{Name: new("pod-a"), IsReady: new(true)}.Build(),
		"pod-b": pb.PodState_builder{Name: new("pod-b"), IsReady: new(true)}.Build(),
	}

	serA := model.NewSeries("pod-a", "main", "cpu", nil, model.SampleTypeCounter, agg.NewTimeWindow(def, model.SampleTypeCounter))
	serB := model.NewSeries("pod-b", "main", "cpu", nil, model.SampleTypeCounter, agg.NewTimeWindow(def, model.SampleTypeCounter))

	// First scrape at the exact same timestamp, with very different counter
	// baselines (1000s vs 10s). Neither series can emit a rate yet, and pod-b's
	// smaller counter must not look like a reset of pod-a's counter.
	serA.AddSample(model.Sample{Timestamp: t0, Value: 1000}, t0)
	serB.AddSample(model.Sample{Timestamp: t0, Value: 10}, t0)
	if got := agg.Reduce(func(yield func(*model.Series) bool) {
		if !yield(serA) {
			return
		}
		yield(serB)
	}, workload, t0); len(got) != 0 {
		t.Fatalf("Reduce() after single scrape per series = %v, want empty", got)
	}

	// Second scrape at t0+10s:
	//   pod-a: +5s over 10s = 0.5 cores -> bucket [0.5, 0.6), upper bound 0.6
	//   pod-b: +2s over 10s = 0.2 cores -> bucket [0.2, 0.3), upper bound 0.3
	// Even though both pods report at the exact same timestamp, both rate
	// observations must be recorded independently in the shared histogram.
	serA.AddSample(model.Sample{Timestamp: at(10 * time.Second), Value: 1005}, at(10*time.Second))
	serB.AddSample(model.Sample{Timestamp: at(10 * time.Second), Value: 12}, at(10*time.Second))

	s, ok := serA.TimeWindow.Apply()
	if !ok {
		t.Fatalf("TimeWindow.Apply() reported no value")
	}
	if got, want := s.DecayingHistogram.TotalWeight, 2.0; !cmp.Equal(got, want, cmpopts.EquateApprox(0, 1e-6)) {
		t.Errorf("TotalWeight = %v, want %v (both pods' rates must count)", got, want)
	}

	res := agg.Reduce(func(yield func(*model.Series) bool) {}, workload, at(10*time.Second))
	if len(res) != 1 {
		t.Fatalf("len(Reduce()) = %d, want 1", len(res))
	}
	if got, want := res[0].sample.Value, 0.6; !cmp.Equal(got, want, cmpopts.EquateApprox(0, 1e-6)) {
		t.Errorf("p100 = %v, want %v", got, want)
	}

	// Third scrape: pod-c joins with a slightly earlier kubelet timestamp (18s)
	// after pod-a already reported at 20s, and pod-a restarts its counter (reset
	// to 1.0 -> 0.1 cores over 10s). Neither cross-series clock skew nor pod-a's
	// reset may produce negative or inflated rates.
	serC := model.NewSeries("pod-c", "main", "cpu", nil, model.SampleTypeCounter, agg.NewTimeWindow(def, model.SampleTypeCounter))
	serA.AddSample(model.Sample{Timestamp: at(20 * time.Second), Value: 1.0}, at(20*time.Second))
	serC.AddSample(model.Sample{Timestamp: at(8 * time.Second), Value: 500}, at(20*time.Second))
	serC.AddSample(model.Sample{Timestamp: at(18 * time.Second), Value: 503}, at(20*time.Second)) // 0.3 cores

	res = agg.Reduce(func(yield func(*model.Series) bool) {}, workload, at(20*time.Second))
	if len(res) != 1 {
		t.Fatalf("len(Reduce()) = %d, want 1", len(res))
	}
	if got, want := res[0].sample.Value, 0.6; !cmp.Equal(got, want, cmpopts.EquateApprox(0, 1e-6)) {
		t.Errorf("p100 after counter reset and lagged series = %v, want %v", got, want)
	}
}

func TestPodScopeDistributions_MergeContainersWithoutResourceRequests(t *testing.T) {
	// Pod has no container resource requests set; histogram rollups across
	// containers must merge observations directly rather than dropping series
	// via scalar request-share weighting.
	workload := map[string]*pb.PodState{
		"pod-1": pb.PodState_builder{
			Name:    new("pod-1"),
			IsReady: new(true),
			Containers: []*pb.ContainerState{
				pb.ContainerState_builder{Name: new("main")}.Build(),
				pb.ContainerState_builder{Name: new("sidecar")}.Build(),
			},
		}.Build(),
	}

	t.Run("DecayingDistribution", func(t *testing.T) {
		def := pb.MetricDefinition_builder{
			Name:  new("cpu_pod"),
			Scope: new(model.ScopePod),
			DecayingDistribution: pb.DecayingDistribution_builder{
				HalfLife:   durationpb.New(time.Hour),
				BucketSize: new("0.1"),
				Percentile: new("p100"),
			}.Build(),
		}.Build()

		agg := CreateSeriesAggregation(def, model.SampleTypeGauge, "cpu", "pod-1", "main")
		sMain := model.NewSeries("pod-1", "main", "cpu", nil, model.SampleTypeGauge, agg.NewTimeWindow(def, model.SampleTypeGauge))
		sSidecar := model.NewSeries("pod-1", "sidecar", "cpu", nil, model.SampleTypeGauge, agg.NewTimeWindow(def, model.SampleTypeGauge))
		sMain.AddSample(model.Sample{Timestamp: t0, Value: 0.25}, t0)
		sSidecar.AddSample(model.Sample{Timestamp: t0, Value: 0.75}, t0)

		res := agg.Reduce(func(yield func(*model.Series) bool) {
			if !yield(sMain) {
				return
			}
			yield(sSidecar)
		}, workload, t0)
		if len(res) != 1 {
			t.Fatalf("len(Reduce()) = %d, want 1", len(res))
		}
		if got, want := res[0].sample.Value, 0.8; !cmp.Equal(got, want, cmpopts.EquateApprox(0, 1e-6)) {
			t.Errorf("pod p100 = %v, want %v", got, want)
		}
	})

	t.Run("Distribution", func(t *testing.T) {
		def := pb.MetricDefinition_builder{
			Name:  new("lat_pod"),
			Scope: new(model.ScopePod),
			Distribution: pb.Distribution_builder{
				Percentile: new("p50"),
			}.Build(),
		}.Build()

		agg := CreateSeriesAggregation(def, model.SampleTypeHistogram, "cpu", "pod-1", "main")
		sMain := model.NewSeries("pod-1", "main", "cpu", nil, model.SampleTypeHistogram, agg.NewTimeWindow(def, model.SampleTypeHistogram))
		sMain.AddSample(model.Sample{Timestamp: t0, Distribution: &model.Distribution{Buckets: map[string]float64{"1": 0, "+Inf": 0}}}, t0)
		sMain.AddSample(model.Sample{Timestamp: at(10 * time.Second), Distribution: &model.Distribution{Buckets: map[string]float64{"1": 10, "+Inf": 10}}}, at(10*time.Second))

		res := agg.Reduce(func(yield func(*model.Series) bool) {
			yield(sMain)
		}, workload, at(10*time.Second))
		if len(res) != 1 {
			t.Fatalf("len(Reduce()) = %d, want 1", len(res))
		}
		if got, want := res[0].sample.Value, 0.5; !cmp.Equal(got, want, cmpopts.EquateApprox(0, 1e-6)) {
			t.Errorf("pod p50 = %v, want %v", got, want)
		}
	})
}

func TestDecayingDistributionAggregation_MultiStepDecayReportsNoValue(t *testing.T) {
	halfLife := 10 * time.Second
	def := pb.DecayingDistribution_builder{
		HalfLife:   durationpb.New(halfLife),
		BucketSize: new("1"),
		Percentile: new("p95"),
	}.Build()
	d := NewDecayingDistributionAggregation(def, model.SampleTypeGauge)
	add(d, t0, 5)

	// Decay in two steps of 11 half-lives each so per-bucket pruning clears all
	// buckets without triggering the single-step fast reset.
	d.histogram.Percentile(0.95, at(11*halfLife))
	d.histogram.Percentile(0.95, at(22*halfLife))

	if _, ok := d.Apply(); ok {
		t.Errorf("Apply() = true after all buckets decayed below threshold, want false")
	}
}
