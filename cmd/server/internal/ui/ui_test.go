package ui

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
	"github.com/gke-labs/extensible-workload-autoscaler/cmd/server/internal/store"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/clock"
)

func TestHandleIndex(t *testing.T) {
	s := store.NewMemoryStore()
	mux := http.NewServeMux()
	RegisterHandlers(mux, s)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("GET / status = %d, want %d", rec.Code, http.StatusOK)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "xAS Server") {
		t.Errorf("GET / body missing title")
	}
	if strings.Contains(body, "series.ControlMetric") {
		t.Errorf("GET / script still references removed series.ControlMetric field")
	}

	notFoundReq := httptest.NewRequest(http.MethodGet, "/unknown", nil)
	notFoundRec := httptest.NewRecorder()
	mux.ServeHTTP(notFoundRec, notFoundReq)
	if notFoundRec.Code != http.StatusNotFound {
		t.Errorf("GET /unknown status = %d, want %d", notFoundRec.Code, http.StatusNotFound)
	}
}

func TestRenderWorkloadTablePodMetrics(t *testing.T) {
	nodeBin, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node not found in PATH; skipping JS execution test")
	}

	start := time.Unix(1000, 0)
	clk := &clock.FakeClock{CurrentTime: start}
	s := store.NewMemoryStoreWithClock(clk)

	id := pb.PolicyId_builder{ClusterName: new("default"), Namespace: new("default"), Name: new("web-pol")}.Build()
	pol := pb.Policy_builder{
		Id: id,
		Workload: pb.WorkloadRef_builder{
			Kind: new("Deployment"),
			Name: new("web"),
		}.Build(),
		MinReplicas: new(int32(1)),
		MaxReplicas: new(int32(10)),
		Metrics: []*pb.MetricDefinition{
			// Global scalar metric: no per-pod entry in ControlMetrics, falls back to series.LastRaw.Value.
			pb.MetricDefinition_builder{
				Name:  new("cpu_global"),
				Gauge: pb.Gauge_builder{Aggregation: new("Avg")}.Build(),
			}.Build(),
			// Pod-scoped metric: reported in ControlMetrics.pod_metrics.
			pb.MetricDefinition_builder{
				Name:  new("mem_pod"),
				Scope: new("Pod"),
				DecayingDistribution: pb.DecayingDistribution_builder{
					HalfLife:   durationpb.New(time.Hour),
					BucketSize: new("0.1"),
					Percentile: new("p95"),
				}.Build(),
			}.Build(),
			// Global histogram metric: no scalar LastRaw.Value, should render "-" rather than throwing or showing 0.
			pb.MetricDefinition_builder{
				Name: new("lat_global"),
				Distribution: pb.Distribution_builder{
					Percentile:  new("p95"),
					Aggregation: new("Max"),
				}.Build(),
			}.Build(),
			// Zero-valued gauge metric to verify 0 renders as "0" rather than "-".
			pb.MetricDefinition_builder{
				Name:  new("queue_zero"),
				Scope: new("Pod"),
				Gauge: pb.Gauge_builder{Aggregation: new("Avg")}.Build(),
			}.Build(),
		},
		Scaling: []*pb.RecommenderDefinition{
			pb.RecommenderDefinition_builder{Name: new("vpa"), Recommender: new("VPA"), Type: new("VPA")}.Build(),
		},
		RecommenderMetrics: map[string]*pb.MetricDefinitionList{
			"vpa": pb.MetricDefinitionList_builder{Definitions: []*pb.MetricDefinition{
				pb.MetricDefinition_builder{
					Name:            new("cpu_container"),
					RecommenderName: new("vpa"),
					Scope:           new("PodContainer"),
					Gauge:           pb.Gauge_builder{Aggregation: new("Max")}.Build(),
				}.Build(),
			}}.Build(),
		},
	}.Build()
	if _, err := s.UpdatePolicy("default", true, pol); err != nil {
		t.Fatalf("UpdatePolicy() error = %v", err)
	}

	if err := s.UpdateWorkload(pb.UpdateWorkloadRequest_builder{
		Id: id,
		Workload: pb.Workload_builder{
			Pods: []*pb.PodState{
				pb.PodState_builder{Name: new("pod-1"), NodeName: new("node-a"), IsReady: new(true)}.Build(),
				pb.PodState_builder{Name: new("pod-2"), IsReady: new(false)}.Build(),
			},
		}.Build(),
	}.Build()); err != nil {
		t.Fatalf("UpdateWorkload() error = %v", err)
	}

	if err := s.AddBatch(pb.IngestMetricsRequest_builder{
		ClusterName: new("default"),
		Timestamp:   timestamppb.New(start),
		PolicyMetrics: []*pb.PolicyMetricBatch{pb.PolicyMetricBatch_builder{
			Namespace: new("default"),
			Name:      new("web-pol"),
			Batches: []*pb.MetricBatch{pb.MetricBatch_builder{
				PodName:       new("pod-1"),
				ContainerName: new("app"),
				Samples: []*pb.MetricSample{
					pb.MetricSample_builder{
						Name:      new("cpu_global"),
						Gauge:     new(0.75),
						Timestamp: timestamppb.New(start),
					}.Build(),
					pb.MetricSample_builder{
						Name:      new("mem_pod"),
						Gauge:     new(1.25),
						Timestamp: timestamppb.New(start),
					}.Build(),
					pb.MetricSample_builder{
						Name: new("lat_global"),
						Histogram: pb.Histogram_builder{
							Buckets: map[string]uint64{"0.5": 4, "+Inf": 5},
						}.Build(),
						Timestamp: timestamppb.New(start),
					}.Build(),
					pb.MetricSample_builder{
						Name:      new("queue_zero"),
						Gauge:     new(0.0),
						Timestamp: timestamppb.New(start),
					}.Build(),
					pb.MetricSample_builder{
						Name:            new("cpu_container"),
						RecommenderName: new("vpa"),
						Gauge:           new(0.42),
						Timestamp:       timestamppb.New(start),
					}.Build(),
				},
			}.Build()},
		}.Build()},
	}.Build()); err != nil {
		t.Fatalf("AddBatch() error = %v", err)
	}
	s.CalculateAll()

	storeJSON, err := json.Marshal(s.Dump())
	if err != nil {
		t.Fatalf("json.Marshal(Dump()) error = %v", err)
	}

	mux := http.NewServeMux()
	RegisterHandlers(mux, s)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	html := rec.Body.String()

	scriptStart := strings.Index(html, "<script>")
	scriptEnd := strings.Index(html, "</script>")
	if scriptStart < 0 || scriptEnd < 0 || scriptEnd <= scriptStart {
		t.Fatalf("failed to extract <script> from rendered HTML")
	}
	uiScript := html[scriptStart+len("<script>") : scriptEnd]

	harness := fmt.Sprintf(`
const storeData = %s;
function makeEl(tag) {
  return {
    tagName: tag,
    id: '',
    className: '',
    open: false,
    innerHTML: '',
    innerText: '',
    children: [],
    appendChild(c) { this.children.push(c); return c; },
    removeChild(c) { this.children = this.children.filter(x => x !== c); },
    querySelector(sel) {
      if (sel === 'summary') return this.children.find(c => c.tagName === 'summary') || null;
      if (sel === '.details-content') return this.children.find(c => c.className === 'details-content') || null;
      return null;
    }
  };
}
const elementsById = { 'policy-list': makeEl('div') };
globalThis.document = {
  getElementById(id) { return elementsById[id] || null; },
  createElement(tag) {
    const el = makeEl(tag);
    return new Proxy(el, {
      set(target, prop, value) {
        target[prop] = value;
        if (prop === 'id' && value) elementsById[value] = target;
        return true;
      }
    });
  }
};
globalThis.setInterval = () => {};
globalThis.fetch = async () => ({ ok: true, json: async () => storeData });
const origConsoleError = console.error;
let hadError = false;
console.error = (...args) => { hadError = true; origConsoleError(...args); };

%s

(async () => {
  await updateDashboard();
  if (hadError) {
    process.exit(2);
  }
  const ps = Object.values(storeData)[0];
  console.log(renderWorkloadTable(ps));
})();
`, string(storeJSON), uiScript)

	cmd := exec.Command(nodeBin, "-e", harness)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("node execution failed (%v):\n%s", err, string(out))
	}

	rendered := string(out)
	wantSnippets := []string{
		// pod-1 row: cpu_global (0.75 from LastRaw.Value), mem_pod (1.3 from DecayingDistribution bucket 0.1), lat_global (-), queue_zero (0), vpa/cpu_container (app: 0.42)
		"<td>pod-1</td><td><span style=\"font-family: monospace; font-size: 0.85em;\">node-a</span></td><td><span class=\"tag tag-ready\">Ready</span></td><td>0.75</td><td>1.3</td><td>-</td><td>0</td><td>app: 0.42</td>",
		// pod-2 row: no metrics reported, no node_name -> all "-"
		"<td>pod-2</td><td><span style=\"font-family: monospace; font-size: 0.85em;\">-</span></td><td><span class=\"tag tag-not-ready\">Not Ready</span></td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td>",
	}
	for _, want := range wantSnippets {
		if !strings.Contains(rendered, want) {
			t.Errorf("rendered workload table missing %q\nGot:\n%s", want, rendered)
		}
	}
}
