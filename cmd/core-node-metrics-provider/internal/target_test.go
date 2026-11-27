package internal

import (
	"net/http"
	"sort"
	"sync/atomic"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
	xasv1 "github.com/gke-labs/extensible-workload-autoscaler/api/xas/v1"
	listers "github.com/gke-labs/extensible-workload-autoscaler/pkg/client/listers/xas/v1"
)

func testClassLister(t *testing.T, classes ...*xasv1.MetricProviderClass) listers.MetricProviderClassLister {
	t.Helper()
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for _, c := range classes {
		if err := indexer.Add(c); err != nil {
			t.Fatalf("adding class: %v", err)
		}
	}
	return listers.NewMetricProviderClassLister(indexer)
}

func testClass(name, typ string, config map[string]string) *xasv1.MetricProviderClass {
	return &xasv1.MetricProviderClass{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       xasv1.MetricProviderClassSpec{Type: typ, Config: config},
	}
}

func testPod(name, ip string) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning, PodIP: ip},
	}
}

func metricDef(name, provider string, params map[string]string) *pb.MetricDefinition {
	return pb.MetricDefinition_builder{Name: new(name), Provider: new(provider), Params: params}.Build()
}

// TestBuildTargets checks that an endpoint read by several metrics, of one or
// several policies, is a single target.
func TestBuildTargets(t *testing.T) {
	p := &CoreNodeMetricsProvider{
		hostIP: "10.0.0.1",
		providerLister: testClassLister(t,
			testClass("kubelet", "Kubelet", map[string]string{"port": "10250", "path": "/metrics/resource"}),
			testClass("prometheus", "Prometheus", map[string]string{"port": "8080", "path": "/metrics"}),
		),
		kubelet: newKubeletExporter(),
	}

	pod1, pod2 := testPod("pod-1", "1.1.1.1"), testPod("pod-2", "2.2.2.2")
	polA := &groupedPolicy{Name: "a", Pods: []corev1.Pod{pod1}, Metrics: []*pb.MetricDefinition{
		metricDef("cpu", "kubelet", map[string]string{"type": "cpu"}),
		metricDef("qps", "prometheus", map[string]string{"metric": "http_requests_total"}),
		metricDef("lat", "prometheus", map[string]string{"metric": "latency_seconds"}),
	}}
	polB := &groupedPolicy{Name: "b", Pods: []corev1.Pod{pod1, pod2}, Metrics: []*pb.MetricDefinition{
		metricDef("mem", "kubelet", map[string]string{"type": "memory"}),
		metricDef("queue", "prometheus", map[string]string{"metric": "queue_size"}),
		metricDef("other-port", "prometheus", map[string]string{"metric": "x", "port": "9090"}),
	}}

	got := make(map[string][]string) // URL -> "policy/metric" of each use
	for _, tg := range p.buildTargets([]*groupedPolicy{polA, polB}) {
		for _, u := range tg.uses {
			got[tg.url] = append(got[tg.url], u.policy.Name+"/"+u.def.GetName())
		}
		sort.Strings(got[tg.url])
	}
	want := map[string][]string{
		"https://10.0.0.1:10250/metrics/resource": {"a/cpu", "b/mem"},
		"http://1.1.1.1:8080/metrics":             {"a/lat", "a/qps", "b/queue"},
		"http://2.2.2.2:8080/metrics":             {"b/queue"},
		"http://1.1.1.1:9090/metrics":             {"b/other-port"},
		"http://2.2.2.2:9090/metrics":             {"b/other-port"},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("buildTargets() mismatch (-want +got):\n%s", diff)
	}
}

type countingTransport struct {
	mockTransport
	requests atomic.Int32
}

func (c *countingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	c.requests.Add(1)
	return c.mockTransport.RoundTrip(req)
}

// TestScrapeOncePerEndpoint checks that an endpoint read by two metrics is
// scraped once, and that each metric gets its own samples.
func TestScrapeOncePerEndpoint(t *testing.T) {
	transport := &countingTransport{mockTransport: mockTransport{body: `# TYPE a counter
a 1
# TYPE b gauge
b 2
`}}
	p := &CoreNodeMetricsProvider{
		httpClient:     &http.Client{Transport: transport},
		providerLister: testClassLister(t, testClass("prometheus", "Prometheus", nil)),
		kubelet:        newKubeletExporter(),
	}
	pol := &groupedPolicy{Name: "a", Pods: []corev1.Pod{testPod("pod-1", "1.1.1.1")}, Metrics: []*pb.MetricDefinition{
		metricDef("metric-a", "prometheus", map[string]string{"metric": "a"}),
		metricDef("metric-b", "prometheus", map[string]string{"metric": "b"}),
	}}

	batches := p.scrapeTargets(p.buildTargets([]*groupedPolicy{pol}))[pol]

	if n := transport.requests.Load(); n != 1 {
		t.Errorf("endpoint scraped %d times, want 1", n)
	}
	var names []string
	for _, b := range batches {
		for _, s := range b.GetSamples() {
			names = append(names, s.GetName())
		}
	}
	sort.Strings(names)
	if diff := cmp.Diff([]string{"metric-a", "metric-b"}, names); diff != "" {
		t.Errorf("exported samples mismatch (-want +got):\n%s", diff)
	}
}
