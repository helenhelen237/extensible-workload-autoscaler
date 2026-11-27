package internal

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
	xasv1 "github.com/gke-labs/extensible-workload-autoscaler/api/xas/v1"
	xasgrpc "github.com/gke-labs/extensible-workload-autoscaler/internal/grpc"
	"github.com/gke-labs/extensible-workload-autoscaler/internal/policy"
	listers "github.com/gke-labs/extensible-workload-autoscaler/pkg/client/listers/xas/v1"
)

type SampleState struct {
	Timestamp int64
	Value     float64
}

type CoreNodeMetricsProvider struct {
	kubeClient     kubernetes.Interface
	providerLister listers.MetricProviderClassLister

	grpcConn   *grpc.ClientConn
	grpcClient pb.XASServerClient

	clusterName   string
	nodeName      string
	hostIP        string
	httpClient    *http.Client // For other HTTP needs if any
	kubeletClient *http.Client
	token         string

	// kubelet exports the metrics scraped from the kubelet.
	kubelet *kubeletExporter
}

func NewCoreNodeMetricsProvider(kubeClient kubernetes.Interface, providerLister listers.MetricProviderClassLister, serverAddress, nodeName, hostIP, clusterName string) *CoreNodeMetricsProvider {
	token, _ := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")

	conn, err := grpc.NewClient(serverAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("did not connect", "error", err)
		os.Exit(1)
	}
	client := pb.NewXASServerClient(conn)

	return &CoreNodeMetricsProvider{
		kubeClient:     kubeClient,
		providerLister: providerLister,
		grpcConn:       conn,
		grpcClient:     client,
		clusterName:    clusterName,
		nodeName:       nodeName,
		hostIP:         hostIP,
		token:          string(token),
		httpClient:     &http.Client{Timeout: 5 * time.Second},
		kubeletClient: &http.Client{
			Timeout: 5 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
		},
		kubelet: newKubeletExporter(),
	}
}

func (a *CoreNodeMetricsProvider) Run(ctx context.Context) {
	defer a.grpcConn.Close()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			a.scrapeAndSend()
		case <-ctx.Done():
			return
		}
	}
}

func (a *CoreNodeMetricsProvider) scrapeAndSend() {
	slog.Info("Scrape cycle starting...")

	// 1. Get Policies from Server
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := pb.ListPoliciesRequest_builder{ClusterName: new(a.clusterName)}.Build()
	pols, err := xasgrpc.ListAllPages(ctx, req, a.grpcClient.ListPolicies, (*pb.ListPoliciesResponse).GetPolicies)
	if err != nil {
		slog.Error("Error listing policies", "error", err)
		return
	}

	slog.Info("Found policies", "count", len(pols))

	// 2. Group the metrics of the providers this binary implements by policy.
	var policies []*groupedPolicy
	for _, pol := range pols {
		var relevantMetrics []*pb.MetricDefinition
		for _, m := range policy.MetricDefinitions(pol) {
			class, err := a.providerLister.Get(m.GetProvider())
			if err != nil {
				slog.Warn("MetricProviderClass not found for metric", "class", m.GetProvider(), "metric", m.GetName())
				continue
			}
			if class.Spec.Type == "Kubelet" || class.Spec.Type == "Prometheus" {
				relevantMetrics = append(relevantMetrics, m)
			}
		}
		if len(relevantMetrics) > 0 && pol.GetSelector() != "" {
			policies = append(policies, &groupedPolicy{
				Namespace: pol.GetId().GetNamespace(),
				Name:      pol.GetId().GetName(),
				Selector:  pol.GetSelector(),
				Metrics:   relevantMetrics,
			})
		}
	}

	// 3. List the pods of each policy on this node.
	for _, pol := range policies {
		pol.Pods = a.listPods(pol)
	}

	// 4. Scrape each endpoint once, for all the metrics that read it.
	batches := a.scrapeTargets(a.buildTargets(policies))

	// 5. Send the batches of each policy.
	for _, pol := range policies {
		if len(batches[pol]) == 0 {
			continue
		}
		a.sendBatch(pb.IngestMetricsRequest_builder{
			Timestamp: timestamppb.Now(),
			PolicyMetrics: []*pb.PolicyMetricBatch{
				pb.PolicyMetricBatch_builder{
					Namespace: new(pol.Namespace),
					Name:      new(pol.Name),
					Batches:   batches[pol],
				}.Build(),
			},
		}.Build())
	}
}

type groupedPolicy struct {
	Namespace string
	Name      string
	Selector  string
	Metrics   []*pb.MetricDefinition
	// Pods are the scrapable pods of the policy on this node.
	Pods []corev1.Pod
}

// listPods returns the scrapable pods of a policy on this node.
func (a *CoreNodeMetricsProvider) listPods(pol *groupedPolicy) []corev1.Pod {
	opts := metav1.ListOptions{
		LabelSelector: pol.Selector,
	}
	if a.nodeName != "" {
		opts.FieldSelector = fmt.Sprintf("spec.nodeName=%s", a.nodeName)
	}

	pods, err := a.kubeClient.CoreV1().Pods(pol.Namespace).List(context.TODO(), opts)
	if err != nil {
		slog.Error("Error listing pods", "error", err)
		return nil
	}
	slog.Debug("Found pods for policy", "policy", pol.Name, "count", len(pods.Items))

	var scrapable []corev1.Pod
	for _, pod := range pods.Items {
		if shouldScrape(&pod) {
			scrapable = append(scrapable, pod)
		}
	}
	return scrapable
}

// target is an endpoint to scrape, along with the metrics that read it.
type target struct {
	url      string
	client   *http.Client
	token    string // Bearer token, if the endpoint requires one.
	exporter Exporter
	uses     []metricUse
}

// metricUse is a metric of a policy that reads an endpoint, along with the
// pods of the policy whose samples it reports.
type metricUse struct {
	policy *groupedPolicy
	def    *pb.MetricDefinition
	class  *xasv1.MetricProviderClass
	pods   []corev1.Pod
}

// buildTargets returns the endpoints to scrape for the metrics of the
// policies: the kubelet endpoint(s) of this node and the Prometheus endpoint
// of each pod. An endpoint read by several metrics, of one or several
// policies, is a single target.
func (a *CoreNodeMetricsProvider) buildTargets(policies []*groupedPolicy) []*target {
	var targets []*target
	byURL := make(map[string]*target)
	addUse := func(url string, newTarget func() *target, use metricUse) {
		t, ok := byURL[url]
		if !ok {
			t = newTarget()
			t.url = url
			byURL[url] = t
			targets = append(targets, t)
		}
		t.uses = append(t.uses, use)
	}

	for _, pol := range policies {
		for _, m := range pol.Metrics {
			class, err := a.providerLister.Get(m.GetProvider())
			if err != nil {
				continue
			}

			switch class.Spec.Type {
			case "Kubelet":
				if a.hostIP == "" {
					continue
				}
				url := endpointURL("https", a.hostIP, param(m, class, "port"), param(m, class, "path"), "10250", "/metrics/resource")
				addUse(url, func() *target {
					return &target{client: a.kubeletClient, token: a.token, exporter: a.kubelet}
				}, metricUse{policy: pol, def: m, class: class, pods: pol.Pods})

			case "Prometheus":
				if param(m, class, "metric") == "" {
					continue
				}
				for _, pod := range pol.Pods {
					if pod.Status.PodIP == "" {
						// The pod has no IP yet: there is nothing to scrape.
						continue
					}
					url := endpointURL("http", pod.Status.PodIP, param(m, class, "port"), param(m, class, "path"), "8080", "/metrics")
					addUse(url, func() *target {
						return &target{client: a.httpClient, exporter: prometheusExporter{}}
					}, metricUse{policy: pol, def: m, class: class, pods: []corev1.Pod{pod}})
				}

			default:
				slog.Warn("Unknown provider type for metric", "type", class.Spec.Type, "metric", m.GetName())
			}
		}
	}
	return targets
}

// scrapeTargets scrapes each target once and exports the metrics that read
// it, returning the batches of each policy.
func (a *CoreNodeMetricsProvider) scrapeTargets(targets []*target) map[*groupedPolicy][]*pb.MetricBatch {
	batches := make(map[*groupedPolicy][]*pb.MetricBatch)
	for _, t := range targets {
		families, err := scrapeEndpoint(t.client, t.url, t.token)
		if err != nil {
			slog.Error("Failed to scrape endpoint", "url", t.url, "error", err)
			continue
		}
		for _, u := range t.uses {
			for _, pod := range u.pods {
				batches[u.policy] = append(batches[u.policy], t.exporter.Export(families, pod, u.def, u.class)...)
			}
		}
	}
	a.kubelet.forgetUnseen()
	return batches
}

func (a *CoreNodeMetricsProvider) sendBatch(pbReq *pb.IngestMetricsRequest) {
	pbReq.SetClusterName(a.clusterName)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	slog.Debug("Sending metrics batch", "policies", len(pbReq.GetPolicyMetrics()))
	resp, err := a.grpcClient.IngestMetrics(ctx, pbReq)
	if err != nil {
		slog.Error("Failed to send batch", "error", err)
		return
	}
	slog.Info("Batch sent", "success", resp.GetSuccess())
}

// shouldScrape reports whether the metrics of a pod can be scraped: only a
// running pod has containers to collect metrics from. This is not an
// eligibility decision: whether a scraped sample is eventually considered
// (e.g. depending on the pod's readiness, or whether it is shutting down) is
// decided by the xAS Server, which knows the pod's lifecycle. See
// podutil.IsEligible.
func shouldScrape(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodRunning
}
