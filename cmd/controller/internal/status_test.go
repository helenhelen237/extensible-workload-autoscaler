package internal

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/types/known/timestamppb"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/server/v1alpha"
	xasv1 "github.com/gke-labs/extensible-workload-autoscaler/api/xas/v1"
	fakeclientset "github.com/gke-labs/extensible-workload-autoscaler/pkg/client/clientset/versioned/fake"
)

func TestUpdateStatus(t *testing.T) {
	policy := &xasv1.ScalingPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "web"},
		Spec: xasv1.ScalingPolicySpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       "web",
			},
			MaxReplicas: 10,
		},
	}

	// Before any status update, marshaling the policy omits the zero status so
	// required status fields like lastUpdated are never serialized as null.
	rawBefore, err := json.Marshal(policy)
	if err != nil {
		t.Fatalf("json.Marshal(policy) error = %v", err)
	}
	if strings.Contains(string(rawBefore), `"status"`) {
		t.Errorf("expected zero status to be omitted from JSON, got %s", rawBefore)
	}

	client := fakeclientset.NewSimpleClientset(policy)
	c := &Controller{xasclientset: client}

	scale := &autoscalingv1.Scale{
		Spec: autoscalingv1.ScaleSpec{Replicas: 3},
	}
	metricTime := time.Date(2026, 10, 15, 12, 30, 0, 0, time.UTC)
	replicas := int32(5)
	resp := pb.ArbitratedRecommendation_builder{
		Recommendation: pb.Recommendation_builder{
			Replicas: new(replicas),
			IsActive: new(true),
		}.Build(),
		Explanation: []*pb.RecommenderStatus{
			pb.RecommenderStatus_builder{
				Name:     new("vpa-sizing"),
				Type:     new("VPA"),
				Phase:    new("Scaling"),
				Mode:     new("Active"),
				IsActive: new(true),
				Message:  new("target applied"),
				Replicas: new(replicas),
				WorkloadResources: []*pb.ContainerResource{
					pb.ContainerResource_builder{
						ContainerName: new("app"),
						Requests:      map[string]string{"cpu": "250m", "memory": "512Mi"},
						Limits:        map[string]string{"cpu": "1", "memory": "1Gi"},
					}.Build(),
				},
				PodResources: []*pb.PodContainerResource{
					pb.PodContainerResource_builder{
						PodName: new("web-0"),
						ContainerResources: pb.ContainerResource_builder{
							ContainerName: new("app"),
							Requests:      map[string]string{"cpu": "300m"},
						}.Build(),
					}.Build(),
				},
			}.Build(),
		},
		MetricStatuses: []*pb.MetricStatus{
			pb.MetricStatus_builder{
				Name:      new("cpu_usage"),
				Value:     new(0.42),
				Timestamp: timestamppb.New(metricTime),
			}.Build(),
		},
	}.Build()

	before := time.Now().Truncate(time.Second)
	if err := c.updateStatus(policy, scale, "app=web", resp); err != nil {
		t.Fatalf("updateStatus() error = %v", err)
	}
	after := time.Now().Add(time.Second)

	got, err := client.XASV1().ScalingPolicies("prod").Get(context.Background(), "web", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if got.Status.LastUpdated.Time.Before(before) || got.Status.LastUpdated.Time.After(after) {
		t.Errorf("Status.LastUpdated = %v, want between %v and %v", got.Status.LastUpdated.Time, before, after)
	}

	wantStatus := xasv1.ScalingPolicyStatus{
		CurrentReplicas: 3,
		DesiredReplicas: 5,
		Selector:        "app=web",
		LastUpdated:     got.Status.LastUpdated,
		Recommendations: []xasv1.RecommendationStatus{
			{
				RecommenderName: "vpa-sizing",
				Type:            "VPA",
				Phase:           "Scaling",
				Mode:            "Active",
				Active:          true,
				Replicas:        &replicas,
				Reason:          "target applied",
				WorkloadResources: []xasv1.ResourceRecommendation{
					{
						ContainerName: "app",
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("250m"),
							corev1.ResourceMemory: resource.MustParse("512Mi"),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("1"),
							corev1.ResourceMemory: resource.MustParse("1Gi"),
						},
					},
				},
				PodResources: []xasv1.PodResourceRecommendation{
					{
						PodName: "web-0",
						Resources: &xasv1.ResourceRecommendation{
							ContainerName: "app",
							Requests: corev1.ResourceList{
								corev1.ResourceCPU: resource.MustParse("300m"),
							},
						},
					},
				},
			},
		},
		MetricStatuses: []xasv1.MetricStatus{
			{
				Name:        "cpu_usage",
				LastUpdated: metav1.NewTime(metricTime),
				Value:       "0.42",
			},
		},
	}

	quantityComparer := cmp.Comparer(func(a, b resource.Quantity) bool {
		return a.Equal(b)
	})
	if diff := cmp.Diff(wantStatus, got.Status, quantityComparer); diff != "" {
		t.Errorf("Status mismatch (-want +got):\n%s", diff)
	}
}
