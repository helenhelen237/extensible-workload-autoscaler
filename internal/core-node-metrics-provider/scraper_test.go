package core_node_metrics_provider

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestShouldScrape checks that every running pod is scraped whatever its
// readiness: the xAS Server decides which samples are considered.
func TestShouldScrape(t *testing.T) {
	now := metav1.Now()
	tests := []struct {
		name string
		pod  corev1.Pod
		want bool
	}{
		{
			name: "running and ready",
			pod:  corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}},
			want: true,
		},
		{
			name: "running, not ready",
			pod:  corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}}},
			want: true,
		},
		{
			name: "pending",
			pod:  corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodPending}},
			want: false,
		},
		{
			name: "succeeded",
			pod:  corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodSucceeded}},
			want: false,
		},
		{
			name: "terminating",
			pod:  corev1.Pod{ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: &now}, Status: corev1.PodStatus{Phase: corev1.PodRunning}},
			want: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldScrape(&tc.pod); got != tc.want {
				t.Errorf("shouldScrape() = %v, want %v", got, tc.want)
			}
		})
	}
}
