package podutil

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
)

func TestIsEligible(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		l    Lifecycle
		want bool
	}{
		{
			name: "ready",
			l:    Lifecycle{Phase: corev1.PodRunning, StartTime: start, Ready: true, ReadyLastTransitionTime: start.Add(2 * time.Minute)},
			want: true,
		},
		{
			name: "ready without lifecycle facts",
			l:    Lifecycle{Ready: true},
			want: true,
		},
		{
			name: "starting up, Ready condition false since start",
			l:    Lifecycle{Phase: corev1.PodRunning, StartTime: start, ReadyLastTransitionTime: start.Add(time.Second)},
			want: false,
		},
		{
			name: "unready later in its life",
			l:    Lifecycle{Phase: corev1.PodRunning, StartTime: start, ReadyLastTransitionTime: start.Add(20 * time.Minute)},
			want: true,
		},
		{
			name: "Ready condition last changed exactly at the end of the initial readiness delay",
			l:    Lifecycle{Phase: corev1.PodRunning, StartTime: start, ReadyLastTransitionTime: start.Add(InitialReadinessDelay)},
			want: true,
		},
		{
			name: "Ready condition last changed just before the end of the initial readiness delay",
			l:    Lifecycle{Phase: corev1.PodRunning, StartTime: start, ReadyLastTransitionTime: start.Add(InitialReadinessDelay - time.Second)},
			want: false,
		},
		{
			name: "pending",
			l:    Lifecycle{Phase: corev1.PodPending},
			want: false,
		},
		{
			name: "running without start time",
			l:    Lifecycle{Phase: corev1.PodRunning, ReadyLastTransitionTime: start.Add(20 * time.Minute)},
			want: false,
		},
		{
			name: "running without Ready condition",
			l:    Lifecycle{Phase: corev1.PodRunning, StartTime: start},
			want: false,
		},
		{
			name: "succeeded",
			l:    Lifecycle{Phase: corev1.PodSucceeded, StartTime: start, ReadyLastTransitionTime: start.Add(20 * time.Minute)},
			want: false,
		},
		{
			name: "failed",
			l:    Lifecycle{Phase: corev1.PodFailed, StartTime: start, ReadyLastTransitionTime: start.Add(20 * time.Minute)},
			want: false,
		},
		{
			name: "terminating while still ready",
			l:    Lifecycle{Phase: corev1.PodRunning, StartTime: start, Ready: true, ReadyLastTransitionTime: start.Add(2 * time.Minute), DeletionTimestamp: start.Add(time.Hour)},
			want: false,
		},
		{
			name: "terminating, unready later in its life",
			l:    Lifecycle{Phase: corev1.PodRunning, StartTime: start, ReadyLastTransitionTime: start.Add(20 * time.Minute), DeletionTimestamp: start.Add(time.Hour)},
			want: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsEligible(tc.l); got != tc.want {
				t.Errorf("IsEligible(%+v) = %v, want %v", tc.l, got, tc.want)
			}
		})
	}
}

func TestIsCPUSampleEligible(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ready := start.Add(time.Minute)
	// Lifecycle of a pod that became ready one minute after it started.
	readyPod := Lifecycle{Phase: corev1.PodRunning, StartTime: start, Ready: true, ReadyLastTransitionTime: ready}
	unreadyPod := Lifecycle{Phase: corev1.PodRunning, StartTime: start, Ready: false, ReadyLastTransitionTime: ready}
	tests := []struct {
		name         string
		l            Lifecycle
		measuredFrom time.Time
		now          time.Time
		want         bool
	}{
		{
			name:         "initializing: sample measured from the readiness transition",
			l:            readyPod,
			measuredFrom: ready,
			now:          start.Add(2 * time.Minute),
			want:         true,
		},
		{
			name:         "initializing: sample measured from before the pod became ready",
			l:            readyPod,
			measuredFrom: ready.Add(-time.Second),
			now:          start.Add(2 * time.Minute),
			want:         false,
		},
		{
			name:         "initializing: pod not ready",
			l:            unreadyPod,
			measuredFrom: ready.Add(10 * time.Minute),
			now:          start.Add(2 * time.Minute),
			want:         false,
		},
		{
			name:         "past the initialization period: sample counts even if measured from before readiness",
			l:            readyPod,
			measuredFrom: ready.Add(-time.Second),
			now:          start.Add(CPUInitializationPeriod),
			want:         true,
		},
		{
			name:         "past the initialization period: unready pod counts",
			l:            unreadyPod,
			measuredFrom: ready.Add(10 * time.Minute),
			now:          start.Add(10 * time.Minute),
			want:         true,
		},
		{
			name:         "no start time: not acknowledged by the kubelet yet",
			l:            Lifecycle{Ready: true},
			measuredFrom: ready,
			now:          ready,
			want:         false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsCPUSampleEligible(tc.l, tc.measuredFrom, tc.now); got != tc.want {
				t.Errorf("IsCPUSampleEligible() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestReadyCondition(t *testing.T) {
	transition := time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC)
	pod := &corev1.Pod{
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
				{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Time{Time: transition}},
			},
		},
	}
	cond := ReadyCondition(pod)
	if cond == nil || cond.Type != corev1.PodReady || !cond.LastTransitionTime.Time.Equal(transition) {
		t.Errorf("ReadyCondition() = %+v, want the Ready condition", cond)
	}

	if cond := ReadyCondition(&corev1.Pod{}); cond != nil {
		t.Errorf("ReadyCondition(no conditions) = %+v, want nil", cond)
	}
}

func TestFromPodState(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	transition := start.Add(time.Minute)
	p := pb.PodState_builder{
		Phase:                   proto.String("Running"),
		IsReady:                 proto.Bool(true),
		StartTime:               timestamppb.New(start),
		ReadyLastTransitionTime: timestamppb.New(transition),
		DeletionTimestamp:       timestamppb.New(transition.Add(time.Hour)),
	}.Build()
	want := Lifecycle{Phase: corev1.PodRunning, StartTime: start, Ready: true, ReadyLastTransitionTime: transition, DeletionTimestamp: transition.Add(time.Hour)}
	if got := FromPodState(p); got != want {
		t.Errorf("FromPodState() = %+v, want %+v", got, want)
	}

	if got := FromPodState(&pb.PodState{}); got != (Lifecycle{}) {
		t.Errorf("FromPodState(empty) = %+v, want zero", got)
	}
}
