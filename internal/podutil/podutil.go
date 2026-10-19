// Package podutil decides whether the metrics of a pod should be considered
// for autoscaling, based on the pod's lifecycle.
package podutil

import (
	"time"

	corev1 "k8s.io/api/core/v1"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
)

// InitialReadinessDelay is the period after a pod starts during which changes
// of its Ready condition are treated as part of its initialization. It matches
// the default of the HPA's --horizontal-pod-autoscaler-initial-readiness-delay.
const InitialReadinessDelay = 30 * time.Second

// CPUInitializationPeriod is the period after a pod starts during which its
// CPU usage is only considered once it is ready and a full sample has been
// measured since it became ready. It matches the default of the HPA's
// --horizontal-pod-autoscaler-cpu-initialization-period.
const CPUInitializationPeriod = 5 * time.Minute

// Lifecycle holds the facts about a pod's lifecycle needed to decide whether
// its metrics are eligible.
type Lifecycle struct {
	// Phase is the pod phase.
	Phase corev1.PodPhase
	// StartTime is when the pod was acknowledged by the kubelet. Zero if unset.
	StartTime time.Time
	// Ready reports whether the pod's Ready condition is currently true.
	Ready bool
	// ReadyLastTransitionTime is when the pod's Ready condition last changed.
	// Zero if the pod has no Ready condition.
	ReadyLastTransitionTime time.Time
	// DeletionTimestamp is when the pod's deletion was requested. Zero if it
	// wasn't.
	DeletionTimestamp time.Time
}

// IsEligible reports whether the metrics of a pod in the given lifecycle state
// should be considered.
//
// A pod whose deletion was requested is never eligible, even while it is
// still ready.
// Otherwise a ready pod is eligible. A pod that isn't ready is eligible only
// if it is running and has been ready before, i.e. it became unready later in
// its life (e.g. its readiness probe fails under load). Pods that are pending,
// have terminated, or have never been ready are not eligible.
//
// The Ready condition only records its last transition, so whether the pod has
// been ready before is inferred as in the HPA: a pod whose Ready condition last
// changed within InitialReadinessDelay of its start has never been ready.
func IsEligible(l Lifecycle) bool {
	if !l.DeletionTimestamp.IsZero() {
		return false
	}
	if l.Ready {
		return true
	}
	if l.Phase != corev1.PodRunning || l.StartTime.IsZero() || l.ReadyLastTransitionTime.IsZero() {
		return false
	}
	return !l.ReadyLastTransitionTime.Before(l.StartTime.Add(InitialReadinessDelay))
}

// IsCPUSampleEligible reports whether a CPU usage sample of an eligible pod
// should be considered. CPU usage is typically high and erratic while a pod
// starts, so during CPUInitializationPeriod after the pod start, a sample only
// counts if the pod is ready and the sample was entirely measured after the
// pod became ready, as in the HPA.
//
// measuredFrom is the start of the interval the sample was measured over (the
// sample's own timestamp for an instantaneous value). now is the time of the
// evaluation.
func IsCPUSampleEligible(l Lifecycle, measuredFrom, now time.Time) bool {
	// Without a start time, the pod has not been acknowledged by the kubelet
	// yet, so it cannot have a meaningful CPU sample.
	if l.StartTime.IsZero() {
		return false
	}
	// Past the initialization period: the sample counts whatever the pod's
	// readiness. A pod that became unready later in its life keeps counting
	// (see IsEligible).
	if !now.Before(l.StartTime.Add(CPUInitializationPeriod)) {
		return true
	}
	// Within the initialization period: the pod must be ready, and the sample
	// interval must start at or after the last readiness transition. Otherwise
	// part of the measured CPU dates from before the pod was ready.
	return l.Ready && !measuredFrom.Before(l.ReadyLastTransitionTime)
}

// ReadyCondition returns the pod's Ready condition, or nil if it has none.
func ReadyCondition(pod *corev1.Pod) *corev1.PodCondition {
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == corev1.PodReady {
			return &pod.Status.Conditions[i]
		}
	}
	return nil
}

// FromPodState returns the lifecycle state of a pod as reported to the Server.
func FromPodState(p *pb.PodState) Lifecycle {
	l := Lifecycle{
		Phase: corev1.PodPhase(p.GetPhase()),
		Ready: p.GetIsReady(),
	}
	if p.GetDeletionTimestamp() != nil {
		l.DeletionTimestamp = p.GetDeletionTimestamp().AsTime()
	}
	if p.GetStartTime() != nil {
		l.StartTime = p.GetStartTime().AsTime()
	}
	if p.GetReadyLastTransitionTime() != nil {
		l.ReadyLastTransitionTime = p.GetReadyLastTransitionTime().AsTime()
	}
	return l
}
