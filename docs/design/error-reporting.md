# Error reporting for recommenders & metric providers

**Status:** Approved (experiment)
**Issue:** [gke-labs/extensible-workload-autoscaler#46](https://github.com/gke-labs/extensible-workload-autoscaler/issues/46)
**Last updated:** 2026-11-18

# Objective

Make recommender failures visible on the `ScalingPolicy` so a policy author can see
*which layer* is broken (configuration, metrics, recommendations) and *why*, without
reading component logs.

# Background

Code facts that constrain the design:

- The node metrics provider is a DaemonSet (`deploy/xas-core-node-metrics-provider.yaml`):
  N instances push for the same policy/metric every cycle. The cluster provider is a single Deployment.
- Recommenders run only when a `WatchControlMetrics` update arrives
  (`internal/recommenders/engine/engine.go`). An `UpdateRecommenderStateRequest`
  **without** a recommendation means "clear this recommender's state"
  (`internal/server/store/memory.go`).
- Nothing calls `recover()`. A plugin panic crashes the process.
- The CRD already has unused `MetricStatus.Error` / `RecommendationStatus.Error` string
  fields (`pkg/apis/xas/v1/types.go`). It has no `Conditions`, no `EventRecorder`, and no `events` RBAC.
- `MetricStatus.timestamp` is the calculation time of the whole `ControlMetrics`, not one per metric.
- `metav1.Condition.Reason` must match `^[A-Za-z]([A-Za-z0-9_,:]*[A-Za-z0-9_])?$`.

# Detailed Design

## Summary of decisions

| # | Topic | Decision |
|---|---|---|
| R1 | Error shape | `code` enum + deterministic `message` |
| R2 | Reported vs. logged | Report what the policy author can act on, plus `INTERNAL`. Anything not tied to a policy is only logged |
| R3 | Recommender transport | `oneof { recommendation, errors }` on `UpdateRecommenderStateRequest` |
| R4 | Metric-provider transport | **Providers don't report; log only.** The ingestion path doesn't change |
| R5 | Arbitration | A recommender with errors counts as absent. Phase-aware: in activation, keep the last state if *all* activation recommenders have errors |
| R6 | Clearing | Replace per recommender on each update |
| S1 | Display | Per-recommender `errors` list + `status.conditions`. Events deferred |
| S2 | Conditions | `ValidConfiguration`, `MetricsAvailable`, `RecommendationAvailable`, normal-true |
| S3 | Derivation | From codes + presence + age. Strict on partial metric failure. Fixed staleness constants |
| S5 | Churn | Semantic diff before `UpdateStatus` |

## API changes

### gRPC (`api/proto/v1alpha/xas.proto`)

```proto
message Error {
  enum Code {
    CODE_UNSPECIFIED      = 0;
    INVALID_CONFIGURATION = 1;  // user-fixable: bad/missing params, unknown class, bad cron, wrong metric scope, metric not defined
    MISSING_DATA          = 2;  // input not there yet: metric has no value, no pods/containers reported
    INTERNAL              = 3;  // plugin failed unexpectedly; not user-fixable, reported so the owner can escalate
  }
  Code code = 1;
  // Deterministic for a given failure: no timestamps, counters or pod names.
  string message = 2;
}

message Errors {
  repeated Error errors = 1;
}

message UpdateRecommenderStateRequest {
  PolicyId id = 1;
  string recommender_name = 2;
  // Neither set = clear this recommender's state (unchanged semantics).
  oneof result {
    Recommendation recommendation = 3;
    Errors errors = 4;
  }
}

message RecommenderStatus {
  // ... existing fields ...
  repeated Error errors = 11;
}
```

The metric-provider messages (`IngestMetricsRequest`, `PolicyBatch`, `MetricBatch`,
`MetricSample`) and `MetricStatus` **don't change**.

### CRD (`pkg/apis/xas/v1/types.go`)

```go
type ScalingPolicyStatus struct {
    // ... existing fields ...

    // NEW: ValidConfiguration, MetricsAvailable, RecommendationAvailable.
    // +listType=map
    // +listMapKey=type
    Conditions []metav1.Condition `json:"conditions,omitempty"`
}

type RecommendationStatus struct {
    // ... existing fields ...

    // Replaces `Error string` (which was never populated).
    Errors []ErrorStatus `json:"errors,omitempty"`
}

// NEW
type ErrorStatus struct {
    Code    string `json:"code"`    // enum name, e.g. "INVALID_CONFIGURATION"
    Message string `json:"message"`
}
```

`MetricStatus.Error` stays as is (unused). With R4 = E nothing produces metric-level errors.

## Flow

```mermaid
sequenceDiagram
    participant P as Metric provider
    participant E as Recommender engine
    participant S as xAS server
    participant C as Controller
    participant K as ScalingPolicy status

    P->>S: IngestMetrics (unchanged; failures are only logged)
    S-->>E: WatchControlMetrics update
    E->>E: Recommend() returns (rec, errs) per recommender
    E->>S: UpdateRecommenderState{oneof: recommendation | errors}
    Note over S: errors go to RecommenderStatus.errors, no recommendation stored<br/>arbitration uses only recommenders that have a recommendation (R5)
    C->>S: GetRecommendation
    S-->>C: recommendation?, explanation[] (with errors), metric_statuses[]
    C->>C: derive 3 conditions (codes + presence + age)<br/>semantic diff vs current status
    C->>K: UpdateStatus (only if changed)
```

## Part I: Reporting

### R1. Error codes

The codes are coarse and grouped by who can fix the error, so they map onto the conditions.
Details go in `message`. Absence-type failures (recommender never invoked, engine down,
stale state, provider not deployed) get **no** code. The controller detects them from
the presence and age of state and shows them as condition reasons (`NoData`, `Stale`,
`NoRecommendation`).

There's no `recover()` around `Recommend()`. `INTERNAL` is for errors a plugin chooses to return.

### R2. What is reported vs. logged

| Item | Decision |
|---|---|
| Unknown `RecommenderClass` | Report `INVALID_CONFIGURATION` for that recommender and keep processing the policy (today the whole policy is aborted) |
| One recommender's owned-metric config is invalid (`syncRecommenderMetrics`) | Report `INVALID_CONFIGURATION` for that recommender and continue (today the whole policy is dropped) |
| Metric referenced but not defined, vs. defined but no value yet (both are "metric not found" today, e.g. `internal/recommenders/linear/linear.go`) | Split: `INVALID_CONFIGURATION` vs. `MISSING_DATA` |
| Metric defined with the wrong `scope` | `INVALID_CONFIGURATION` |
| Engine `fetchWorkload` fails | Log only (transient) |
| Any metric-provider failure (bad selector, missing param, unknown `MetricProviderClass`, metrics API/kubelet/Prometheus failure) | Log only (R4) |
| A component can't reach the xAS server, or its push is rejected | Log only |

### R3. Recommender transport

`Recommend()` returns `(*pb.Recommendation, []*pb.Error)`. The engine sends whichever one is
non-empty, and errors win if both are. The server copies errors into
`RecommenderStatus.errors` and drops any stored recommendation for that recommender.
Partial results with warnings (e.g. VPA-style) put the warnings in
`Recommendation.message`, not in `errors`.

### R4. Metric providers: log only

Providers don't report errors to the server. When a metric has a problem, it shows on the
policy as `MetricsAvailable=False` with reason `NoData` or `Stale`. The *cause* is only in
provider logs, and `ValidConfiguration` covers only references and recommender definitions.
This keeps the ingestion API unchanged and avoids DaemonSet fan-in (N identical errors
per cycle) and the per-reporter identity, replacement and TTL machinery that comes with it.
If we add provider reporting later, `PolicyBatch.errors` plus a reporter identity is the
preferred route.

### R5. Arbitration with errored recommenders

- A recommender that reports errors has no stored recommendation, so arbitration ignores it.
  Behavior change: an activation recommender that fails no longer votes "inactive".
- **Scaling phase:** unchanged. The other recommenders decide, and if none remain, replicas
  aren't touched.
- **Activation phase:** if **every** non-dry-run activation recommender has errors, keep
  the last activation state instead of computing `isActive=false` (which would scale to zero
  after `window`). If some are healthy, use the OR of the healthy ones. "Last activation state"
  is in memory for now; surviving restarts is a follow-up.

### R6. Clearing

Each recommender is the only reporter of its own state, so every `UpdateRecommenderState`
replaces that recommender's errors (or recommendation). No TTL purge is needed: a
recommender that stops updating shows up as `Stale` (S3c).

## Part II: Displaying

### S1. Mechanisms

- `status.recommendations[].errors`: a list of `{code, message}`.
- `status.conditions`: three `metav1.Condition`s, set with `meta.SetStatusCondition` so
  `lastTransitionTime` only changes on a transition.
- Events: deferred (would need an `EventRecorder` and `events` RBAC).

### S2. Conditions (normal-true)

`ValidConfiguration`, `MetricsAvailable`, `RecommendationAvailable`. All are `True` when healthy.
On recovery they flip back to `True`, and `lastTransitionTime` then means "healthy since". This works with
`kubectl wait --for=condition=MetricsAvailable`.

### S3. Derivation

Inputs after `GetRecommendation`: per-metric `{value?, timestamp}`, per-recommender
`{last_updated, errors, mode}`, the arbitrated `recommendation?`, the policy spec, and the
controller's own reference validation.

| Condition | False when | Reasons |
|---|---|---|
| `ValidConfiguration` | controller `validateReferences` fails, **or** any recommender carries `INVALID_CONFIGURATION` | `InvalidReference`, `InvalidRecommenderDefinition`; `Valid` when True |
| `MetricsAvailable` | **any** defined metric has no value, **or** its last sample is older than the threshold (strict) | `NoData`, `Stale`; `Available` when True |
| `RecommendationAvailable` | no non-dry-run recommender has a fresh, error-free status | from codes: `InvalidConfiguration`, `MissingData`, `Internal`; absence-based: `NoRecommendation`, `Stale`, `AllDryRun`; `Available` when True |

Notes:
- Code change: the controller must write status **before** its early `return` when
  validation fails.
- Code change: per-metric `Stale` needs the store to expose a last-sample time for each
  metric. Today only a policy-wide timestamp exists.
- Conditions overlap: a recommender with a config error makes both `ValidConfiguration`
  and `RecommendationAvailable` False (Deployment `Available`/`Progressing` work the same way).
- Staleness thresholds are fixed constants per condition, defined in one place in the
  controller. This is the controller's first clock dependency, so inject the clock for tests.
- Messages name the item, e.g. `recommender "nightly": invalid cron "0 25 * * *"`. If there
  are several, the message lists the first and a count.

### S4. Events: deferred

### S5. Status-update churn

The controller compares the new status with the current one before `UpdateStatus` and
ignores `lastUpdated`/timestamps. Error producers must follow these rules (S6):

- `Error.message` is the same for the same failure: no timestamps, counters or pod names.
  Per-pod detail goes to logs.
- Bounded: at most 10 errors per recommender and 256 characters per message. The server
  truncates longer ones.

## What the user sees

Healthy:

```yaml
status:
  conditions:
  - {type: ValidConfiguration, status: "True", reason: Valid}
  - {type: MetricsAvailable, status: "True", reason: Available}
  - {type: RecommendationAvailable, status: "True", reason: Available}
  recommendations:
  - {recommenderName: linear, phase: Scaling, active: true, replicas: 3}
```

Metric with a bad selector (provider logs it):

```yaml
  conditions:
  - {type: ValidConfiguration, status: "True", reason: Valid}
  - {type: MetricsAvailable, status: "False", reason: NoData, message: 'metric "qps" has no value'}
  - {type: RecommendationAvailable, status: "False", reason: MissingData,
     message: 'recommender "linear": metric "qps" has no value'}
  recommendations:
  - recommenderName: linear
    errors:
    - {code: MISSING_DATA, message: 'metric "qps" has no value'}
```

Recommender misconfigured:

```yaml
  conditions:
  - {type: ValidConfiguration, status: "False", reason: InvalidRecommenderDefinition,
     message: 'recommender "nightly": invalid cron "0 25 * * *"'}
  - {type: MetricsAvailable, status: "True", reason: Available}
  - {type: RecommendationAvailable, status: "True", reason: Available}   # healthy "linear" still drives scaling
  recommendations:
  - recommenderName: nightly
    phase: Activation
    errors:
    - {code: INVALID_CONFIGURATION, message: 'invalid cron "0 25 * * *"'}
  - {recommenderName: linear, phase: Scaling, active: true, replicas: 3}
```

Engine down / stale:

```yaml
  conditions:
  - {type: RecommendationAvailable, status: "False", reason: Stale,
     message: 'no recommendation newer than 2m0s'}
```

# Alternatives considered

- **Provider error reporting** via `PolicyBatch.errors` with a reporter identity, per-reporter
  replacement and a TTL backstop. It shows the *cause* of metric failures on the policy, but
  needs DaemonSet dedup and changes to the ingestion API. Deferred.
- **Error shape:** a free-text `reason` field, or no enum at all. Rejected because a free-text
  reason can't safely become a condition reason, and with no enum nothing is machine-readable.
- **gRPC status codes:** RPC semantics don't fit autoscaling inputs.
- **`errors` next to `recommendation`** (instead of `oneof`): it would force a rule for
  an update that carries both.
- **Errored recommender freezes all arbitration:** one broken recommender would block the healthy ones.
- **A single `Ready` condition, or abnormal-true conditions:** one reason for many causes,
  and there would be no "healthy since" time.

# Test plan

- **Unit tests:** engine error paths per R2 row, server store `oneof` handling and
  arbitration (R5, including activation hold), controller condition derivation table tests
  with a fake clock, semantic-diff no-op updates, and `make verify`.
- **End-to-end on kind:** `hack/create-kind-cluster.sh` and `hack/deploy.sh`, then apply
  `ScalingPolicy` objects for the "What the user sees" cases (healthy, metric with a bad
  selector, recommender with an invalid cron, engine scaled to zero) and check
  `status.conditions` and `status.recommendations[].errors` with `kubectl get -o yaml` and
  `kubectl wait --for=condition=…`. Tear the cluster down afterwards.
