# Fallow

An idle-workload reclamation controller for Kubernetes.

Clusters accumulate workloads nobody uses: the preview environment from a merged PR, the
internal dashboard two people bookmarked in 2023, the batch service whose upstream was
switched off months ago. They cost money and nobody deletes them, because nobody is
certain they are really dead.

Fallow makes that certainty someone else's problem. It watches enrolled namespaces, and
when a workload has been idle long enough, it escalates one careful step at a time —
warning first, scaling down second, deleting last — and every step before the last is
reversible in one reconcile.

```
  idle 72h            +24h                +168h
     │                  │                    │
  ┌──▼───────┐    ┌─────▼────────┐    ┌──────▼──────┐
  │ Notified │───▶│ ScaledToZero │───▶│   Deleted   │
  └──────────┘    └──────────────┘    └─────────────┘
   annotation      replicas → 0        manifest → ConfigMap,
   + event         (count saved)       then removed
     │                  │                    │
     └──────────────────┴────────────────────┘
       any sign of life → back to Active, at the original size
```

## See it work

```console
$ make demo
```

No cluster needed. The demo runs the real reconciler against an in-memory API server and a
fake clock, so a 74-hour escalation prints in milliseconds — including a workload that
comes back to life mid-escalation and one that has to be recovered from its archive.

```
Hour 24 -- scaled to zero

  events:
    ScaledToZero team-a/checkout-api: scaled to 0 replicas (was 3) after 24h0m0s idle
    ScaledToZero team-a/data-explorer: scaled to 0 replicas (was 2) after 24h0m0s idle

  NAMESPACE  WORKLOAD        REPLICAS  STAGE         NOTE
  team-a     checkout-api    0         ScaledToZero  will restore to 3 replicas
  team-a     data-explorer   0         ScaledToZero  will restore to 2 replicas
  team-a     billing-worker  2         Active        opted out with fallow.dev/exclude
```

## A policy

```yaml
apiVersion: fallow.dev/v1alpha1
kind: ReclaimPolicy
metadata:
  name: dev-reclaim
spec:
  namespaceSelector:
    matchLabels:
      fallow.dev/reclaim: enabled   # enrollment is opt-in, per namespace
  idleAfter: 72h
  stages:
    notify:
      message: "Idle for three days. Scaling to zero in 24h."
    scaleToZero:
      after: 24h
    delete:
      after: 168h
      archive: true                 # default; without it, deletion is permanent
  dryRun: true                      # report everything, change nothing
```

Stage deadlines are cumulative from the moment the workload was first seen idle, so this
policy notifies at 72h, scales to zero at 96h, and deletes at 264h.

More in [config/samples/](config/samples/): a dry-run starter, a notify-only policy, an
aggressive one for preview environments, and [what enrolling a namespace looks
like](config/samples/enrollment_example.yaml).

## Why it is safe to run

Reclamation is disruptive, so nearly every design decision here resolves toward doing
nothing:

**Nothing is enrolled by default.** A policy with no `namespaceSelector` governs nothing
and says so in its status. Installing Fallow on a cluster changes nothing until a namespace
opts in by label.

**A workload with no instrumentation is never idle.** Idleness has to be positively
established. A Deployment with no activity signal reads as *unknown*, and unknown is
treated as busy — so enrolling a namespace cannot reclaim workloads nobody has wired
signals for yet.

**Signals must agree unanimously.** The default detector is a conservative AND across every
configured source. One source still seeing traffic keeps the workload alive.

**A broken signal votes "busy".** A source that errors — a metrics scrape timing out, a
backend down — is counted as evidence of *activity*, never of idleness. A failed metrics
pipeline cannot cause a cluster-wide scale-down.

**Escalation climbs one rung per pass.** A controller that was down for a week comes back
to find every deadline expired. It still walks each workload down the ladder in order
rather than jumping to deletion, so every transition is announced and the replica count is
captured before anything is removed.

**Deletion is refused if it cannot be made reversible.** The manifest goes to a ConfigMap
first; if that write fails, the delete does not happen. Reversibility is a precondition,
not a best-effort extra.

**Cluster infrastructure is untouchable.** `kube-*` namespaces are refused regardless of
what any selector matches, as is Fallow's own namespace — a policy that could scale down
the controller mid-escalation would strand every workload at whatever rung it had reached.

**Owners can always opt out.** `fallow.dev/exclude: "true"` on a workload takes it out of
scope entirely, whatever any policy says.

**`dryRun: true` writes nothing to any workload.** It reports the full escalation — every
event, every stage transition, the ConfigMap name it would have used — while tracking
progress in the policy's own status. Run it for a week and read the status before turning
it on.

## How a workload's state is stored

Everything needed to reverse an action lives on the workload itself, not in controller
memory:

| Annotation | Meaning |
|---|---|
| `fallow.dev/stage` | Current rung: `Active`, `Notified`, `ScaledToZero`, `Deleted` |
| `fallow.dev/idle-since` | When it was first seen idle (RFC 3339) — every deadline derives from this |
| `fallow.dev/original-replicas` | The count to restore to |
| `fallow.dev/policy` | Which policy claimed it; a second policy will not touch a claimed workload |
| `fallow.dev/archived-in` | On a restored manifest: the ConfigMap it came from |
| `fallow.dev/exclude` | Set by owners to opt out |

A restart therefore loses nothing, and a workload's position on the ladder can be read with
`kubectl describe` rather than inferred from logs.

## Recovering a deleted workload

The archive is a ConfigMap in the same namespace, under the same RBAC, with the same
lifecycle as the workload it replaces. Recovery needs nothing that is not already in the
cluster:

```console
$ kubectl -n team-a get configmap fallow-archive-checkout-api \
    -o jsonpath='{.data.manifest\.yaml}' | kubectl apply -f -
deployment.apps/checkout-api created
```

The stored manifest is sanitized for re-apply — no `resourceVersion`, `uid`, `status`, or
`managedFields`, and no owner references that would have the garbage collector delete it on
sight — and it carries the replica count from *before* reclamation, so a recovered workload
comes back at full size rather than at zero.

## Deciding what "idle" means

Every organisation measures "in use" differently, so the reconciler depends only on the
[`idle.Detector`](internal/idle/detector.go) interface and the signals are assembled at
wiring time. Three sources ship in [internal/idle/sources.go](internal/idle/sources.go):

| Source | Reads | Idle when |
|---|---|---|
| `LastActivity` | the `fallow.dev/last-activity` annotation | nothing recorded within the window |
| `RolloutQuiet` | Deployment conditions and creation time | no rollout within the window — a service shipped this morning is work in progress, not garbage |
| `CPUBelow` | a `MetricsProvider` you supply | usage under a millicore threshold |

`LastActivity` reads an annotation rather than querying a metrics backend inline, which
means whatever already knows about traffic — an ingress metrics scraper, an Envoy
access-log pipeline, a Prometheus recording rule — can write it on whatever cadence suits,
and the controller stays fast with no hard dependency on a metrics stack.

`CPUBelow` needs a `MetricsProvider`, so it is not wired by default; supply one backed by
`metrics.k8s.io` in [cmd/manager/main.go](cmd/manager/main.go) to enable it. Adding a source
of your own means implementing `Name()` and `Evaluate()` and passing it to `idle.NewAllOf` —
the reconciler does not change.

## Running it

```console
$ make install        # install the CRD
$ make install-rbac   # namespace, service account, roles
$ make run            # run the controller locally against the current kubecontext
$ make samples        # apply the sample policies
$ make status         # kubectl get reclaimpolicies
```

`make run` uses your current kubecontext. Manager flags:

| Flag | Default | Purpose |
|---|---|---|
| `--last-activity-window` | `24h` | Silence before the activity signal reads idle |
| `--rollout-quiet-window` | `168h` | Since last rollout before a workload stops counting as in progress |
| `--resync-period` | `5m` | Upper bound between looks at a policy |
| `--protected-namespaces` | — | Comma-separated namespaces no policy may touch |
| `--leader-elect` | `false` | Only one replica escalates at a time |

## What a policy reports

```console
$ kubectl get rp
NAME          IDLE AFTER   NAMESPACES   WORKLOADS   RECLAIMED   DRY RUN   AGE
dev-reclaim   72h          4            37          6           true      9d
```

`status.targets` carries the per-workload breakdown — stage, idle-since, the saved replica
count, and a human-readable reason naming the signals that produced the verdict. Records of
deleted workloads are kept there, including the ConfigMap holding each manifest, since that
is the only remaining trail back to them.

## Development

```console
$ make check     # fmt, vet, test
$ make verify    # fail if generated files are stale
$ make help      # everything else
```

The CRD, deepcopy functions and RBAC role are generated from source markers by
`controller-gen`, pinned in `go.mod` as a tool dependency — `go tool controller-gen` works on
a clean checkout with no install step. Edit the `+kubebuilder:` markers, not the generated
files, then run `make manifests generate`.

Tests run against a fake API server and a fake clock, so the full three-day ladder is
exercised in milliseconds with no envtest binaries or cluster required.

## Limits worth knowing

- **Deployments only.** StatefulSets, CronJobs and Rollouts are not governed. `TargetStatus`
  carries a `Kind` field in anticipation, but nothing else does yet.
- **No in-cluster deployment manifest.** There is no image build or manager Deployment here;
  `make run` runs the controller locally against a cluster. Running it in-cluster means
  supplying both, plus a `POD_NAMESPACE` env var so it protects its own namespace.
- **No validating webhook.** A policy whose `delete.after` is zero, or whose stages are
  incoherent, is accepted by the API server. The CRD schema catches shape errors; it does
  not catch unwise ones.
- **Status grows.** Deleted-workload records accumulate in `status.targets` and are never
  pruned. On a policy that reclaims heavily, that will eventually want a retention limit.
- **Recovery is manual.** The controller archives and deletes; it never restores a deleted
  workload on its own. That is deliberate — an automatic un-delete is a surprising thing for
  a controller to do — but it does mean recovery is a human action.
