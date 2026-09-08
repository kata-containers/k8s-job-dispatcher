# k8s-job-dispatcher

Run exactly one node-pinned Kubernetes Job per selected node, paced, with
guaranteed per-node coverage.

Give it any `batch/v1` Job manifest as a template and a way to select nodes. It
creates one Job per node, pins each to its node with `spec.nodeName`, keeps at
most `--parallelism` of them in flight at a time, and exits non-zero listing the
nodes whose Jobs failed and what stopped each of them.

This page is the reference for using it. [ARCHITECTURE.md](ARCHITECTURE.md) is
the design: what guarantees it makes, and what it costs to bypass the scheduler
to make them.

## Why not a DaemonSet or an Indexed Job

A **DaemonSet** is the usual way to run something on every node, but it is
level-triggered and never finishes: there is no moment at which it reports "all
nodes are done", and no exit code to gate an upgrade on. Its rollout is also hard
to pace precisely.

An **Indexed Job** with `completions = <node count>` and topology spread gets you
pacing, but not coverage: once `parallelism < completions` the scheduler stops
balancing the spread, because it ignores already-completed pods when it does. Some
nodes get two Jobs and others none.

This dispatcher gives you all three at once — every node covered exactly once, a
bounded number running at any moment, and a single exit code for the whole
rollout — which is what makes it usable as a Helm hook or a CI gate.

## What it does

1. Reads a `batch/v1` Job template from disk and connects to the API server
   (in-cluster, or via kubeconfig).
2. Resolves the target namespace from `--namespace`, `$POD_NAMESPACE`, the
   ServiceAccount namespace file, then `default`.
3. Selects nodes, either from an explicit `--nodes` list or by paginated node
   `LIST` with repeatable `--node-selector` (the union of the matches) and an
   optional `--node-field-selector`, waiting for the eligible set to settle.
4. Applies **DaemonSet-equivalent taint admission**, using the template's own
   tolerations, so it reaches the same nodes a DaemonSet would and skips the ones
   it would not — `spec.nodeName` bypasses the scheduler, so this has to be
   re-implemented rather than inherited.
5. Optionally cleans the nodes it owns but no longer selects
   (`--cleanup-job-template`), so a selector change converges in both directions.
6. Deletes any Jobs an earlier run left behind, before opening a single slot:
   their pods still count against the parallelism you asked for.
7. Fans out one Job per node, generating DNS-safe names from `--name-prefix` and
   stamping tracking labels so it only ever sees its own Jobs.
8. **Revalidates each node immediately before dispatching to it** — same UID,
   still selected, still admissible, facts still reported — because selection
   happened at least one Job ago.
9. Refills the in-flight set as Jobs finish, polling them concurrently by `GET`,
   each read bounded by its own timeout.
10. Adopts a pre-existing Job that belongs to this run, and deletes and recreates
    one left over from an earlier run — telling them apart by `ownerReference`,
    since Job names repeat across runs.
11. Optionally does the node-scoped API work around each Job: claiming and
    demoting labels beforehand; afterwards waiting for `Ready`, verifying CRI
    runtime handlers, applying labels until they stick, and lifting start-up
    taints.
12. Reads the pod behind a Job that failed or stalled, so the reason travels with
    the node's result instead of being left in a log that the Job's TTL deletes,
    and records that result as an Event and on the Node itself.
13. Exits 0 only when every node's Job *and* its post-success work succeeded.

### Node identity

A Job is bound to a node by *name*, and a name can outlive the machine that
answered to it: drain a node, delete it, and let a replacement register under the
same name, and a rollout still holding the old selection would configure a machine
nobody chose.

So every node write is bracketed by the `metadata.uid` the dispatcher selected —
re-checked before dispatch, and carried into each label and taint patch as a JSON
Patch `test` operation, which makes a mismatch a *rejected write* rather than a
race. Nodes named with `--nodes` are fetched rather than taken as bare names, for
the same reason.

The other half of that check belongs to your Job: each container receives
`NODE_MACHINE_ID` from the node's `.status.nodeInfo.machineID`, so a token-free
privileged Job can ask the host itself whether it is the machine that was chosen.

### Why the node work lives here

The per-node Jobs are usually the privileged, host-mutating half of whatever you
are rolling out, and they run on every targeted node — including nodes that also
run untrusted workloads, where root can read any token mounted into a pod. Doing
the API work from the dispatcher instead means **those Jobs need no
ServiceAccount token at all**. The dispatcher is a single unprivileged pod you
can pin to a trusted node.

It also tightens the contract: a node is labelled only once its Job succeeded as
a whole and the node reported `Ready` again, never from inside a pipeline that
might still fail after writing the label.

## Usage

The image is published to `ghcr.io/kata-containers/k8s-job-dispatcher`. Each
release also attaches a `.tar.gz` per architecture holding the binary, its
licence and `THIRD-PARTY-NOTICES.txt`, for anyone packaging it themselves rather
than running the image, an SPDX SBOM resolved from the crate graph and recording
the licence each crate is under, and `SHA256SUMS`. The tarballs are
reproducible: same commit, same bytes.

The image carries the same two files under
`/usr/share/doc/k8s-job-dispatcher`. Both are written during the build by
[cargo-about](https://github.com/EmbarkStudios/cargo-about) from `about.toml`
and `about.hbs`, so they describe the binary beside them rather than whatever
the tree looked like when somebody last remembered to regenerate them. Which
licences may appear at all is `deny.toml`'s decision, enforced in CI.

Everything carries GitHub build provenance, so what a release claims to be can
be checked rather than assumed:

```bash
gh attestation verify k8s-job-dispatcher-0.1.0-linux-amd64.tar.gz \
  --repo kata-containers/k8s-job-dispatcher
gh attestation verify oci://ghcr.io/kata-containers/k8s-job-dispatcher:0.1.0 \
  --repo kata-containers/k8s-job-dispatcher
```

Each architecture inside the index is attested too, not just the index itself,
so a consumer that resolves one platform can verify the digest it actually
pulls — and from the registry, without the GitHub API:

```bash
digest="$(oras resolve --platform linux/s390x \
  ghcr.io/kata-containers/k8s-job-dispatcher:0.1.0)"
gh attestation verify "oci://ghcr.io/kata-containers/k8s-job-dispatcher@${digest}" \
  --repo kata-containers/k8s-job-dispatcher --bundle-from-oci
```

Note that `gh` publishes no ppc64le or s390x build, so verification has to run
somewhere it exists; the artefact's architecture is unrelated to the verifier's.

The amd64 and arm64 binaries are statically linked and need nothing. The ppc64le
and s390x ones need glibc and `libgcc_s.so.1`, for the reason described under
[Building](#building). The image carries both; the tarball carries neither, so
whoever packages those provides them.

```bash
k8s-job-dispatcher \
  --job-template=/etc/job/install-job.yaml \
  --name-prefix=rollout-install \
  --parallelism=20 \
  --node-selector='kubernetes.io/os=linux'
```

The template is cloned per node, with `metadata.name`, `spec.template.spec.nodeName`
and the tracking labels set. Everything else — containers, volumes, tolerations,
`backoffLimit` — is yours.

### RBAC

The minimum any run needs:

```yaml
rules:
  - apiGroups: [""]
    resources: ["nodes"]
    verbs: ["get", "list", "patch"]
  - apiGroups: ["batch"]
    resources: ["jobs"]
    verbs: ["create", "get"]
```

`get` on nodes is needed by the pre-dispatch revalidation and `patch` by the
result every run records on the node — which the node-label flags and
`--remove-node-taints` then need nothing beyond. On Jobs, `create` fans out and
`get` polls.

A run given an owner needs `jobs: ["list", "delete"]` on top of that: only an
`ownerReference` tells this run's Jobs from an earlier run's, so a run without
one leaves the leftovers alone rather than sweeping them, and cannot
`--yield-to-live-run` either. `--owner-job-name` also needs `get` on the owning
Job, which the rule above already covers, while `--owner-job-from-pod` needs
`pods: ["get"]` to read the pod's own `ownerReferences`.

`nodes/proxy: ["get"]` is additionally needed for
`--kubelet-timeout-warn-secs`.

Every run reports what became of each node, which means listing the pods a failed
Job left behind and publishing the answer as an Event, so grant both:

```yaml
  # in --namespace
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["list"]
```

```yaml
  # in the default namespace, which is the only one the apiserver takes an Event
  # about a Node in
  - apiGroups: [""]
    resources: ["events"]
    verbs: ["create"]
```

Neither is enforced — a run whose report is refused says so once and carries on,
with the Job's own `Failed` condition as the only account of what happened. That
is a rollout nobody can debug, though, which is why [reporting is not a flag](#reporting-what-happened).

## Options

### Selecting nodes

| Flag | Default | Purpose |
| --- | --- | --- |
| `--node-selector` | *(all nodes)* | Label selector, repeatable; the target set is the **union** of the matches, mirroring `nodeSelectorTerms` |
| `--node-field-selector` | — | Field selector, AND-ed with the label selector |
| `--nodes` | — | Explicit comma-separated node names; overrides the selectors and skips taint admission |
| `--ignore-node-taints` | `false` | Target matched nodes even when the template does not tolerate their taints — for cleanup runs that must reach nodes tainted since |
| `--wait-for-nodes-secs` | `0` | Keep re-resolving while nothing is eligible. Also declares that nodes are expected, making having nowhere to dispatch to — nothing matched, or everything matched untolerated — an error rather than a silent no-op |
| `--node-settle-secs` | `15` | How long the eligible set must stay unchanged before it is accepted |
| `--skip-satisfied-nodes` | `false` | Leave out nodes that already carry `--node-label` at its finished value and serve `--require-node-handlers` |
| `--node-page-size` | `500` | `LIST` page size |

`--wait-for-nodes-secs` exists because the dispatcher runs once while a DaemonSet
would not: on a fresh cluster the labels your selectors match may be written
seconds later by an add-on such as node-feature-discovery that is still starting.

`--node-settle-secs` is the other half of that. Those labels arrive **one node at
a time**, so a single unchanged poll is not convergence — it just means nothing
landed in the last few seconds. Waiting for a quiet period instead stops a rollout
from starting on whichever node won the race and leaving the rest out. It only
applies while `--wait-for-nodes-secs` is set; without a wait there is nothing to
settle.

`--skip-satisfied-nodes` is for a run that repeats in order to *cover* nodes that
joined since, rather than to roll a change out. It leaves alone any node where
every label this instance writes already holds the finished value and, where
`--require-node-handlers` is set, whose runtime says it is serving one of them —
so a run with nothing to do is one `LIST` and no Jobs at all, instead of a pod per
node discovering that its work is done. A node at the pending value is never
skipped: it was claimed by a run that did not see it through, and finishing it is
the most useful thing a later run can do.

The handlers half is what catches a host rebuilt under a Node object that kept its
labels. What nothing here catches is a *new version* of what the Jobs install,
because the label records that a run finished and not which one. Skipping is
coverage, never a rollout: the run that upgrades a fleet is the one that does not
pass this flag.

### Dispatching

| Flag | Default | Purpose |
| --- | --- | --- |
| `--job-template` | *required* | Path to the `batch/v1` Job YAML |
| `--name-prefix` | *required* | Prefix for generated Job names, also recorded as the owner label's value |
| `--namespace` | *(see above)* | Namespace to create Jobs in |
| `--parallelism` | `100` | Maximum Jobs in flight |
| `--poll-interval-secs` | `5` | Seconds between status polls |
| `--owner-job-name` | — | Adds an `ownerReference` to this Job, so the per-node Jobs are garbage-collected with it |
| `--owner-job-from-pod` | — | Same, for a run that cannot name its own Job: the owner is the Job that created this pod |
| `--yield-to-live-run` | `false` | Exit without dispatching while another run of this name prefix is still working. Needs an owner |
| `--tracking-label-prefix` | `k8s-job-dispatcher` | Prefix for `<prefix>/owner`, `<prefix>/node` and `<prefix>/node-name` |
| `--cleanup-job-template` | — | Job template for nodes this instance owns but no longer selects (below) |
| `--require-node-runtime-version` | `false` | Fail a node that reports no `containerRuntimeVersion` instead of dispatching to it |
| `--require-node-machine-id` | `false` | Fail a node that reports no `machineID` instead of dispatching to it |

Every per-node Job's containers receive `CONTAINER_RUNTIME_VERSION` from the
node's `.status.nodeInfo.containerRuntimeVersion` and `NODE_MACHINE_ID` from its
`.status.nodeInfo.machineID`, so a Job needing either does not have to read the
Node object itself. Existing values in the template are left alone, and a fact the
node does not report is simply not set. Use the two `--require-…` flags when the
Job cannot work without one — dispatching anyway would start a privileged pod
certain to fail immediately.

Give two dispatchers sharing a namespace different `--tracking-label-prefix`
values and neither will mistake the other's Jobs for its own.

A CronJob's Job is called `<cronjob>-<timestamp>`, a name nothing can put in the
manifest that starts it, so a run like that names its pod instead and the owning
Job is resolved from there:

```yaml
args: ["--owner-job-from-pod=$(POD_NAME)"]
env:
  - name: POD_NAME
    valueFrom:
      fieldRef:
        fieldPath: metadata.name
```

That pod has to be in the namespace the per-node Jobs are created in. Kubernetes
does not honour an `ownerReference` pointing into another namespace: it reads the
dependent as having no owner left and deletes it, which is the opposite of what
asking for an owner was for.

`--yield-to-live-run` covers the case where two runs of the same name prefix
overlap — an upgrade landing while something scheduled is mid-rollout, or two
people upgrading at once. Without it the second run reads the first one's
per-node Jobs as an earlier run's leftovers and deletes them, taking down
privileged pods in the middle of their work. With it, the second run finds the
Job driving them, sees it is still running, says whose fleet it is and exits 0.

It is off by default because standing aside is only free for a run that repeats.
An owning Job that is suspended, or whose pod cannot be scheduled, looks busy for
as long as it exists, and a one-shot run that yields to it has simply not
happened.

Generated Job names are DNS-1123-safe and collision-free: a name is used verbatim
only when sanitizing changed nothing and it fits in 63 characters, and otherwise
carries a hash of the full prefix-and-node identity. Two long node names, two long
release prefixes, and two names that merely normalize alike therefore all stay
distinct. The derivation is an implementation detail — an earlier run's Jobs are
found through the owner label and their `ownerReference`, never by recomputing what
they would be called today.

#### Converging a changed selection

Narrow your selector and the nodes that dropped out are still configured, still
carrying your label, and nothing will ever come back for them. Pass
`--cleanup-job-template` and the next run starts by running that template on the
nodes this instance owns but no longer selects, then dispatches to the ones it
does. It needs `--node-label-key` (with `--node-label` or `--claim-node-pending`)
to know which nodes are its own, and fails at startup if that is missing rather
than discovering it halfway through.

Ownership is judged against the **matched** set, not the admitted one: a taint
says a pod cannot run on a node right now, not that the node stopped being yours,
and reading it the other way would tear down every node somebody cordoned between
two runs.

A node that leaves the selection *while a rollout is running* is a different case:
it fails its own entry and is otherwise left untouched, because a selector
changing mid-rollout is not an instruction to dismantle a host. The next run's
cleanup pass picks it up if it is still out — and if it re-entered the selection
before that pass reached it, it is left alone there too.

### Labelling nodes

The dispatcher writes **your** label key, never one of its own choosing, because
what selects on it — a RuntimeClass, a node affinity, a monitoring query — is
yours.

| Flag | Default | Purpose |
| --- | --- | --- |
| `--node-label-key` | — | The label key to write. Required by the three flags below |
| `--node-label` | — | Value to set once the node's Job succeeded |
| `--remove-node-label` | `false` | Remove the key before the Job runs, for cleanup runs |
| `--claim-node-pending` | `false` | Set the key to the pending value before the Job runs, unless already present |
| `--node-label-pending-value` | `false` | The value meaning "claimed, not finished" |
| `--instance-label-prefix` | — | Enables multi-instance bookkeeping (below) |
| `--multi-install-suffix` | `default` | This instance's name below that prefix |
| `--require-node-handlers` | — | Comma-separated CRI handlers the node must serve, per `.status.runtimeHandlers`, before it is labelled |
| `--wait-node-ready-secs` | `0` | Wait for the node to report `Ready` after its Job, before labelling |
| `--remove-node-taints` | — | Taints to lift after labelling, as `key` or `key:effect`. Requires `--node-label` |
| `--kubelet-timeout-warn-secs` | `0` | Warn if the kubelet's `runtimeRequestTimeout` is below this |

A few of these are subtler than they look:

- **`--claim-node-pending`** makes a node discoverable by a later cleanup even if
  this run dies midway. Without it, a run that modified a node but crashed before
  labelling it leaves a node the default cleanup selection cannot see. It is fatal
  when it cannot be written: mutating a host that nothing can later discover is
  worse than not touching it at all.
- **`--remove-node-label` demotes rather than deletes** before the Job, so
  nothing new is selected onto the node while it is being taken apart, but the
  *key* survives for a cleanup that fails and has to be retried. The key is
  removed only once the cleanup Job actually succeeded.
- **`--require-node-handlers` passes if any one handler is served.** A node only
  serves the handlers built for its architecture, so demanding all of them would
  fail every mixed-architecture fleet. Serving *none* of them is the real symptom:
  the runtime never read what was written. A node that reports no handlers at all
  (Kubernetes below 1.30) never fails the check.
- **Labels are applied until they stick.** On k3s and RKE2 a CRI restart takes the
  kubelet with it, and a kubelet coming back re-registers its node with cached
  labels, silently undoing ours. `Ready` does not rule this out, so the label has
  to be observed to hold and re-applied when it drifts.
- **`--remove-node-taints` requires `--node-label`**, because lifting a start-up
  taint before the node carries the label meant to gate workloads would let them
  arrive ungated.

#### Several instances sharing one label

When more than one deployment of the same thing shares `--node-label-key`, set
`--instance-label-prefix` and give each instance its own
`--multi-install-suffix`. Each then marks the nodes it holds with
`<prefix>/<suffix>`, and a cleanup takes the shared key away only once no other
instance's mark is left:

- another instance is serving here → the shared key stays;
- the others have only claimed the node → the shared key is demoted to the pending
  value, so nothing is selected here but those instances can still find the node;
- ours was the last mark → the shared key goes.

Without `--instance-label-prefix` there is assumed to be a single instance, which
owns the shared key outright.

### Reporting what happened

There is no flag in this section. A run says on stdout which nodes failed and
what stopped each of them, and its final error repeats that list, but neither
survives the run: the log goes with the dispatcher's pod, and the per-node Jobs
go with their `ttlSecondsAfterFinished`, usually well before anybody looks. So
every run also writes the same answer where it outlives all of that — as an
Event against the Node, and on the Node itself.

This is not configurable because a rollout that has to be asked to explain
itself is one nobody asked, and the request would be missing from exactly the
deployment that later needs the answer.

The reason itself is read from the failed Job's **pod** while it is still there —
the failing container, its exit code, and the termination message it left in
`/dev/termination-log`:

```text
node worker-2: its job rollout-install-worker-2 failed: install-stage-host-check
exited 1: this node has no usable virtualization backend [BackoffLimitExceeded]
```

A Job whose pod is not running gets the same treatment without waiting for it to
fail. Nothing marks a Job that cannot schedule its pod or cannot pull its image,
so it stays "running" until `activeDeadlineSeconds` turns it into a bare failure
— an hour of a rollout looking busy on the usual settings. Two minutes in, and
every five after that, the run says what the wait is for instead.

**As an Event**, a `Normal`/`JobSucceeded` or `Warning`/`JobFailed` against the
Node, plus `Warning`/`JobPending` for the stalls above. This is what `kubectl
describe node` shows and what an event exporter already collects, so the result
reaches existing tooling without it knowing anything about this dispatcher.

The Event goes in the `default` namespace, not `--namespace`: a Node has no
namespace of its own, and the apiserver rejects an Event whose `involvedObject`
has none unless the Event itself is in `default`. That is also where the
kubelet's own node events go, and it is where `events: ["create"]` has to be
granted — the same verb in the run's own namespace grants nothing.

**On the Node**, under `--tracking-label-prefix`:

| Key | Kind | Value |
| --- | --- | --- |
| `<prefix>/result` | label | `succeeded` or `failed` |
| `<prefix>/error` | annotation | why it failed; removed when it succeeds |
| `<prefix>/finished-at` | annotation | when that was decided |

The state is a label so a fleet can be searched by it — `kubectl get nodes -l
<prefix>/result=failed` — and the reason is an annotation because it is a
sentence, which is neither short enough nor punctuated simply enough for a label
value. This is the durable half: the apiserver drops Events after its
`--event-ttl` (an hour by default), while what is on the node stays until the
next run overwrites it. It is written with `nodes: ["patch"]`, which every run
therefore needs. A cleanup run (`--remove-node-label`) clears all three instead
of writing `succeeded`, since the installation the result described is what it
just took away.

Both writes are best-effort: a result that could not be recorded is said once in
the log and never turned into a second failure. A cluster that refuses them —
an Event quota, a Role short a verb — gets a run that behaves exactly as it
would have and an account of it that stops at the Job's `Failed` condition.

## Running on a schedule

A run reaches the nodes that exist while it runs. A node that joins the cluster
afterwards matches the same selector and was never dispatched to, and nothing
comes back for it — where a DaemonSet would have covered it without being asked.
A CronJob is that "asked", and three flags exist for it:

```yaml
apiVersion: batch/v1
kind: CronJob
spec:
  schedule: "*/15 * * * *"
  concurrencyPolicy: Forbid
  startingDeadlineSeconds: 300
  successfulJobsHistoryLimit: 1
  failedJobsHistoryLimit: 3
  jobTemplate:
    spec:
      backoffLimit: 0
      template:
        spec:
          containers:
            - name: dispatcher
              args:
                - --owner-job-from-pod=$(POD_NAME)
                - --yield-to-live-run
                - --skip-satisfied-nodes
                - --wait-for-nodes-secs=0
                # and the same template, selectors and labelling flags as the
                # run that installed the fleet in the first place
              env:
                - name: POD_NAME
                  valueFrom:
                    fieldRef:
                      fieldPath: metadata.name
```

`--wait-for-nodes-secs=0` because that flag declares nodes are *expected* and
turns having nowhere to dispatch to into an error, which for something periodic
should be a quiet no-op. That covers a node still carrying
`node.kubernetes.io/not-ready` on its way in, which the next run finds ready. It
also switches off settling, so a run can catch a fleet mid-labelling and
dispatch to part of it — the rest arrive on the next one. Both are the opposite of
what a one-shot run wants, so do not copy its values here.

`concurrencyPolicy: Forbid` keeps two of these from overlapping, and
`--yield-to-live-run` covers what it cannot: an upgrade running while one of these
is mid-rollout. `backoffLimit: 0` because a run that failed on a node should
surface as a failed Job rather than be retried immediately — the next run is the
retry, and a `CronJob` whose last runs all failed is the alert.

Whether these runs also tear down nodes that have fallen out of the selection is
decided by whether `--cleanup-job-template` is passed. Leaving it out is the
conservative default: a node dropping out is usually a label gone wrong somewhere,
and dismantling a host on a timer with nobody watching is worse than a stale node
waiting for the next upgrade to clean it up.

## Building

```bash
cargo build --release          # binary at target/release/k8s-job-dispatcher
cargo test                     # unit tests only; no cluster needed
docker build -t k8s-job-dispatcher .
```

On amd64 and arm64 the binary is statically linked against musl, and those
images are `distroless/static`, carrying no runtime dependency whatsoever.
ppc64le and s390x link against glibc on `distroless/base-nossl`, which adds the
loader, glibc, and the libgcc that Rust's unwinder lives in. Builder and runtime
are the same Debian release, so the glibc it was linked against is the one it
finds.

s390x has no published musl `std` at all. ppc64le does, but no CI runner
executes that architecture natively, so musl there would be a variable proven
only under emulation.

All four targets are cross-compiled, so `rustc` runs natively and building for
s390x costs roughly what building for amd64 does. Pass `--platform` to pick one:

```bash
docker build --platform linux/s390x -t k8s-job-dispatcher:s390x .
```

## History

This started life as `kata-deploy-job-dispatcher` inside
[kata-containers](https://github.com/kata-containers/kata-containers), where it
runs the per-node install and cleanup Jobs for `kata-deploy`'s job mode. It was
extracted here so other projects can use it, and its Kata-specific label keys
became the `--node-label-key`, `--instance-label-prefix` and
`--tracking-label-prefix` flags. For the equivalent of the original behaviour,
pass:

```
--tracking-label-prefix=kata-deploy-job-dispatcher
--node-label-key=katacontainers.io/kata-runtime
--instance-label-prefix=kata-deploy.katacontainers.io
--require-node-runtime-version
--require-node-machine-id
```

There, the machine ID reaches the Jobs as `KATA_DEPLOY_NODE_MACHINE_ID`; here it
is the unprefixed `NODE_MACHINE_ID`.

## License

Apache License 2.0. See [LICENSE](LICENSE).
