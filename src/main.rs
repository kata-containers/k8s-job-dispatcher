// Copyright (c) 2026 NVIDIA Corporation
//
// SPDX-License-Identifier: Apache-2.0

//! Runs exactly one node-pinned Job per selected node, at most `--parallelism` at
//! a time, and exits non-zero listing the nodes whose Jobs failed.
//!
//! An Indexed Job with topology spread gives the pacing but not the coverage: once
//! `parallelism < completions` the scheduler stops balancing the spread, because it
//! ignores already-completed pods. A DaemonSet gives the coverage but never
//! finishes, so there is nothing to gate an upgrade on.

mod diagnosis;
mod job;
mod node_filter;
mod nodes;
mod report;

use anyhow::{bail, Context, Result};
use clap::{ArgGroup, Parser};
use job::{
    build_node_job, interpret_status, job_name, job_owned_by, sanitize_label_value, JobOutcome,
    TrackingLabels, DEFAULT_TRACKING_LABEL_PREFIX,
};
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{Node, Pod};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::api::{Api, DeleteParams, ListParams, PostParams};
use kube::Client;
use log::{error, info, warn};
use node_filter::{
    describe_taint, partition_by_tolerations, suggested_toleration, PodAdmission, SkippedNode,
};
use nodes::{InstanceMarker, NodeFacts, NodeLabelling, NodeOps, DEFAULT_PENDING_LABEL_VALUE};
use report::Reporter;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

/// A GET that keeps failing - RBAC changed under us, say - would otherwise be
/// polled forever and the run would end only when something killed it, with no
/// result reported for any node.
const JOB_READ_ERROR_BUDGET: Duration = Duration::from_secs(300);

/// Bounds a single status read, so one slow apiserver request cannot hold every
/// other node behind it.
const JOB_GET_TIMEOUT: Duration = Duration::from_secs(15);

/// How long a Job may go without its pod running before the run says what it is
/// waiting on. Such a Job does not fail until its deadline expires, which is an
/// hour of a rollout looking busy on the usual settings.
const WAITING_REPORT_AFTER: Duration = Duration::from_secs(120);

/// And how often afterwards. The reason is re-read, so a node that moves from
/// unschedulable to pulling says so.
const WAITING_REPORT_EVERY: Duration = Duration::from_secs(300);

/// Saying an unchanged tally every poll buries everything else in the log, and
/// Helm prints that log as a single line.
const PROGRESS_HEARTBEAT: Duration = Duration::from_secs(60);

#[derive(Parser, Debug, Clone)]
#[command(
    author,
    version,
    about = "Run one node-pinned Job per selected node, paced and with guaranteed coverage.",
    group(ArgGroup::new("owner").args(["owner_job_name", "owner_job_from_pod"]))
)]
struct Args {
    /// Path to a YAML file containing the batch/v1 Job to run on each node. It is
    /// cloned per node with metadata.name and nodeName set.
    #[arg(long)]
    job_template: String,

    /// Job template run on nodes this instance owns but which no longer match the
    /// selection, at the start of the next run. Without it a selector change
    /// converges in one direction only, leaving removed nodes configured and
    /// advertised for good.
    #[arg(long)]
    cleanup_job_template: Option<String>,

    /// Prefix for generated per-node Job names, also recorded as the owner label's
    /// value.
    #[arg(long)]
    name_prefix: String,

    /// Namespace to create the per-node Jobs in. Defaults to $POD_NAMESPACE, then
    /// the in-cluster service-account namespace, then "default".
    #[arg(long)]
    namespace: Option<String>,

    /// Maximum number of per-node Jobs in flight at once.
    #[arg(long, default_value_t = 100)]
    parallelism: usize,

    /// Label selector picking target nodes, e.g. "kubernetes.io/os=linux".
    /// Repeatable, in which case the target set is the UNION of the matches, the
    /// way nodeAffinity OR-s its nodeSelectorTerms.
    #[arg(long)]
    node_selector: Vec<String>,

    /// Field selector picking target nodes, ANDed with the label selector.
    #[arg(long)]
    node_field_selector: Option<String>,

    /// Explicit comma-separated node names. Overrides the selectors and skips
    /// taint admission.
    #[arg(long)]
    nodes: Option<String>,

    /// Target matched nodes even when the template does not tolerate their taints.
    /// For cleanup runs, which must reach every node acted on before.
    #[arg(long, default_value_t = false)]
    ignore_node_taints: bool,

    /// Seconds to keep re-resolving the target nodes while none is eligible yet,
    /// for labels an add-on such as node-feature-discovery is still writing.
    ///
    /// Also declares that at least one node is expected, so an empty selection
    /// becomes an error once the wait expires instead of a silent no-op.
    #[arg(long, default_value_t = 0)]
    wait_for_nodes_secs: u64,

    /// Seconds the eligible set must stay unchanged before it is accepted. Labels
    /// arrive node by node, so treating one unchanged poll as convergence misses a
    /// node labelled just after it.
    #[arg(long, default_value_t = 15)]
    node_settle_secs: u64,

    /// Owner Job to add an ownerReference to, so the per-node Jobs are
    /// garbage-collected together with it.
    #[arg(long)]
    owner_job_name: Option<String>,

    /// Same owner, taken from the Job that created this pod, for a run that cannot
    /// name its own Job: a CronJob's is generated. Pass the pod's name through the
    /// downward API (`fieldRef: metadata.name`).
    #[arg(long)]
    owner_job_from_pod: Option<String>,

    /// Exit without dispatching while another run of this name prefix is still
    /// working, instead of taking its Jobs over as an earlier run's leftovers.
    #[arg(long, requires = "owner")]
    yield_to_live_run: bool,

    /// Dispatch only to nodes that do not already carry --node-label at its
    /// finished value and serve --require-node-handlers. Covers nodes that joined
    /// since; does not roll a change out.
    #[arg(long, requires = "node_label")]
    skip_satisfied_nodes: bool,

    /// Prefix for the labels stamped on created Jobs (`<prefix>/owner`,
    /// `<prefix>/node`, `<prefix>/node-name`). Two dispatchers sharing a namespace
    /// need different prefixes to not read each other's Jobs.
    #[arg(long, default_value = DEFAULT_TRACKING_LABEL_PREFIX)]
    tracking_label_prefix: String,

    /// Label key to write on a node, e.g. "example.com/ready". Required by
    /// --node-label, --remove-node-label and --claim-node-pending, which supply
    /// only the value.
    #[arg(long)]
    node_label_key: Option<String>,

    /// Value to set on --node-label-key once a node's Job succeeded, before
    /// lifting any --remove-node-taints.
    #[arg(long)]
    node_label: Option<String>,

    /// Extra `key=value` to write alongside --node-label, repeatable. Removed
    /// alongside it too, but never used to decide which nodes this instance
    /// owns.
    #[arg(long)]
    extra_node_label: Vec<String>,

    /// Remove --node-label-key before creating a node's Job, so that whatever
    /// selects on it stops sending work there. For cleanup runs.
    #[arg(long, default_value_t = false)]
    remove_node_label: bool,

    /// Set --node-label-key to the pending value before creating a node's Job,
    /// unless already present, so a run that dies midway still leaves the node
    /// discoverable by a later cleanup.
    #[arg(long, default_value_t = false)]
    claim_node_pending: bool,

    /// Value meaning "claimed, not finished".
    #[arg(long, default_value = DEFAULT_PENDING_LABEL_VALUE)]
    node_label_pending_value: String,

    /// Label-key prefix under which each instance marks the nodes it holds, e.g.
    /// "deployer.example.com". Enables the multi-instance bookkeeping: the shared
    /// --node-label-key is taken away only once no other instance's mark is left.
    #[arg(long)]
    instance_label_prefix: Option<String>,

    /// This instance's name below --instance-label-prefix. Defaults to "default".
    #[arg(long)]
    multi_install_suffix: Option<String>,

    /// Comma-separated CRI runtime handlers a node must be serving, per its
    /// `.status.runtimeHandlers`, before it is labelled. Any one of them counts.
    #[arg(long)]
    require_node_handlers: Option<String>,

    /// Comma-separated taints to lift after labelling a node, as `key` (any
    /// effect) or `key:effect`. Requires --node-label.
    #[arg(long)]
    remove_node_taints: Option<String>,

    /// Seconds to wait for a node to report Ready after its Job finished, before
    /// labelling it. 0 disables the wait.
    #[arg(long, default_value_t = 0)]
    wait_node_ready_secs: u64,

    /// Warn when a node's kubelet `runtimeRequestTimeout` is below this many
    /// seconds. 0 disables the check.
    #[arg(long, default_value_t = 0)]
    kubelet_timeout_warn_secs: u64,

    /// Fail a node reporting no `.status.nodeInfo.containerRuntimeVersion` instead
    /// of dispatching to it, for Jobs that cannot work without knowing it.
    #[arg(long, default_value_t = false)]
    require_node_runtime_version: bool,

    /// Fail a node reporting no `.status.nodeInfo.machineID` instead of
    /// dispatching to it, for Jobs that check NODE_MACHINE_ID against the host
    /// they landed on.
    #[arg(long, default_value_t = false)]
    require_node_machine_id: bool,

    /// Seconds between status polls.
    #[arg(long, default_value_t = 5)]
    poll_interval_secs: u64,

    /// Page size used when listing nodes.
    #[arg(long, default_value_t = 500)]
    node_page_size: u32,

    /// Not a flag: set on the copy of these arguments that cleans the nodes this
    /// instance owns but no longer selects, where a node back in the selection is
    /// one to leave alone rather than one to act on.
    #[arg(skip)]
    removed_node_cleanup: bool,
}

// Overwhelmingly I/O-bound, so two workers keep the footprint small.
#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    // Helm prints this log as one escaped line, where the module path is width
    // spent on nothing.
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .format_target(false)
        .init();

    let args = Args::parse();
    let tracking = TrackingLabels::with_prefix(&args.tracking_label_prefix);

    let client = Client::try_default()
        .await
        .context("failed to create Kubernetes client")?;

    let namespace = resolve_namespace(args.namespace.clone());
    info!("k8s-job-dispatcher starting (namespace: {namespace})");

    // Read up front: the tolerations in the template's pod spec decide which
    // tainted nodes the per-node Jobs may run on.
    let template_raw = std::fs::read_to_string(&args.job_template)
        .with_context(|| format!("failed to read job template {}", args.job_template))?;
    let template: Job = serde_yaml::from_str(&template_raw)
        .with_context(|| format!("failed to parse job template {}", args.job_template))?;

    let node_ops = Arc::new(node_ops_from_args(&client, &args)?);

    // Resolved before any node is touched: a rollout that discovers halfway
    // through that it cannot converge has already changed the cluster.
    let cleanup = match args.cleanup_job_template.as_deref() {
        Some(path) => Some((
            path,
            cleanup_ownership_key(node_ops.labelling.as_ref())?.to_string(),
        )),
        None => None,
    };

    let owner = match (
        args.owner_job_name.as_deref(),
        args.owner_job_from_pod.as_deref(),
    ) {
        (Some(name), _) => Some(owner_ref_for_job(&client, &namespace, name).await?),
        (None, Some(pod)) => Some(owner_ref_from_pod(&client, &namespace, pod).await?),
        (None, None) => None,
    };

    let jobs: Api<Job> = Api::namespaced(client.clone(), &namespace);

    // Asked before the nodes are resolved: a run that is about to stand aside would
    // otherwise wait out --wait-for-nodes-secs to then dispatch to none of them.
    if let Some(owner) = owner.as_ref().filter(|_| args.yield_to_live_run) {
        let owner_value = sanitize_label_value(&args.name_prefix);
        if let Some(holder) =
            live_run_holding_the_fleet(&jobs, &owner_value, owner, &tracking).await?
        {
            info!(
                "the Job {holder} is still running per-node Jobs of its own; leaving the fleet to \
                 it and dispatching nothing"
            );
            return Ok(());
        }
    }

    let admission = template_admission(&template);
    let mut nodes = resolve_nodes(&client, &args, &admission).await?;
    // The matched set, not the admitted one: this decides which nodes the
    // instance still owns, and a taint only says a pod cannot run there right
    // now. Reading an untolerated taint as "no longer mine" would clean up every
    // node somebody cordoned between two runs.
    let desired_nodes = if args.nodes.is_some() {
        nodes.clone()
    } else {
        let api: Api<Node> = Api::all(client.clone());
        let (matched, _, _) = select_nodes(&api, &args, &admission).await?;
        matched
    };

    if let Some((cleanup_path, ownership_key)) = cleanup {
        nodes = converge_removed_nodes(
            &jobs,
            &client,
            &args,
            &admission,
            &namespace,
            cleanup_path,
            &ownership_key,
            owner.as_ref(),
            &node_ops,
            &desired_nodes,
            nodes,
            &tracking,
        )
        .await?;
    }

    if nodes.is_empty() {
        info!("no target nodes matched the selection; nothing to do");
        return Ok(());
    }

    // Only the dispatch pass skips. The cleanup pass above acts on nodes that left
    // the selection, and one of those needs taking apart however finished it looks.
    if args.skip_satisfied_nodes {
        let selected = nodes.len();
        nodes.retain(|node| !node_ops.is_satisfied(node));
        if nodes.is_empty() {
            info!("all {selected} selected node(s) already carry this run's result; nothing to do");
            return Ok(());
        }
        if nodes.len() < selected {
            info!(
                "{} of {selected} selected node(s) already carry this run's result and are left \
                 alone",
                selected - nodes.len()
            );
        }
    }

    let parallelism = args.parallelism.clamp(1, nodes.len());
    info!(
        "fanning out {} per-node Job(s) with parallelism {}",
        nodes.len(),
        parallelism
    );

    run_fanout(
        &jobs,
        &template,
        &nodes,
        &args,
        &namespace,
        parallelism,
        owner.as_ref(),
        node_ops.clone(),
        &client,
        &tracking,
    )
    .await
}

/// Which nodes this instance owns has to be recorded somewhere before the ones it
/// no longer selects can be found again.
fn cleanup_ownership_key(labelling: Option<&NodeLabelling>) -> Result<&str> {
    labelling.map(NodeLabelling::ownership_key).ok_or_else(|| {
        anyhow::anyhow!(
            "--cleanup-job-template needs a label recording which nodes this instance owns, so it \
             needs --node-label-key together with --node-label or --claim-node-pending; without \
             one there is no way to tell a node this instance configured from any other"
        )
    })
}

/// Clean the nodes this instance owns but no longer selects, before dispatching to
/// the ones it does, so that a selector change converges in both directions.
#[allow(clippy::too_many_arguments)]
async fn converge_removed_nodes(
    jobs: &Api<Job>,
    client: &Client,
    args: &Args,
    admission: &PodAdmission,
    namespace: &str,
    cleanup_path: &str,
    ownership_key: &str,
    owner: Option<&OwnerReference>,
    node_ops: &Arc<NodeOps>,
    desired_nodes: &[Node],
    nodes: Vec<Node>,
    tracking: &TrackingLabels,
) -> Result<Vec<Node>> {
    let cleanup_raw = std::fs::read_to_string(cleanup_path)
        .with_context(|| format!("failed to read cleanup job template {cleanup_path}"))?;
    let cleanup_template: Job = serde_yaml::from_str(&cleanup_raw)
        .with_context(|| format!("failed to parse cleanup job template {cleanup_path}"))?;

    let removed = nodes_owned_but_not_desired(client, ownership_key, desired_nodes).await?;
    if removed.is_empty() {
        return Ok(nodes);
    }

    info!(
        "{} node(s) no longer match this instance's selection; cleaning them before dispatching \
         to the desired set",
        removed.len()
    );

    let mut cleanup_args = args.clone();
    cleanup_args.name_prefix = format!("{}-removed", args.name_prefix);
    cleanup_args.cleanup_job_template = None;
    cleanup_args.removed_node_cleanup = true;

    let mut cleanup_ops = (**node_ops).clone();
    cleanup_ops.label_value = None;
    cleanup_ops.remove_label = true;
    cleanup_ops.claim_pending = false;
    cleanup_ops.remove_taints.clear();
    cleanup_ops.wait_ready = None;
    cleanup_ops.require_handlers.clear();
    cleanup_ops.kubelet_timeout_warn = None;

    let parallelism = cleanup_args.parallelism.clamp(1, removed.len());
    run_fanout(
        jobs,
        &cleanup_template,
        &removed,
        &cleanup_args,
        namespace,
        parallelism,
        owner,
        Arc::new(cleanup_ops),
        client,
        tracking,
    )
    .await
    .context("failed to clean the nodes removed from the selection")?;

    // Cleanup can take long enough for a removed node to re-enter the selection.
    // It was left untouched by the guarded cleanup path, so it belongs to this
    // run's dispatch pass.
    resolve_nodes(client, args, admission).await
}

async fn nodes_owned_but_not_desired(
    client: &Client,
    ownership_key: &str,
    desired: &[Node],
) -> Result<Vec<Node>> {
    let desired: HashSet<&str> = desired
        .iter()
        .filter_map(|node| node.metadata.name.as_deref())
        .collect();

    let owned = Api::<Node>::all(client.clone())
        .list(&ListParams::default().labels(ownership_key))
        .await
        .with_context(|| {
            format!("failed to list the nodes carrying this instance's marker {ownership_key}")
        })?;

    Ok(owned
        .items
        .into_iter()
        .filter(|node| {
            node.metadata
                .name
                .as_deref()
                .is_some_and(|name| !desired.contains(name))
        })
        .collect())
}

/// Split on the first `=`, so a value containing one survives.
///
/// The same key twice is rejected: one patch cannot set a key to two values,
/// and the one that loses would then be waited on for ever.
fn extra_labels_from_args(values: &[String]) -> Result<Vec<(String, String)>> {
    let mut labels: Vec<(String, String)> = Vec::new();

    for entry in values {
        let (key, value) = entry
            .split_once('=')
            .map(|(key, value)| (key.trim().to_string(), value.to_string()))
            .filter(|(key, _)| !key.is_empty())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "--extra-node-label {entry:?} is not key=value; give the label to write, \
                     e.g. --extra-node-label=example.com/ready=true"
                )
            })?;

        if labels.iter().any(|(seen, _)| seen == &key) {
            bail!("--extra-node-label {key} was given more than once, with two values to write");
        }
        labels.push((key, value));
    }

    Ok(labels)
}

/// A shared key would let the extra label's unconditional cleanup take away
/// what the ownership bookkeeping had decided to keep.
fn check_extra_label_keys(
    labelling: Option<&NodeLabelling>,
    extra: &[(String, String)],
) -> Result<()> {
    let Some(labelling) = labelling else {
        return Ok(());
    };

    for (key, _) in extra {
        if labelling.keys().contains(&key.as_str()) {
            bail!(
                "--extra-node-label {key} is already written as an ownership key by \
                 --node-label-key or --instance-label-prefix; give it a key of its own"
            );
        }
    }

    Ok(())
}

/// Neither written nor removed without one of the two flags that do it.
fn check_extra_label_flags(args: &Args) -> Result<()> {
    let wants_labelling = args.node_label.is_some() || args.remove_node_label;
    if !args.extra_node_label.is_empty() && !wants_labelling {
        bail!(
            "--extra-node-label needs --node-label or --remove-node-label: it is written \
             alongside the first and removed alongside the second, and neither happens without \
             one of them"
        );
    }
    Ok(())
}

fn comma_separated(value: Option<&str>) -> Vec<String> {
    value
        .unwrap_or_default()
        .split(',')
        .map(|item| item.trim().to_string())
        .filter(|item| !item.is_empty())
        .collect()
}

/// The dispatcher writes the caller's key, never one of its own choosing, so a
/// value without a key is rejected rather than guessed at.
fn labelling_from_args(args: &Args) -> Result<Option<NodeLabelling>> {
    let wants_labelling =
        args.node_label.is_some() || args.remove_node_label || args.claim_node_pending;

    let Some(key) = args
        .node_label_key
        .as_deref()
        .map(str::trim)
        .filter(|key| !key.is_empty())
    else {
        if wants_labelling {
            bail!(
                "--node-label-key is required by --node-label, --remove-node-label and \
                 --claim-node-pending: they supply the label's value, not its key"
            );
        }
        if args.instance_label_prefix.is_some() || args.multi_install_suffix.is_some() {
            bail!(
                "--instance-label-prefix and --multi-install-suffix only mark the nodes this \
                 instance holds, which needs --node-label-key and one of --node-label, \
                 --remove-node-label or --claim-node-pending"
            );
        }
        return Ok(None);
    };

    if !wants_labelling {
        bail!(
            "--node-label-key writes nothing on its own; pass one of --node-label, \
             --remove-node-label or --claim-node-pending to say what to write"
        );
    }

    if args.multi_install_suffix.is_some() && args.instance_label_prefix.is_none() {
        bail!(
            "--multi-install-suffix names this instance below --instance-label-prefix, so it \
             needs that flag too; without it there is a single instance owning {key} outright"
        );
    }

    Ok(Some(NodeLabelling {
        key: key.to_string(),
        pending_value: args.node_label_pending_value.clone(),
        instance: args
            .instance_label_prefix
            .as_deref()
            .map(|prefix| InstanceMarker::new(prefix, args.multi_install_suffix.as_deref())),
    }))
}

/// Lifting a start-up taint before the node carries the label meant to gate the
/// workloads it keeps away would let them arrive ungated.
fn check_taint_flags(args: &Args) -> Result<()> {
    if !comma_separated(args.remove_node_taints.as_deref()).is_empty() && args.node_label.is_none()
    {
        bail!(
            "--remove-node-taints needs --node-label: a start-up taint may only be lifted once \
             the node is labelled, otherwise workloads reach it before the label that is \
             supposed to gate them"
        );
    }

    Ok(())
}

fn node_ops_from_args(client: &Client, args: &Args) -> Result<NodeOps> {
    check_taint_flags(args)?;
    check_extra_label_flags(args)?;

    let labelling = labelling_from_args(args)?;
    let extra_labels = extra_labels_from_args(&args.extra_node_label)?;
    check_extra_label_keys(labelling.as_ref(), &extra_labels)?;

    let mut ops = NodeOps::new(client, &args.tracking_label_prefix);

    ops.labelling = labelling;
    ops.label_value = args.node_label.clone();
    ops.extra_labels = extra_labels;
    ops.remove_label = args.remove_node_label;
    ops.claim_pending = args.claim_node_pending;
    ops.remove_taints = comma_separated(args.remove_node_taints.as_deref());
    ops.require_handlers = comma_separated(args.require_node_handlers.as_deref());
    if args.wait_node_ready_secs > 0 {
        ops.wait_ready = Some(Duration::from_secs(args.wait_node_ready_secs));
    }
    if args.kubelet_timeout_warn_secs > 0 {
        ops.kubelet_timeout_warn = Some(Duration::from_secs(args.kubelet_timeout_warn_secs));
    }

    Ok(ops)
}

fn resolve_namespace(flag: Option<String>) -> String {
    if let Some(ns) = flag.filter(|s| !s.trim().is_empty()) {
        return ns;
    }
    if let Ok(ns) = std::env::var("POD_NAMESPACE") {
        if !ns.trim().is_empty() {
            return ns;
        }
    }
    if let Ok(ns) =
        std::fs::read_to_string("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
    {
        let ns = ns.trim().to_string();
        if !ns.is_empty() {
            return ns;
        }
    }
    "default".to_string()
}

fn template_admission(template: &Job) -> PodAdmission {
    let pod = template
        .spec
        .as_ref()
        .and_then(|spec| spec.template.spec.as_ref());

    PodAdmission {
        tolerations: pod
            .and_then(|pod| pod.tolerations.clone())
            .unwrap_or_default(),
        host_network: pod.and_then(|pod| pod.host_network).unwrap_or(false),
    }
}

fn selector_passes(selectors: &[String]) -> Vec<Option<&str>> {
    if selectors.is_empty() {
        return vec![None];
    }
    selectors.iter().map(|s| Some(s.as_str())).collect()
}

/// An explicit `--nodes` list is taken verbatim: it has no daemonset equivalent
/// and names exact nodes, so it is honoured as a deliberate override. Each is
/// fetched, since a node that has to be identified by UID cannot be a bare name.
async fn resolve_nodes(
    client: &Client,
    args: &Args,
    admission: &PodAdmission,
) -> Result<Vec<Node>> {
    if let Some(list) = args.nodes.as_deref() {
        let mut names: Vec<String> = list
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        names.sort();
        names.dedup();

        let api: Api<Node> = Api::all(client.clone());
        let mut nodes = Vec::with_capacity(names.len());
        for name in names {
            nodes.push(
                api.get(&name)
                    .await
                    .with_context(|| format!("failed to resolve explicitly named node {name}"))?,
            );
        }
        return Ok(nodes);
    }

    let api: Api<Node> = Api::all(client.clone());
    let poll = Duration::from_secs(args.poll_interval_secs.max(1));
    let deadline = Instant::now() + Duration::from_secs(args.wait_for_nodes_secs);
    let mut announced_wait = false;
    let mut previous: Option<Vec<String>> = None;
    let mut stable_since: Option<Instant> = None;

    let report_skipped = |skipped: &[SkippedNode]| {
        for node in skipped {
            info!(
                "skipping node {}: it carries the taint {}, which this run does not tolerate \
                 (a DaemonSet would not have been scheduled there either)",
                node.name,
                describe_taint(&node.taint)
            );
        }
    };

    loop {
        let (_, admitted, skipped) = select_nodes(&api, args, admission).await?;
        let expired = Instant::now() >= deadline;

        if !admitted.is_empty() {
            let mut names: Vec<String> = admitted
                .iter()
                .filter_map(|node| node.metadata.name.clone())
                .collect();
            names.sort();

            let changed = previous.as_deref() != Some(names.as_slice());
            if changed {
                previous = Some(names.clone());
                stable_since = Some(Instant::now());
            }

            // The labels a selector matches are written per node by an add-on
            // that is itself still starting, and they arrive one node at a time,
            // so a single unchanged poll is not convergence: it misses a node
            // labelled just after it.
            let settled = args.wait_for_nodes_secs == 0
                || stable_since.is_some_and(|since| {
                    since.elapsed() >= Duration::from_secs(args.node_settle_secs)
                });

            if expired || settled {
                report_skipped(&skipped);
                return Ok(admitted);
            }

            if changed {
                info!(
                    "{} node(s) eligible so far ({}); waiting for the set to stay unchanged for {}s",
                    names.len(),
                    names.join(", "),
                    args.node_settle_secs
                );
            }
            tokio::time::sleep(poll).await;
            continue;
        }

        if previous.as_deref() != Some(&[]) {
            previous = Some(Vec::new());
            stable_since = Some(Instant::now());
        }

        if expired {
            report_skipped(&skipped);
            return no_eligible_nodes(args, &skipped);
        }

        if !announced_wait {
            info!(
                "no node is eligible yet; re-checking for up to {}s. Nodes become eligible \
                 asynchronously: the labels a selector matches are often written by an add-on \
                 (node-feature-discovery, say) that starts alongside this dispatcher, and \
                 start-up taints clear only once the node settles",
                args.wait_for_nodes_secs
            );
            announced_wait = true;
        }
        tokio::time::sleep(poll).await;
    }
}

/// Returns the matched set, the admitted subset and what was skipped. The matched
/// set is what ownership is judged against: a taint says a pod cannot run on a
/// node right now, not that the node stopped being ours.
async fn select_nodes(
    api: &Api<Node>,
    args: &Args,
    admission: &PodAdmission,
) -> Result<(Vec<Node>, Vec<Node>, Vec<SkippedNode>)> {
    let mut matched: Vec<Node> = Vec::new();
    for selector in selector_passes(&args.node_selector) {
        matched.extend(list_nodes(api, args, selector).await?);
    }

    if args.ignore_node_taints {
        let matched = dedup_by_name(matched, None);
        return Ok((matched.clone(), matched, Vec::new()));
    }

    let (admitted, skipped) = partition_by_tolerations(&matched, admission);
    Ok((
        dedup_by_name(matched.clone(), None),
        dedup_by_name(matched, Some(&admitted)),
        skipped,
    ))
}

/// Keeps the objects rather than the names, so the facts they carry can be handed
/// to the per-node Jobs.
fn dedup_by_name(nodes: Vec<Node>, keep: Option<&[String]>) -> Vec<Node> {
    let mut seen: Vec<String> = Vec::new();
    let mut unique: Vec<Node> = Vec::new();

    for node in nodes {
        let Some(name) = node.metadata.name.clone() else {
            continue;
        };
        if let Some(keep) = keep {
            if !keep.contains(&name) {
                continue;
            }
        }
        if seen.contains(&name) {
            continue;
        }
        seen.push(name);
        unique.push(node);
    }

    unique.sort_by(|a, b| a.metadata.name.cmp(&b.metadata.name));
    unique
}

/// Nowhere to dispatch to only fails a run that waited, since it expected nodes.
/// For a run that repeats, an untolerated taint is as likely a node on its way up.
fn no_eligible_nodes(args: &Args, skipped: &[SkippedNode]) -> Result<Vec<Node>> {
    let waited = if args.wait_for_nodes_secs > 0 {
        format!(" after waiting {}s", args.wait_for_nodes_secs)
    } else {
        String::new()
    };

    if let Some(blocked) = skipped.first() {
        if args.wait_for_nodes_secs == 0 {
            info!(
                "all {} selected node(s) carry a taint this run does not tolerate (node {} has \
                 taint {}); nothing to do",
                skipped.len(),
                blocked.name,
                describe_taint(&blocked.taint)
            );
            return Ok(Vec::new());
        }

        bail!(
            "all {} selected node(s) carry a taint this run does not tolerate{}, so there is \
             nowhere to dispatch to. First blocker: node {} has taint {}. If you meant to target \
             these nodes, tolerate it in the Job template:\n{}",
            skipped.len(),
            waited,
            blocked.name,
            describe_taint(&blocked.taint),
            suggested_toleration(&blocked.taint)
        );
    }

    if args.wait_for_nodes_secs > 0 {
        bail!(
            "no node matched the selection{}, so there is nowhere to dispatch to. A node matching \
             any one of these selectors is enough: {}. Check that whatever applies those labels \
             (node-feature-discovery, for one) is running, or set --wait-for-nodes-secs=0 to make \
             an empty selection a no-op.",
            waited,
            describe_selectors(&args.node_selector)
        );
    }

    Ok(Vec::new())
}

fn describe_selectors(selectors: &[String]) -> String {
    if selectors.is_empty() {
        return "<none> (every node)".to_string();
    }
    selectors
        .iter()
        .map(|s| format!("[{s}]"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Whether one node still matches the selection, asked of the apiserver rather
/// than of the list this run started from, which may be minutes old by now.
async fn node_still_selected(client: &Client, args: &Args, node: &str) -> Result<bool> {
    if let Some(nodes) = args.nodes.as_deref() {
        return Ok(nodes
            .split(',')
            .map(str::trim)
            .any(|candidate| candidate == node));
    }

    let api = Api::<Node>::all(client.clone());
    let node_field = format!("metadata.name={node}");
    let field_selector = match args.node_field_selector.as_deref() {
        Some(existing) if !existing.is_empty() => format!("{existing},{node_field}"),
        _ => node_field,
    };

    for selector in selector_passes(&args.node_selector) {
        // No selector means every node, which is an absent labelSelector rather
        // than an empty one: what an empty string means is up to the apiserver.
        let params = ListParams {
            limit: Some(1),
            label_selector: selector.map(str::to_string),
            field_selector: Some(field_selector.clone()),
            ..Default::default()
        };
        if !api
            .list(&params)
            .await
            .with_context(|| format!("failed to revalidate the node selection for {node}"))?
            .items
            .is_empty()
        {
            return Ok(true);
        }
    }

    Ok(false)
}

async fn list_nodes(
    api: &Api<Node>,
    args: &Args,
    label_selector: Option<&str>,
) -> Result<Vec<Node>> {
    let mut nodes = Vec::new();
    let mut continue_token: Option<String> = None;

    loop {
        let lp = ListParams {
            limit: Some(args.node_page_size.max(1)),
            label_selector: label_selector.map(str::to_string),
            field_selector: args.node_field_selector.clone(),
            continue_token: continue_token.clone(),
            ..Default::default()
        };

        let page = api.list(&lp).await.with_context(|| {
            format!(
                "failed to list nodes (label selector: {})",
                label_selector.unwrap_or("<none>")
            )
        })?;
        nodes.extend(page.items);

        match page.metadata.continue_ {
            Some(token) if !token.is_empty() => continue_token = Some(token),
            _ => break,
        }
    }

    Ok(nodes)
}

/// Non-controller, so it does not interfere with the Job controller's own
/// ownership of pods.
async fn owner_ref_for_job(client: &Client, namespace: &str, name: &str) -> Result<OwnerReference> {
    let jobs: Api<Job> = Api::namespaced(client.clone(), namespace);
    let job = jobs
        .get(name)
        .await
        .with_context(|| format!("failed to get owner job {name}"))?;
    let uid = job
        .metadata
        .uid
        .ok_or_else(|| anyhow::anyhow!("owner job {name} has no uid"))?;
    Ok(OwnerReference {
        api_version: "batch/v1".to_string(),
        kind: "Job".to_string(),
        name: name.to_string(),
        uid,
        controller: Some(false),
        block_owner_deletion: Some(false),
    })
}

/// The pod is looked up in the namespace the per-node Jobs go into: Kubernetes does
/// not honour an `ownerReference` across namespaces, and deletes the dependent as
/// unowned instead.
async fn owner_ref_from_pod(
    client: &Client,
    namespace: &str,
    pod_name: &str,
) -> Result<OwnerReference> {
    let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
    let pod = pods
        .get(pod_name)
        .await
        .with_context(|| format!("failed to get pod {pod_name} in namespace {namespace}"))?;
    owning_job_of_pod(&pod, pod_name)
}

/// The kind alone is not enough: another API group is free to have a Job of its own.
fn references_a_batch_job(reference: &OwnerReference) -> bool {
    reference.kind == "Job"
        && reference
            .api_version
            .split('/')
            .next()
            .is_some_and(|group| group == "batch")
}

/// Read off the pod rather than by fetching the Job: the uid there was written by
/// the Job controller, so there is nothing a GET could confirm about it.
fn owning_job_of_pod(pod: &Pod, pod_name: &str) -> Result<OwnerReference> {
    pod.metadata
        .owner_references
        .iter()
        .flatten()
        .find(|reference| references_a_batch_job(reference))
        .map(|reference| OwnerReference {
            api_version: reference.api_version.clone(),
            kind: reference.kind.clone(),
            name: reference.name.clone(),
            uid: reference.uid.clone(),
            controller: Some(false),
            block_owner_deletion: Some(false),
        })
        .ok_or_else(|| {
            anyhow::anyhow!(
                "pod {pod_name} is not owned by a Job, so there is nothing for the per-node Jobs \
                 to be garbage-collected with. --owner-job-from-pod is for a dispatcher running as \
                 a Job it cannot name, such as a CronJob's; anything else should either name its \
                 Job with --owner-job-name or pass neither"
            )
        })
}

/// A node the run could not finish, and what stopped it. The reason is kept so
/// that the summary at the end can name it, not only the log.
struct NodeFailure {
    node: String,
    reason: String,
}

/// Everything a node's result is written to, in one place, so that no path can
/// report to the log and forget the cluster.
///
/// Written as each node finishes: a dispatcher killed halfway through - a
/// release timeout, its own node drained - would otherwise take the whole
/// account with it.
struct Outcomes {
    ops: Arc<NodeOps>,
    reporter: Reporter,
}

impl Outcomes {
    async fn failed(&self, node: Option<&Node>, name: &str, reason: String) -> NodeFailure {
        error!("node {name}: {reason}");
        self.ops.record_failure(name, uid_of(node), &reason).await;
        if let Some(node) = node {
            self.reporter.node_failed(node, &reason).await;
        }
        NodeFailure {
            node: name.to_string(),
            reason,
        }
    }

    async fn succeeded(&self, node: Option<&Node>, name: &str) {
        self.ops.record_success(name, uid_of(node)).await;
        if let Some(node) = node {
            self.reporter.node_succeeded(node).await;
        }
    }

    /// Not a result: the node is still being worked on.
    async fn waiting(&self, node: Option<&Node>, name: &str, detail: &str) {
        warn!("node {name}: {detail}");
        if let Some(node) = node {
            self.reporter.node_waiting(node, detail).await;
        }
    }
}

fn uid_of(node: Option<&Node>) -> Option<&str> {
    node?.metadata.uid.as_deref()
}

#[allow(clippy::too_many_arguments)]
async fn run_fanout(
    jobs: &Api<Job>,
    template: &Job,
    nodes: &[Node],
    args: &Args,
    namespace: &str,
    parallelism: usize,
    owner: Option<&OwnerReference>,
    node_ops: Arc<NodeOps>,
    client: &Client,
    tracking: &TrackingLabels,
) -> Result<()> {
    let mut queue: VecDeque<Node> = nodes.iter().cloned().collect();
    let mut in_flight: HashMap<String, String> = HashMap::new();
    let mut succeeded = 0usize;
    let mut failed: Vec<NodeFailure> = Vec::new();

    // So that a failure discovered by node name alone can still be reported
    // against the object.
    let by_name: HashMap<&str, &Node> = nodes
        .iter()
        .filter_map(|node| Some((node.metadata.name.as_deref()?, node)))
        .collect();
    let outcomes = Outcomes {
        ops: node_ops.clone(),
        reporter: Reporter::new(client, &args.tracking_label_prefix),
    };

    // Read only when a Job fails or stalls; the Job's status holds no reason.
    let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
    let mut dispatched_at: HashMap<String, Instant> = HashMap::new();
    // When each Job was last asked what it waits for, and what it said.
    let mut reported_waiting: HashMap<String, (Instant, Option<String>)> = HashMap::new();

    let post = PostParams::default();
    let poll = Duration::from_secs(args.poll_interval_secs.max(1));
    // Sanitized once, since callers pass raw values such as a Helm release name.
    let owner_value = sanitize_label_value(&args.name_prefix);
    clear_stale_jobs(jobs, &owner_value, owner, tracking).await?;

    // Post-success work can take minutes, so it runs off to the side while holding
    // a slot - the node is not done before it finishes - which keeps one unready
    // node from stalling every other node's Job.
    let mut post_work: JoinSet<Result<()>> = JoinSet::new();
    let mut post_nodes: HashMap<tokio::task::Id, String> = HashMap::new();
    let mut unreadable: HashMap<String, Instant> = HashMap::new();
    let mut progress = Progress::default();

    loop {
        while in_flight.len() + post_work.len() < parallelism {
            let Some(selected) = queue.pop_front() else {
                break;
            };
            let Some(node) = selected.metadata.name.as_deref() else {
                error!("selected a Node object without metadata.name; refusing to dispatch to it");
                continue;
            };
            let Some(expected_uid) = selected.metadata.uid.as_deref() else {
                failed.push(
                    outcomes
                        .failed(
                            Some(&selected),
                            node,
                            "the selected Node has no UID; refusing to dispatch to it".to_string(),
                        )
                        .await,
                );
                continue;
            };

            // Everything below re-reads the node: selection happened at least one
            // Job ago, and a name can by now belong to a different machine, to a
            // node outside the selection, or to one that has become untouchable.
            let fresh = match node_ops.get(node).await {
                Ok(fresh) => fresh,
                Err(err) => {
                    failed.push(
                        outcomes
                            .failed(
                                Some(&selected),
                                node,
                                format!("could not re-read it before dispatch ({err:#})"),
                            )
                            .await,
                    );
                    continue;
                }
            };
            if fresh.metadata.uid.as_deref() != Some(expected_uid) {
                failed.push(
                    outcomes
                        // No node object: the machine now answering to this
                        // name was never acted on.
                        .failed(
                            None,
                            node,
                            format!(
                                "was selected as UID {expected_uid} but now has UID {:?}; \
                                 refusing to mutate a replacement machine under a stale identity",
                                fresh.metadata.uid
                            ),
                        )
                        .await,
                );
                continue;
            }

            let still_selected = node_still_selected(client, args, node).await?;
            if args.removed_node_cleanup && still_selected {
                info!(
                    "node {node}: re-entered the selection before it could be cleaned; leaving it \
                     as it is"
                );
                succeeded += 1;
                continue;
            }
            if !args.removed_node_cleanup && !still_selected {
                // Left alone rather than cleaned up here: a selection changing
                // under a running rollout is not an instruction to dismantle a
                // host. A later run starts by cleaning what it owns but no longer
                // selects, and picks this node up if it is still out.
                failed.push(
                    outcomes
                        .failed(
                            Some(&fresh),
                            node,
                            "no longer matches the selection; refusing to act on a stale \
                             selection, leaving the node as it is"
                                .to_string(),
                        )
                        .await,
                );
                continue;
            }

            // Whoever passed --ignore-node-taints asked for every selected node to
            // be acted on whatever it carries, so re-checking would go back on it.
            if !args.ignore_node_taints {
                let (admitted, skipped) = partition_by_tolerations(
                    std::slice::from_ref(&fresh),
                    &template_admission(template),
                );
                if admitted.is_empty() {
                    failed.push(
                        outcomes
                            .failed(
                                Some(&fresh),
                                node,
                                format!(
                                    "acquired the untolerated taint {} before dispatch; refusing \
                                     to start a Job the taint manager would evict",
                                    skipped
                                        .first()
                                        .map(|item| describe_taint(&item.taint))
                                        .unwrap_or_else(|| "<unknown>".to_string())
                                ),
                            )
                            .await,
                    );
                    continue;
                }
            }

            let node_facts = NodeFacts::from_node(&fresh);
            if args.require_node_runtime_version && node_facts.container_runtime_version.is_none() {
                failed.push(
                    outcomes
                        .failed(
                            Some(&fresh),
                            node,
                            "reports no containerRuntimeVersion and \
                             --require-node-runtime-version is set"
                                .to_string(),
                        )
                        .await,
                );
                continue;
            }
            if args.require_node_machine_id && node_facts.machine_id.is_none() {
                failed.push(
                    outcomes
                        .failed(
                            Some(&fresh),
                            node,
                            "reports no machineID and --require-node-machine-id is set, so a \
                             token-free Job could not defend against node-name reuse"
                                .to_string(),
                        )
                        .await,
                );
                continue;
            }

            // Fails the node rather than proceeding: for cleanup, proceeding would
            // dismantle a node still advertising itself as ready.
            if let Err(err) = node_ops.before_dispatch(node, expected_uid).await {
                failed.push(
                    outcomes
                        .failed(Some(&fresh), node, format!("{err:#}"))
                        .await,
                );
                continue;
            }

            let name = job_name(&owner_value, node);
            let node_job = build_node_job(
                template,
                &name,
                node,
                &owner_value,
                owner,
                Some(&node_facts),
                tracking,
            );
            match jobs.create(&post, &node_job).await {
                Ok(_) => info!("created job {name} (node {node})"),
                // Job names derive from the node and the name prefix, so a 409 is
                // either this run's own Job (the dispatcher restarted) or one left
                // by a previous run - and those need opposite treatment.
                Err(kube::Error::Api(e)) if e.code == 409 => {
                    match adopt_or_replace(jobs, &name, &node_job, &owner_value, owner, tracking)
                        .await
                    {
                        Ok(Adoption::Adopted) => {
                            info!("job {name} (node {node}) is this run's own, adopting it")
                        }
                        Ok(Adoption::Recreated) => {
                            info!("job {name} (node {node}) was left by an earlier run, recreated")
                        }
                        Err(err) => {
                            failed.push(
                                outcomes
                                    .failed(Some(&fresh), node, format!("{err:#}"))
                                    .await,
                            );
                            continue;
                        }
                    }
                }
                Err(e) => {
                    failed.push(
                        outcomes
                            .failed(
                                Some(&fresh),
                                node,
                                format!("its job {name} could not be created: {e}"),
                            )
                            .await,
                    );
                    continue;
                }
            }
            dispatched_at.insert(name.clone(), Instant::now());
            in_flight.insert(name, node.to_string());
        }

        if in_flight.is_empty() && post_work.is_empty() {
            break;
        }

        // Nothing to poll for, so wait on what is finishing rather than spinning
        // on the poll interval.
        if in_flight.is_empty() {
            if let Some(joined) = post_work.join_next_with_id().await {
                record_post_work(
                    joined,
                    &mut post_nodes,
                    &mut succeeded,
                    &mut failed,
                    &outcomes,
                    &by_name,
                )
                .await;
            }
            continue;
        }

        tokio::time::sleep(poll).await;

        // Read concurrently, each with a timeout of its own, so one slow request
        // cannot hold every other node behind it. GET rather than LIST, so the
        // Role needs no `list` on batch/jobs for the polling itself.
        let mut finished: Vec<String> = Vec::new();
        let mut reads = JoinSet::new();
        for (name, node) in &in_flight {
            unreadable.entry(name.clone()).or_insert_with(Instant::now);
            let api = jobs.clone();
            let name = name.clone();
            let node = node.clone();
            reads.spawn(async move {
                let result = tokio::time::timeout(JOB_GET_TIMEOUT, api.get(&name)).await;
                (name, node, result)
            });
        }

        while let Some(joined) = reads.join_next().await {
            let (name, node, result) =
                joined.context("a Job status polling task terminated unexpectedly")?;
            let j = match result {
                Ok(Ok(j)) => j,
                // Waiting for a Job that no longer exists never ends, so the node
                // fails and a later run retries it.
                Ok(Err(kube::Error::Api(e))) if e.code == 404 => {
                    failed.push(
                        outcomes
                            .failed(
                                by_name.get(node.as_str()).copied(),
                                &node,
                                format!(
                                    "its job {name} no longer exists, so its result cannot be \
                                     established"
                                ),
                            )
                            .await,
                    );
                    finished.push(name);
                    continue;
                }
                Ok(Err(e)) => {
                    let since = unreadable[&name];
                    if since.elapsed() >= JOB_READ_ERROR_BUDGET {
                        failed.push(
                            outcomes
                                .failed(
                                    by_name.get(node.as_str()).copied(),
                                    &node,
                                    format!(
                                        "its job {name} has been unreadable for {}s ({e}), so its \
                                         result cannot be established",
                                        since.elapsed().as_secs()
                                    ),
                                )
                                .await,
                        );
                        finished.push(name);
                    } else {
                        error!("failed to get job {name} (node {node}): {e}");
                    }
                    continue;
                }
                Err(_) => {
                    let since = unreadable[&name];
                    if since.elapsed() >= JOB_READ_ERROR_BUDGET {
                        failed.push(
                            outcomes
                                .failed(
                                    by_name.get(node.as_str()).copied(),
                                    &node,
                                    format!(
                                        "its job {name} has been unreadable for {}s because its \
                                         GET requests keep timing out after {}s, so its result \
                                         cannot be established",
                                        since.elapsed().as_secs(),
                                        JOB_GET_TIMEOUT.as_secs()
                                    ),
                                )
                                .await,
                        );
                        finished.push(name);
                    } else {
                        error!(
                            "timed out after {}s getting job {name} (node {node})",
                            JOB_GET_TIMEOUT.as_secs()
                        );
                    }
                    continue;
                }
            };
            unreadable.remove(&name);
            match interpret_status(&j) {
                JobOutcome::Succeeded => {
                    finished.push(name.clone());
                    info!("node {node}: job {name} succeeded");
                    let Some(expected_uid) = by_name
                        .get(node.as_str())
                        .and_then(|candidate| candidate.metadata.uid.clone())
                    else {
                        failed.push(
                            outcomes
                                .failed(
                                    None,
                                    &node,
                                    "the selected Node UID is gone from dispatcher state"
                                        .to_string(),
                                )
                                .await,
                        );
                        continue;
                    };
                    let ops = node_ops.clone();
                    let target = node.clone();
                    let handle = post_work
                        .spawn(async move { ops.after_success(&target, &expected_uid).await });
                    post_nodes.insert(handle.id(), node);
                }
                JobOutcome::Failed => {
                    // Read now, before the Job's TTL takes the pod away.
                    let last_seen = reported_waiting
                        .get(&name)
                        .and_then(|(_, why)| why.as_deref());
                    let why = diagnosis::why_the_job_failed(&pods, &j, &name, last_seen).await;
                    failed.push(
                        outcomes
                            .failed(
                                by_name.get(node.as_str()).copied(),
                                &node,
                                format!("its job {name} failed: {why}"),
                            )
                            .await,
                    );
                    finished.push(name);
                }
                JobOutcome::Running => {
                    report_if_stalled(
                        &pods,
                        &outcomes,
                        by_name.get(node.as_str()).copied(),
                        &node,
                        &name,
                        &dispatched_at,
                        &mut reported_waiting,
                    )
                    .await;
                }
            }
        }
        for name in finished {
            in_flight.remove(&name);
            unreadable.remove(&name);
            dispatched_at.remove(&name);
            reported_waiting.remove(&name);
        }

        while let Some(joined) = post_work.try_join_next_with_id() {
            record_post_work(
                joined,
                &mut post_nodes,
                &mut succeeded,
                &mut failed,
                &outcomes,
                &by_name,
            )
            .await;
        }

        progress.report(Tally {
            succeeded,
            failed: failed.len(),
            in_flight: in_flight.len(),
            finishing: post_work.len(),
            queued: queue.len(),
        });
    }

    if !failed.is_empty() {
        bail!(summarize(failed, namespace, &tracking.owner, &owner_value));
    }

    info!("all {succeeded} node(s) completed successfully");
    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Tally {
    succeeded: usize,
    failed: usize,
    in_flight: usize,
    finishing: usize,
    queued: usize,
}

#[derive(Default)]
struct Progress {
    said: Option<Tally>,
    said_at: Option<Instant>,
}

impl Progress {
    fn report(&mut self, tally: Tally) {
        if !self.worth_saying(&tally) {
            return;
        }
        self.said = Some(tally);
        self.said_at = Some(Instant::now());
        let Tally {
            succeeded,
            failed,
            in_flight,
            finishing,
            queued,
        } = tally;
        info!(
            "progress: {succeeded} succeeded, {failed} failed, {in_flight} in-flight, \
             {finishing} finishing, {queued} queued"
        );
    }

    fn worth_saying(&self, tally: &Tally) -> bool {
        if self.said.as_ref() != Some(tally) {
            return true;
        }
        self.said_at
            .is_none_or(|at| at.elapsed() >= PROGRESS_HEARTBEAT)
    }
}

/// The run's last word, and for anyone who ran `helm install` the only one
/// they will see.
fn summarize(
    mut failed: Vec<NodeFailure>,
    namespace: &str,
    owner_label: &str,
    owner_value: &str,
) -> String {
    failed.sort_by(|left, right| left.node.cmp(&right.node));
    // Two entries mean two paths reported one node; the first is what happened.
    failed.dedup_by(|left, right| left.node == right.node);

    let mut summary = format!("{} node(s) failed:", failed.len());
    for failure in &failed {
        summary.push_str(&format!("\n  {}: {}", failure.node, failure.reason));
    }
    summary.push_str(&format!(
        "\nThe full logs of the per-node Jobs that are still around: \
         kubectl logs -n {namespace} -l {owner_label}={owner_value} --all-containers --prefix"
    ));
    summary
}

/// Say what a Job that is getting nowhere is waiting for, since running is not
/// a state the Job controller reports on.
async fn report_if_stalled(
    pods: &Api<Pod>,
    outcomes: &Outcomes,
    node: Option<&Node>,
    name: &str,
    job: &str,
    dispatched_at: &HashMap<String, Instant>,
    reported: &mut HashMap<String, (Instant, Option<String>)>,
) {
    let waited = match dispatched_at.get(job) {
        Some(since) => since.elapsed(),
        // Adopted from an earlier run, which is the one that knows.
        None => return,
    };
    if waited < WAITING_REPORT_AFTER {
        return;
    }
    if let Some((last, _)) = reported.get(job) {
        if last.elapsed() < WAITING_REPORT_EVERY {
            return;
        }
    }

    // Recorded either way: asking costs a pod LIST, and a running Job answers
    // nothing to every poll.
    let asked_at = Instant::now();
    let Some(detail) = diagnosis::what_the_job_waits_for(pods, job).await else {
        let kept = reported.get(job).and_then(|(_, why)| why.clone());
        reported.insert(job.to_string(), (asked_at, kept));
        return;
    };

    reported.insert(job.to_string(), (asked_at, Some(detail.clone())));
    outcomes
        .waiting(
            node,
            name,
            &format!(
                "its job {job} has been going {}s without running: {detail}",
                waited.as_secs()
            ),
        )
        .await;
}

/// The Job driving another run of this name prefix, if one is still working.
///
/// The sweep below deletes a Job owned by anyone but us as an earlier run's
/// leftover, which is right once that run has ended and wrong while it has not:
/// two live runs would delete each other's privileged pods mid-work. Only an
/// unfinished owner counts, since leftovers are what the sweep is for.
async fn live_run_holding_the_fleet(
    jobs: &Api<Job>,
    owner_value: &str,
    owner: &OwnerReference,
    tracking: &TrackingLabels,
) -> Result<Option<String>> {
    let selector = format!("{}={}", tracking.owner, owner_value);
    let mut token: Option<String> = None;
    let mut asked: HashSet<String> = HashSet::new();
    loop {
        let mut params = ListParams::default().labels(&selector).limit(500);
        if let Some(value) = token.as_deref() {
            params = params.continue_token(value);
        }
        let page = jobs.list(&params).await.with_context(|| {
            format!("failed to list the per-node Jobs owned by {owner_value} to see who holds them")
        })?;

        for job in &page.items {
            let Some(other) = foreign_owner_of(job, owner) else {
                continue;
            };
            if !asked.insert(other.uid.clone()) {
                continue;
            }
            match jobs.get(&other.name).await {
                // Gone, so whatever it left is the sweep's to take.
                Err(kube::Error::Api(status)) if status.code == 404 => (),
                Err(err) => {
                    return Err(err).with_context(|| {
                        format!(
                            "failed to read the Job {} that owns per-node Jobs of this name \
                             prefix; without knowing whether it is still running, this run can \
                             neither take them over nor stand aside",
                            other.name
                        )
                    })
                }
                // The name was reused by a later Job, so this says nothing about the
                // run being asked about.
                Ok(found) if found.metadata.uid.as_deref() != Some(other.uid.as_str()) => (),
                Ok(found) if interpret_status(&found) == JobOutcome::Running => {
                    return Ok(Some(other.name))
                }
                Ok(_) => (),
            }
        }

        token = page.metadata.continue_;
        if token.as_deref().is_none_or(str::is_empty) {
            return Ok(None);
        }
    }
}

/// An orphan yields `None`: there is no run behind it to stand aside for.
fn foreign_owner_of(job: &Job, ours: &OwnerReference) -> Option<OwnerReference> {
    job.metadata
        .owner_references
        .iter()
        .flatten()
        .find(|reference| references_a_batch_job(reference) && reference.uid != ours.uid)
        .cloned()
}

/// Delete the Jobs an earlier run left behind, before any slot is opened. Their
/// pods still count against the parallelism the caller asked for, and a
/// privileged pod from a previous run doing the same work concurrently is exactly
/// what the pacing exists to prevent.
async fn clear_stale_jobs(
    jobs: &Api<Job>,
    owner_value: &str,
    owner: Option<&OwnerReference>,
    tracking: &TrackingLabels,
) -> Result<()> {
    // Without an owner reference this run cannot tell its own Jobs from an
    // earlier run's, so there is nothing safe to delete.
    let Some(owner) = owner else {
        return Ok(());
    };
    let selector = format!("{}={}", tracking.owner, owner_value);
    let stale = stale_job_names(jobs, &selector, owner_value, owner, tracking).await?;

    let mut pending: VecDeque<String> = stale.into();
    let mut deletes = JoinSet::new();
    const DELETE_CONCURRENCY: usize = 16;
    while !pending.is_empty() || !deletes.is_empty() {
        while deletes.len() < DELETE_CONCURRENCY {
            let Some(name) = pending.pop_front() else {
                break;
            };
            info!("deleting the stale per-node job {name} before opening rollout slots");
            let jobs = jobs.clone();
            deletes.spawn(async move {
                match jobs.delete(&name, &DeleteParams::foreground()).await {
                    Ok(_) => Ok(()),
                    Err(kube::Error::Api(status)) if status.code == 404 => Ok(()),
                    Err(err) => Err(err)
                        .with_context(|| format!("failed to delete the stale per-node job {name}")),
                }
            });
        }
        if let Some(result) = deletes.join_next().await {
            result.context("a stale Job deletion task failed")??;
        }
    }

    for _ in 0..REPLACE_ATTEMPTS {
        if !stale_job_exists(jobs, &selector, owner_value, owner, tracking).await? {
            return Ok(());
        }
        tokio::time::sleep(REPLACE_INTERVAL).await;
    }

    bail!(
        "stale per-node Jobs were still terminating after {}s; refusing to start more Jobs beyond \
         the configured parallelism",
        (REPLACE_ATTEMPTS as u64) * REPLACE_INTERVAL.as_secs()
    )
}

/// Paginated: a cluster large enough to need pacing is large enough for one page
/// of Jobs to hide the rest.
async fn stale_job_names(
    jobs: &Api<Job>,
    selector: &str,
    owner_value: &str,
    owner: &OwnerReference,
    tracking: &TrackingLabels,
) -> Result<Vec<String>> {
    let mut token: Option<String> = None;
    let mut names = Vec::new();
    loop {
        let mut params = ListParams::default().labels(selector).limit(500);
        if let Some(value) = token.as_deref() {
            params = params.continue_token(value);
        }
        let page = jobs
            .list(&params)
            .await
            .with_context(|| format!("failed to list the per-node Jobs owned by {owner_value}"))?;
        names.extend(
            page.items
                .into_iter()
                .filter(|job| {
                    job_disposition(job, owner_value, Some(owner), tracking) == Disposition::Stale
                })
                .filter_map(|job| job.metadata.name),
        );
        token = page.metadata.continue_;
        if token.as_deref().is_none_or(str::is_empty) {
            return Ok(names);
        }
    }
}

async fn stale_job_exists(
    jobs: &Api<Job>,
    selector: &str,
    owner_value: &str,
    owner: &OwnerReference,
    tracking: &TrackingLabels,
) -> Result<bool> {
    let mut token: Option<String> = None;
    loop {
        let mut params = ListParams::default().labels(selector).limit(500);
        if let Some(value) = token.as_deref() {
            params = params.continue_token(value);
        }
        let page = jobs
            .list(&params)
            .await
            .with_context(|| format!("failed to wait for the stale Jobs owned by {owner_value}"))?;
        if page.items.iter().any(|job| {
            job_disposition(job, owner_value, Some(owner), tracking) == Disposition::Stale
        }) {
            return Ok(true);
        }
        token = page.metadata.continue_;
        if token.as_deref().is_none_or(str::is_empty) {
            return Ok(false);
        }
    }
}

enum Adoption {
    Adopted,
    Recreated,
}

async fn adopt_or_replace(
    jobs: &Api<Job>,
    name: &str,
    desired: &Job,
    owner_value: &str,
    owner: Option<&OwnerReference>,
    tracking: &TrackingLabels,
) -> Result<Adoption> {
    let existing = jobs
        .get(name)
        .await
        .with_context(|| format!("failed to fetch the pre-existing job {name}"))?;

    match job_disposition(&existing, owner_value, owner, tracking) {
        Disposition::NotOurs => bail!(
            "job {name} already exists but is not labeled {}={owner_value}; refusing to adopt a \
             Job that is not this run's",
            tracking.owner
        ),
        Disposition::Stale => {
            replace_job(jobs, name, desired).await?;
            Ok(Adoption::Recreated)
        }
        Disposition::Current => Ok(Adoption::Adopted),
    }
}

#[derive(Debug, PartialEq)]
enum Disposition {
    NotOurs,
    Stale,
    Current,
}

/// The owner label cannot tell this run's Job from an earlier run's: it holds the
/// name prefix, which is the same every time. An `ownerReference` to *this*
/// dispatcher can, and the distinction matters because a finished Job from the
/// previous run would otherwise be read as this one's result.
fn job_disposition(
    existing: &Job,
    owner_value: &str,
    owner: Option<&OwnerReference>,
    tracking: &TrackingLabels,
) -> Disposition {
    if !job_owned_by(existing, tracking, owner_value) {
        return Disposition::NotOurs;
    }

    // Without an owner of our own there is nothing to compare against.
    let Some(owner) = owner else {
        return Disposition::Current;
    };

    if existing
        .metadata
        .owner_references
        .iter()
        .flatten()
        .any(|reference| reference.uid == owner.uid)
    {
        Disposition::Current
    } else {
        Disposition::Stale
    }
}

const REPLACE_ATTEMPTS: u32 = 30;
const REPLACE_INTERVAL: Duration = Duration::from_secs(2);

/// Deletion is asynchronous and the name is only free once it completes, so the
/// create is retried while the apiserver still answers 409.
async fn replace_job(jobs: &Api<Job>, name: &str, desired: &Job) -> Result<()> {
    // Foreground, so the Job outlives its pods rather than the other way round:
    // two of these pods on one node would both do the same privileged work at the
    // same time.
    let delete = DeleteParams::foreground();
    match jobs.delete(name, &delete).await {
        Ok(_) => (),
        Err(kube::Error::Api(e)) if e.code == 404 => (),
        Err(err) => {
            return Err(err).with_context(|| format!("failed to delete the stale job {name}"))
        }
    }

    let post = PostParams::default();
    for _ in 0..REPLACE_ATTEMPTS {
        match jobs.create(&post, desired).await {
            Ok(_) => return Ok(()),
            Err(kube::Error::Api(e)) if e.code == 409 => {
                tokio::time::sleep(REPLACE_INTERVAL).await;
            }
            Err(err) => {
                return Err(err).with_context(|| format!("failed to recreate the job {name}"))
            }
        }
    }

    bail!(
        "the stale job {name} was still being deleted {}s after it was asked to go; giving up on \
         recreating it",
        (REPLACE_ATTEMPTS as u64) * REPLACE_INTERVAL.as_secs()
    )
}

/// A node whose Job passed but whose post-success work did not is a failed node:
/// the work is only complete once the node is labelled.
async fn record_post_work(
    joined: std::result::Result<(tokio::task::Id, Result<()>), tokio::task::JoinError>,
    post_nodes: &mut HashMap<tokio::task::Id, String>,
    succeeded: &mut usize,
    failed: &mut Vec<NodeFailure>,
    outcomes: &Outcomes,
    by_name: &HashMap<&str, &Node>,
) {
    let (id, outcome) = match joined {
        Ok((id, outcome)) => (id, outcome),
        Err(err) => (err.id(), Err(anyhow::anyhow!("{err}"))),
    };
    let node = post_nodes
        .remove(&id)
        .unwrap_or_else(|| "<unknown>".to_string());
    let object = by_name.get(node.as_str()).copied();

    match outcome {
        Ok(()) => {
            *succeeded += 1;
            info!("node {node}: done");
            outcomes.succeeded(object, &node).await;
        }
        Err(err) => {
            failed.push(
                outcomes
                    .failed(object, &node, format!("its Job finished but {err:#}"))
                    .await,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::batch::v1::JobSpec;
    use k8s_openapi::api::core::v1::{PodSpec, PodTemplateSpec, Toleration};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use std::collections::BTreeMap;

    #[test]
    fn no_selector_means_one_unfiltered_pass() {
        assert_eq!(selector_passes(&[]), vec![None]);
    }

    #[test]
    fn a_tally_is_said_when_it_changes_and_when_it_goes_quiet() {
        let waiting = Tally {
            succeeded: 0,
            failed: 0,
            in_flight: 1,
            finishing: 0,
            queued: 0,
        };
        let mut progress = Progress::default();

        assert!(progress.worth_saying(&waiting));
        progress.report(waiting);
        assert!(!progress.worth_saying(&waiting));
        assert!(progress.worth_saying(&Tally {
            succeeded: 1,
            in_flight: 0,
            ..waiting
        }));

        progress.said_at = Some(Instant::now() - PROGRESS_HEARTBEAT);
        assert!(progress.worth_saying(&waiting));
    }

    #[test]
    fn one_selector_means_one_filtered_pass() {
        let selectors = vec!["kubernetes.io/os=linux".to_string()];
        assert_eq!(
            selector_passes(&selectors),
            vec![Some("kubernetes.io/os=linux")]
        );
    }

    #[test]
    fn repeated_selectors_become_one_pass_each() {
        let selectors = vec![
            "feature.node.kubernetes.io/cpu-cpuid.VMX=true".to_string(),
            "feature.node.kubernetes.io/cpu-cpuid.SVM=true".to_string(),
        ];
        assert_eq!(
            selector_passes(&selectors),
            vec![
                Some("feature.node.kubernetes.io/cpu-cpuid.VMX=true"),
                Some("feature.node.kubernetes.io/cpu-cpuid.SVM=true"),
            ]
        );
    }

    fn owner(uid: &str) -> OwnerReference {
        OwnerReference {
            uid: uid.to_string(),
            name: "rollout-install".to_string(),
            kind: "Job".to_string(),
            api_version: "batch/v1".to_string(),
            ..Default::default()
        }
    }

    fn existing_job(owner_label: &str, owned_by: Option<&OwnerReference>) -> Job {
        let mut job = Job {
            metadata: ObjectMeta {
                name: Some("rollout-install-node1".to_string()),
                labels: Some(BTreeMap::from([(
                    TrackingLabels::default().owner,
                    owner_label.to_string(),
                )])),
                ..Default::default()
            },
            ..Default::default()
        };
        job.metadata.owner_references = owned_by.map(|reference| vec![reference.clone()]);
        job
    }

    fn disposition_of(
        existing: &Job,
        owner_value: &str,
        owner: Option<&OwnerReference>,
    ) -> Disposition {
        job_disposition(existing, owner_value, owner, &TrackingLabels::default())
    }

    #[test]
    fn a_job_from_another_run_is_never_adopted() {
        let existing = existing_job("someone-else", Some(&owner("uid-1")));
        assert_eq!(
            disposition_of(&existing, "rollout-install", Some(&owner("uid-1"))),
            Disposition::NotOurs
        );
    }

    #[test]
    fn a_job_from_an_earlier_run_is_replaced() {
        let existing = existing_job("rollout-install", Some(&owner("uid-old")));
        assert_eq!(
            disposition_of(&existing, "rollout-install", Some(&owner("uid-new"))),
            Disposition::Stale
        );

        let orphan = existing_job("rollout-install", None);
        assert_eq!(
            disposition_of(&orphan, "rollout-install", Some(&owner("uid-new"))),
            Disposition::Stale
        );
    }

    #[test]
    fn this_runs_own_job_is_adopted() {
        let existing = existing_job("rollout-install", Some(&owner("uid-1")));
        assert_eq!(
            disposition_of(&existing, "rollout-install", Some(&owner("uid-1"))),
            Disposition::Current
        );

        // Outside a Helm hook there is no owner, so the label is all there is.
        let existing = existing_job("rollout-install", None);
        assert_eq!(
            disposition_of(&existing, "rollout-install", None),
            Disposition::Current
        );
    }

    fn pod_owned_by(references: &[OwnerReference]) -> Pod {
        Pod {
            metadata: ObjectMeta {
                name: Some("rollout-reconcile-29283840-abcde".to_string()),
                owner_references: Some(references.to_vec()),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn controller(kind: &str, api_version: &str) -> OwnerReference {
        OwnerReference {
            uid: "uid-of-the-run".to_string(),
            name: "rollout-reconcile-29283840".to_string(),
            kind: kind.to_string(),
            api_version: api_version.to_string(),
            controller: Some(true),
            block_owner_deletion: Some(true),
        }
    }

    #[test]
    fn the_job_that_created_the_pod_becomes_the_owner() {
        let pod = pod_owned_by(&[controller("Job", "batch/v1")]);
        let owner = owning_job_of_pod(&pod, "rollout-reconcile-29283840-abcde").unwrap();

        assert_eq!(owner.name, "rollout-reconcile-29283840");
        assert_eq!(owner.uid, "uid-of-the-run");
        // Never a controller reference, whatever the pod's own says.
        assert_eq!(owner.controller, Some(false));
        assert_eq!(owner.block_owner_deletion, Some(false));
    }

    #[test]
    fn a_pod_no_job_created_has_no_owner_to_offer() {
        for pod in [
            pod_owned_by(&[]),
            pod_owned_by(&[controller("ReplicaSet", "apps/v1")]),
            pod_owned_by(&[controller("Job", "example.com/v1")]),
            Pod::default(),
        ] {
            assert!(owning_job_of_pod(&pod, "some-pod").is_err());
        }
    }

    #[test]
    fn the_owning_job_is_found_among_other_references() {
        let pod = pod_owned_by(&[
            controller("ReplicaSet", "apps/v1"),
            controller("Job", "batch/v1"),
        ]);

        assert_eq!(
            owning_job_of_pod(&pod, "some-pod").unwrap().name,
            "rollout-reconcile-29283840"
        );
    }

    #[test]
    fn a_job_this_run_owns_has_no_other_run_behind_it() {
        let ours = owner("uid-1");
        assert_eq!(
            foreign_owner_of(&existing_job("rollout-install", Some(&ours)), &ours),
            None
        );
    }

    #[test]
    fn a_job_owned_by_another_run_names_it() {
        let theirs = OwnerReference {
            name: "rollout-install-dispatcher".to_string(),
            ..owner("uid-theirs")
        };
        let found = foreign_owner_of(
            &existing_job("rollout-install", Some(&theirs)),
            &owner("uid-ours"),
        );

        assert_eq!(
            found.map(|reference| reference.name).as_deref(),
            Some("rollout-install-dispatcher")
        );
    }

    #[test]
    fn a_job_with_nobody_behind_it_is_the_sweeps_to_take() {
        let outsider = OwnerReference {
            api_version: "example.com/v1".to_string(),
            ..owner("uid-theirs")
        };
        for job in [
            existing_job("rollout-install", None),
            existing_job("rollout-install", Some(&outsider)),
        ] {
            assert_eq!(foreign_owner_of(&job, &owner("uid-ours")), None);
        }
    }

    #[test]
    fn yielding_needs_an_owner_to_compare_against() {
        let base = [
            "k8s-job-dispatcher",
            "--job-template=/etc/job/install-job.yaml",
            "--name-prefix=rollout-install",
            "--yield-to-live-run",
        ];
        assert!(Args::try_parse_from(base).is_err());

        let mut named = base.to_vec();
        named.push("--owner-job-name=rollout-install-dispatcher");
        assert!(Args::try_parse_from(named).is_ok());

        let mut from_pod = base.to_vec();
        from_pod.push("--owner-job-from-pod=rollout-reconcile-29283840-abcde");
        assert!(Args::try_parse_from(from_pod).is_ok());
    }

    #[test]
    fn skipping_satisfied_nodes_needs_the_label_that_says_so() {
        let base = [
            "k8s-job-dispatcher",
            "--job-template=/etc/job/install-job.yaml",
            "--name-prefix=rollout-install",
            "--skip-satisfied-nodes",
        ];
        assert!(Args::try_parse_from(base).is_err());

        let mut labelled = base.to_vec();
        labelled.extend(["--node-label-key=example.com/ready", "--node-label=true"]);
        assert!(Args::try_parse_from(labelled).is_ok());
    }

    #[test]
    fn the_owner_job_is_either_named_or_taken_from_a_pod() {
        assert!(Args::try_parse_from([
            "k8s-job-dispatcher",
            "--job-template=/etc/job/install-job.yaml",
            "--name-prefix=rollout-install",
            "--owner-job-name=rollout-install-dispatcher",
            "--owner-job-from-pod=rollout-reconcile-29283840-abcde",
        ])
        .is_err());
    }

    fn job_with_pod_spec(spec: Option<PodSpec>) -> Job {
        Job {
            spec: Some(JobSpec {
                template: PodTemplateSpec {
                    spec,
                    ..Default::default()
                },
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn template_admission_is_read_from_the_pod_spec() {
        let toleration = Toleration {
            key: Some("node-role.kubernetes.io/control-plane".to_string()),
            operator: Some("Exists".to_string()),
            ..Default::default()
        };
        let template = job_with_pod_spec(Some(PodSpec {
            tolerations: Some(vec![toleration.clone()]),
            host_network: Some(true),
            ..Default::default()
        }));

        let admission = template_admission(&template);
        assert_eq!(admission.tolerations, vec![toleration]);
        assert!(admission.host_network);
    }

    #[test]
    fn a_template_without_tolerations_admits_nothing_extra() {
        for template in [job_with_pod_spec(None), Job::default()] {
            let admission = template_admission(&template);
            assert!(admission.tolerations.is_empty());
            assert!(!admission.host_network);
        }
    }

    fn args_from(extra: &[&str]) -> Args {
        let mut argv = vec![
            "k8s-job-dispatcher",
            "--job-template=/etc/job/install-job.yaml",
            "--name-prefix=rollout-install",
        ];
        argv.extend_from_slice(extra);
        Args::parse_from(argv)
    }

    fn skipped_node(name: &str) -> SkippedNode {
        SkippedNode {
            name: name.to_string(),
            taint: k8s_openapi::api::core::v1::Taint {
                key: "node-role.kubernetes.io/control-plane".to_string(),
                effect: "NoSchedule".to_string(),
                ..Default::default()
            },
        }
    }

    #[test]
    fn nodes_are_resolved_once_by_default() {
        assert_eq!(args_from(&[]).wait_for_nodes_secs, 0);
    }

    /// Only meaningful while waiting: without a wait there is nothing to settle.
    #[test]
    fn the_eligible_set_has_to_settle_before_it_is_accepted() {
        assert_eq!(args_from(&[]).node_settle_secs, 15);
        assert_eq!(args_from(&["--node-settle-secs=60"]).node_settle_secs, 60);
    }

    #[test]
    fn converging_removed_nodes_needs_an_ownership_label() {
        let err = cleanup_ownership_key(None)
            .expect_err("without a label there is no record of what this instance owns");
        assert!(err.to_string().contains("--node-label-key"), "{err}");
    }

    #[test]
    fn a_lone_instance_owns_nodes_through_the_shared_key() {
        let labelling = labelling_from_args(&args_from(&[
            "--node-label-key=example.com/ready",
            "--node-label=true",
        ]))
        .expect("a key and a value are enough")
        .expect("labelling was asked for");

        assert_eq!(
            cleanup_ownership_key(Some(&labelling)).unwrap(),
            "example.com/ready"
        );
    }

    #[test]
    fn one_instance_among_many_owns_nodes_through_its_marker() {
        let labelling = labelling_from_args(&args_from(&[
            "--node-label-key=example.com/ready",
            "--node-label=true",
            "--instance-label-prefix=deployer.example.com",
            "--multi-install-suffix=dev",
        ]))
        .expect("a complete set of flags is valid")
        .expect("labelling was asked for");

        assert_eq!(
            cleanup_ownership_key(Some(&labelling)).unwrap(),
            "deployer.example.com/dev"
        );
    }

    /// The cleanup pass reads a node back in the selection as one to leave alone,
    /// so it must not inherit the labelling that would advertise it.
    #[test]
    fn the_removed_node_pass_is_not_a_flag() {
        assert!(!args_from(&[]).removed_node_cleanup);
    }

    #[test]
    fn an_empty_selection_is_a_no_op_when_we_never_waited() {
        let nodes = no_eligible_nodes(&args_from(&[]), &[]).expect("empty selection is a no-op");
        assert!(nodes.is_empty());
    }

    #[test]
    fn an_empty_selection_fails_once_the_wait_expires() {
        let args = args_from(&[
            "--wait-for-nodes-secs=120",
            "--node-selector=feature.node.kubernetes.io/cpu-cpuid.SVM in (true)",
        ]);
        let err = no_eligible_nodes(&args, &[]).expect_err("waiting means nodes were expected");
        let msg = err.to_string();

        assert!(msg.contains("after waiting 120s"), "{msg}");
        assert!(
            msg.contains("[feature.node.kubernetes.io/cpu-cpuid.SVM in (true)]"),
            "{msg}"
        );
    }

    #[test]
    fn nodes_matched_but_all_tainted_are_a_no_op_when_we_never_waited() {
        let nodes = no_eligible_nodes(&args_from(&[]), &[skipped_node("cp-0")])
            .expect("a repeating run leaves a node on its way up for its next pass");
        assert!(nodes.is_empty());
    }

    #[test]
    fn nodes_matched_but_all_tainted_fail_with_the_missing_toleration() {
        let args = args_from(&["--wait-for-nodes-secs=120"]);
        let err = no_eligible_nodes(&args, &[skipped_node("cp-0")])
            .expect_err("a fully tainted selection has nowhere to dispatch to");
        let msg = err.to_string();

        assert!(msg.contains("node cp-0"), "{msg}");
        assert!(
            msg.contains("node-role.kubernetes.io/control-plane:NoSchedule"),
            "{msg}"
        );
        assert!(msg.contains("operator: Exists"), "{msg}");
    }

    #[test]
    fn selectors_are_spelled_out_for_errors() {
        assert_eq!(describe_selectors(&[]), "<none> (every node)");
        assert_eq!(
            describe_selectors(&["a=b".to_string(), "c=d".to_string()]),
            "[a=b], [c=d]"
        );
    }

    #[test]
    fn no_labelling_is_configured_by_default() {
        assert!(labelling_from_args(&args_from(&[]))
            .expect("no labelling flags is valid")
            .is_none());
    }

    #[test]
    fn a_node_label_needs_its_key() {
        for flag in [
            "--node-label=true",
            "--remove-node-label",
            "--claim-node-pending",
        ] {
            let err = labelling_from_args(&args_from(&[flag]))
                .expect_err("a node label without a key cannot be written");
            assert!(err.to_string().contains("--node-label-key"), "{err}");
        }
    }

    #[test]
    fn a_key_without_an_action_is_rejected() {
        let err = labelling_from_args(&args_from(&["--node-label-key=example.com/ready"]))
            .expect_err("a key alone writes nothing");
        assert!(err.to_string().contains("writes nothing"), "{err}");
    }

    /// Otherwise it would be silently ignored, which is how two instances end up
    /// clobbering each other.
    #[test]
    fn an_instance_name_needs_its_prefix() {
        let err = labelling_from_args(&args_from(&[
            "--node-label-key=example.com/ready",
            "--node-label=true",
            "--multi-install-suffix=dev",
        ]))
        .expect_err("an instance name without a prefix is ignored otherwise");
        assert!(err.to_string().contains("--instance-label-prefix"), "{err}");
    }

    #[test]
    fn labelling_is_built_from_the_flags() {
        let labelling = labelling_from_args(&args_from(&[
            "--node-label-key=example.com/ready",
            "--node-label=true",
            "--node-label-pending-value=installing",
            "--instance-label-prefix=deployer.example.com",
            "--multi-install-suffix=dev",
        ]))
        .expect("a complete set of flags is valid")
        .expect("labelling was asked for");

        assert_eq!(labelling.key, "example.com/ready");
        assert_eq!(labelling.pending_value, "installing");
        assert_eq!(
            labelling.instance.as_ref().map(InstanceMarker::key),
            Some("deployer.example.com/dev")
        );
    }

    #[test]
    fn the_node_result_keys_follow_the_tracking_prefix() {
        let args = args_from(&["--tracking-label-prefix=deployer.example.com"]);

        let keys = nodes::ResultKeys::with_prefix(&args.tracking_label_prefix);
        assert_eq!(keys.state, "deployer.example.com/result");
    }

    #[test]
    fn the_summary_names_every_node_and_what_stopped_it() {
        let failure = |node: &str, reason: &str| NodeFailure {
            node: node.to_string(),
            reason: reason.to_string(),
        };

        let summary = summarize(
            vec![
                failure(
                    "worker-2",
                    "its job failed: host-check exited 1: no /dev/kvm",
                ),
                failure("worker-1", "its pod is unschedulable"),
            ],
            "kube-system",
            "k8s-job-dispatcher/owner",
            "kata-deploy-install",
        );

        assert!(summary.starts_with("2 node(s) failed:"));
        // Sorted, so two runs over the same fleet read the same way.
        assert!(summary.contains("\n  worker-1: its pod is unschedulable\n  worker-2: its job failed: host-check exited 1: no /dev/kvm"));
        assert!(summary.contains(
            "kubectl logs -n kube-system -l k8s-job-dispatcher/owner=kata-deploy-install"
        ));
    }

    /// Counting a node twice would misstate how much of the fleet is affected.
    #[test]
    fn a_node_is_summarized_once() {
        let summary = summarize(
            vec![
                NodeFailure {
                    node: "worker-1".to_string(),
                    reason: "first".to_string(),
                },
                NodeFailure {
                    node: "worker-1".to_string(),
                    reason: "second".to_string(),
                },
            ],
            "kube-system",
            "owner",
            "run",
        );

        assert!(summary.starts_with("1 node(s) failed:"));
        assert!(summary.contains("worker-1: first"));
        assert!(!summary.contains("second"));
    }

    #[test]
    fn a_single_instance_needs_no_marker() {
        let labelling = labelling_from_args(&args_from(&[
            "--node-label-key=example.com/ready",
            "--node-label=true",
        ]))
        .expect("a key and a value are enough")
        .expect("labelling was asked for");

        assert!(labelling.instance.is_none());
        assert_eq!(labelling.pending_value, DEFAULT_PENDING_LABEL_VALUE);
    }

    #[test]
    fn lifting_taints_needs_something_to_label_with() {
        let err = check_taint_flags(&args_from(&[
            "--node-label-key=example.com/ready",
            "--claim-node-pending",
            "--remove-node-taints=example.com/startup",
        ]))
        .expect_err("a claim is not the label that gates the workloads");
        assert!(err.to_string().contains("--node-label"), "{err}");

        check_taint_flags(&args_from(&[
            "--node-label-key=example.com/ready",
            "--node-label=true",
            "--remove-node-taints=example.com/startup",
        ]))
        .expect("a taint may be lifted once the node is labelled");
    }

    #[test]
    fn extra_labels_are_split_into_pairs() {
        assert_eq!(
            extra_labels_from_args(&[
                "example.com/ready=true".to_string(),
                "example.com/mode=ppcie".to_string(),
            ])
            .unwrap(),
            vec![
                ("example.com/ready".to_string(), "true".to_string()),
                ("example.com/mode".to_string(), "ppcie".to_string()),
            ]
        );
    }

    #[test]
    fn an_extra_label_without_an_equals_is_rejected() {
        let err = extra_labels_from_args(&["example.com/ready".to_string()])
            .expect_err("a bare key has no value to write");
        assert!(err.to_string().contains("is not key=value"), "{err}");
    }

    #[test]
    fn only_the_first_equals_splits_an_extra_label() {
        assert_eq!(
            extra_labels_from_args(&["example.com/pair=a=b".to_string()]).unwrap(),
            vec![("example.com/pair".to_string(), "a=b".to_string())]
        );
    }

    #[test]
    fn one_extra_label_key_cannot_be_given_twice() {
        let err = extra_labels_from_args(&[
            "example.com/ready=true".to_string(),
            "example.com/ready=false".to_string(),
        ])
        .expect_err("one key cannot hold two values");
        assert!(err.to_string().contains("more than once"), "{err}");
    }

    #[test]
    fn an_extra_label_cannot_take_an_ownership_key() {
        let labelling = labelling_from_args(&args_from(&[
            "--node-label-key=example.com/ready",
            "--node-label=true",
            "--instance-label-prefix=deployer.example.com",
            "--multi-install-suffix=dev",
        ]))
        .expect("a complete set of flags is valid");

        for key in ["example.com/ready", "deployer.example.com/dev"] {
            let err = check_extra_label_keys(
                labelling.as_ref(),
                &[(key.to_string(), "true".to_string())],
            )
            .expect_err("an ownership key is not an extra label's to write");
            assert!(err.to_string().contains("ownership key"), "{err}");
        }

        check_extra_label_keys(
            labelling.as_ref(),
            &[("example.com/mode".to_string(), "ppcie".to_string())],
        )
        .expect("a key of its own is fine");
    }

    #[test]
    fn an_extra_label_needs_something_to_write_or_remove_it_with() {
        let err =
            check_extra_label_flags(&args_from(&["--extra-node-label=example.com/ready=true"]))
                .expect_err("nothing writes or removes an extra label on its own");
        assert!(err.to_string().contains("--node-label"), "{err}");

        check_extra_label_flags(&args_from(&[
            "--node-label-key=example.com/ready",
            "--node-label=true",
            "--extra-node-label=example.com/mode=ppcie",
        ]))
        .expect("an extra label alongside --node-label is fine");

        check_extra_label_flags(&args_from(&[
            "--node-label-key=example.com/ready",
            "--remove-node-label",
            "--extra-node-label=example.com/mode=ppcie",
        ]))
        .expect("an extra label alongside --remove-node-label, with no value, is also fine");
    }

    #[test]
    fn the_tracking_prefix_comes_from_the_flags() {
        assert_eq!(
            args_from(&[]).tracking_label_prefix,
            DEFAULT_TRACKING_LABEL_PREFIX
        );
        assert_eq!(
            TrackingLabels::with_prefix(
                &args_from(&["--tracking-label-prefix=example.com/dispatcher"])
                    .tracking_label_prefix
            )
            .owner,
            "example.com/dispatcher/owner"
        );
    }
}
