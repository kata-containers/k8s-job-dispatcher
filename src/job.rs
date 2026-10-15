// Copyright (c) 2026 NVIDIA Corporation
//
// SPDX-License-Identifier: Apache-2.0

use crate::nodes::NodeFacts;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{EnvVar, PodSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

pub const CONTAINER_RUNTIME_VERSION_ENV: &str = "CONTAINER_RUNTIME_VERSION";

/// Lets a Job ask the host it landed on whether it is the machine that was
/// chosen: a Job is bound by node name, and a name can outlive the machine that
/// answered to it.
pub const NODE_MACHINE_ID_ENV: &str = "NODE_MACHINE_ID";

pub const DEFAULT_TRACKING_LABEL_PREFIX: &str = "k8s-job-dispatcher";

/// Limit on both a DNS-1123 label and a Kubernetes label value.
pub const MAX_LABEL_LEN: usize = 63;

/// Keys stamped on every Job the dispatcher creates. They share one caller-chosen
/// prefix so that two dispatchers in a namespace do not read each other's Jobs.
#[derive(Debug, Clone)]
pub struct TrackingLabels {
    pub owner: String,
    pub node: String,
    /// Node names can exceed [`MAX_LABEL_LEN`] or contain characters invalid in a
    /// label value, so the authoritative name lives in an annotation.
    pub node_annotation: String,
}

impl TrackingLabels {
    pub fn with_prefix(prefix: &str) -> Self {
        let prefix = prefix.trim().trim_end_matches('/');
        Self {
            owner: format!("{prefix}/owner"),
            node: format!("{prefix}/node"),
            node_annotation: format!("{prefix}/node-name"),
        }
    }
}

impl Default for TrackingLabels {
    fn default() -> Self {
        Self::with_prefix(DEFAULT_TRACKING_LABEL_PREFIX)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobOutcome {
    Running,
    Succeeded,
    Failed,
}

pub fn sanitize_node(node: &str) -> String {
    let lowered = node.to_ascii_lowercase();
    let mapped: String = lowered
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c
            } else {
                '-'
            }
        })
        .collect();
    mapped.trim_matches('-').to_string()
}

fn short_hash(s: &str) -> String {
    let digest = Sha256::digest(s.as_bytes());
    digest[..12]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// A deterministic, DNS-1123-safe Job name for a node. A name is used verbatim
/// only when sanitizing changed nothing and it fits; otherwise a hash of the full
/// prefix-and-node identity is appended, so that neither a long node name, a long
/// release prefix, nor two names that normalize alike can collide.
///
/// The derivation is free to change between releases: an earlier run's Jobs are
/// found through the owner label and their `ownerReference`, never by recomputing
/// what they would be called today.
pub fn job_name(prefix: &str, node: &str) -> String {
    let sanitized = sanitize_node(node);
    let base = format!("{prefix}-{sanitized}");
    if sanitized == node && base.len() <= MAX_LABEL_LEN {
        return base;
    }
    let hash = short_hash(&format!("{prefix}\0{node}"));
    let keep = MAX_LABEL_LEN.saturating_sub(hash.len() + 1);
    let truncated = base.chars().take(keep).collect::<String>();
    format!("{}-{}", truncated.trim_end_matches('-'), hash)
}

/// Sanitize into a value safe as both a DNS-1123 Job-name prefix and a label
/// value, so that callers can pass a raw value (a Helm release name, say) without
/// risking an invalid or over-long one. Hash-suffixed whenever sanitizing changed
/// the value, so two distinct releases cannot normalize onto one identity.
pub fn sanitize_label_value(value: &str) -> String {
    let sanitized = sanitize_node(value);
    if sanitized == value && sanitized.len() <= MAX_LABEL_LEN {
        return sanitized;
    }
    let hash = short_hash(value);
    let keep = MAX_LABEL_LEN.saturating_sub(hash.len() + 1);
    let prefix = sanitized
        .chars()
        .take(keep)
        .collect::<String>()
        .trim_end_matches('-')
        .to_string();
    format!(
        "{}-{hash}",
        if prefix.is_empty() {
            "dispatcher"
        } else {
            &prefix
        }
    )
}

/// Whether a pre-existing (409) Job is safe to adopt. The dispatcher only ever
/// GETs in-flight Jobs by name, so adopting one it cannot recognise later would
/// leave it stuck in-flight forever.
pub fn job_owned_by(job: &Job, tracking: &TrackingLabels, owner_value: &str) -> bool {
    job.metadata
        .labels
        .as_ref()
        .and_then(|labels| labels.get(&tracking.owner))
        .map(|value| value == owner_value)
        .unwrap_or(false)
}

pub fn build_node_job(
    template: &Job,
    name: &str,
    node: &str,
    owner_value: &str,
    owner: Option<&OwnerReference>,
    facts: Option<&NodeFacts>,
    tracking: &TrackingLabels,
) -> Job {
    let mut job = template.clone();

    job.metadata.name = Some(name.to_string());
    // The apiserver rejects name and generateName both being set.
    job.metadata.generate_name = None;

    let labels = job.metadata.labels.get_or_insert_with(BTreeMap::new);
    labels.insert(tracking.owner.clone(), owner_value.to_string());
    labels.insert(tracking.node.clone(), sanitize_label_value(node));

    let annotations = job.metadata.annotations.get_or_insert_with(BTreeMap::new);
    annotations.insert(tracking.node_annotation.clone(), node.to_string());

    if let Some(owner_ref) = owner {
        job.metadata.owner_references = Some(vec![owner_ref.clone()]);
    }

    let spec = job.spec.get_or_insert_with(Default::default);

    // Mirrored onto the pod template so the pods are selectable too.
    let tmpl_meta = spec.template.metadata.get_or_insert_with(Default::default);
    let tmpl_labels = tmpl_meta.labels.get_or_insert_with(BTreeMap::new);
    tmpl_labels.insert(tracking.owner.clone(), owner_value.to_string());

    let pod_spec = spec.template.spec.get_or_insert_with(Default::default);
    pod_spec.node_name = Some(node.to_string());

    if let Some(facts) = facts {
        inject_node_facts(pod_spec, facts);
    }

    job
}

/// Handing the node's own facts down is what lets a per-node Job act on them
/// without a ServiceAccount token of its own. Every container gets them, since
/// they are independent processes; an existing value wins, so a template can
/// override.
fn inject_node_facts(pod_spec: &mut PodSpec, facts: &NodeFacts) {
    if facts.container_runtime_version.is_none() && facts.machine_id.is_none() {
        return;
    }

    let containers = pod_spec
        .init_containers
        .iter_mut()
        .flatten()
        .chain(pod_spec.containers.iter_mut());

    for container in containers {
        let env = container.env.get_or_insert_with(Vec::new);
        for (name, value) in [
            (
                CONTAINER_RUNTIME_VERSION_ENV,
                facts.container_runtime_version.as_deref(),
            ),
            (NODE_MACHINE_ID_ENV, facts.machine_id.as_deref()),
        ] {
            let Some(value) = value else {
                continue;
            };
            if env.iter().any(|var| var.name == name) {
                continue;
            }
            env.push(EnvVar {
                name: name.to_string(),
                value: Some(value.to_string()),
                value_from: None,
            });
        }
    }
}

pub fn interpret_status(job: &Job) -> JobOutcome {
    let Some(status) = job.status.as_ref() else {
        return JobOutcome::Running;
    };

    if let Some(conditions) = status.conditions.as_ref() {
        for c in conditions {
            if c.status != "True" {
                continue;
            }
            match c.type_.as_str() {
                "Failed" => return JobOutcome::Failed,
                "Complete" => return JobOutcome::Succeeded,
                _ => {}
            }
        }
    }

    if status.succeeded.unwrap_or(0) >= 1 {
        return JobOutcome::Succeeded;
    }

    JobOutcome::Running
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case("worker-0", "worker-0")]
    #[case("Worker.Example.COM", "worker-example-com")]
    #[case("--node--", "node")]
    #[case("a_b/c", "a-b-c")]
    fn test_sanitize_node(#[case] input: &str, #[case] expected: &str) {
        assert_eq!(sanitize_node(input), expected);
    }

    #[rstest]
    #[case("rollout-install", "rollout-install")]
    fn test_sanitize_label_value_short(#[case] input: &str, #[case] expected: &str) {
        assert_eq!(sanitize_label_value(input), expected);
    }

    #[test]
    fn normalization_cannot_merge_release_identities() {
        let dotted = sanitize_label_value("rollout.install");
        let dashed = sanitize_label_value("rollout-install");

        assert_ne!(dotted, dashed);
        assert!(dotted.starts_with("rollout-install-"));
        assert_eq!(dashed, "rollout-install");
    }

    #[test]
    fn test_sanitize_label_value_truncates() {
        let out = sanitize_label_value(&"a".repeat(100));
        assert!(out.len() <= MAX_LABEL_LEN);
        assert!(
            !out.ends_with('-'),
            "truncation must not leave a trailing dash"
        );
    }

    /// A value sanitizing to nothing still has to be a valid label value.
    #[test]
    fn an_unusable_value_falls_back_to_a_name() {
        let out = sanitize_label_value("___");
        assert!(out.starts_with("dispatcher-"));
        assert!(out.len() <= MAX_LABEL_LEN);
    }

    #[rstest]
    #[case("k8s-job-dispatcher")]
    #[case("example.com/dispatcher")]
    #[case("trailing-slash/")]
    fn tracking_labels_share_their_prefix(#[case] prefix: &str) {
        let tracking = TrackingLabels::with_prefix(prefix);
        let expected = prefix.trim_end_matches('/');

        assert_eq!(tracking.owner, format!("{expected}/owner"));
        assert_eq!(tracking.node, format!("{expected}/node"));
        assert_eq!(tracking.node_annotation, format!("{expected}/node-name"));
    }

    #[test]
    fn test_job_owned_by() {
        let tracking = TrackingLabels::default();
        let mut job = Job::default();
        assert!(!job_owned_by(&job, &tracking, "rollout-install"));
        job.metadata
            .labels
            .get_or_insert_with(BTreeMap::new)
            .insert(tracking.owner.clone(), "rollout-install".to_string());
        assert!(job_owned_by(&job, &tracking, "rollout-install"));
        assert!(!job_owned_by(&job, &tracking, "other-owner"));
    }

    #[test]
    fn a_job_tracked_under_another_prefix_is_not_ours() {
        let mut job = Job::default();
        job.metadata
            .labels
            .get_or_insert_with(BTreeMap::new)
            .insert(
                TrackingLabels::with_prefix("someone-else").owner,
                "rollout-install".to_string(),
            );

        assert!(!job_owned_by(
            &job,
            &TrackingLabels::default(),
            "rollout-install"
        ));
    }

    #[rstest]
    #[case("rollout-install", "worker-0", "rollout-install-worker-0")]
    fn test_job_name_short(#[case] prefix: &str, #[case] node: &str, #[case] expected: &str) {
        assert_eq!(job_name(prefix, node), expected);
    }

    #[test]
    fn normalized_node_names_cannot_merge_job_names() {
        assert_ne!(
            job_name("rollout-install", "worker.a"),
            job_name("rollout-install", "worker-a")
        );
    }

    #[test]
    fn long_release_prefixes_cannot_merge_job_names() {
        let shared = "rollout-install-with-a-very-long-shared-prefix-";
        let name_a = job_name(&format!("{shared}alpha"), "worker-0");
        let name_b = job_name(&format!("{shared}bravo"), "worker-0");

        assert_ne!(name_a, name_b);
        assert!(name_a.len() <= MAX_LABEL_LEN);
        assert!(name_b.len() <= MAX_LABEL_LEN);
    }

    #[test]
    fn test_job_name_truncated_and_unique() {
        let prefix = "rollout-install";
        let long_a = "node-with-a-really-really-really-really-really-long-name-aaaaaaa";
        let long_b = "node-with-a-really-really-really-really-really-long-name-bbbbbbb";

        let name_a = job_name(prefix, long_a);
        let name_b = job_name(prefix, long_b);

        assert!(
            name_a.len() <= 63,
            "name too long: {} ({})",
            name_a,
            name_a.len()
        );
        assert!(
            name_b.len() <= 63,
            "name too long: {} ({})",
            name_b,
            name_b.len()
        );
        assert_ne!(
            name_a, name_b,
            "different node names must yield different job names"
        );
    }

    #[test]
    fn job_template_preserves_yaml_aliases_and_script_content() {
        let template: Job = serde_saphyr::from_str(
            r#"
apiVersion: batch/v1
kind: Job
metadata:
  name: install
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: install
          image: &image busybox:1.37
          command:
            - sh
            - -c
            - |
              echo "installing"
              echo "done"
          securityContext:
            privileged: true
          resources:
            requests:
              cpu: 100m
              memory: 64Mi
        - name: verify
          image: *image
"#,
        )
        .unwrap();

        let pod = template.spec.unwrap().template.spec.unwrap();
        let install = &pod.containers[0];
        assert_eq!(pod.containers[1].image, install.image);
        assert_eq!(
            install.command.as_ref().unwrap()[2],
            "echo \"installing\"\necho \"done\"\n"
        );
        assert_eq!(
            install.security_context.as_ref().unwrap().privileged,
            Some(true)
        );
        let requests = install
            .resources
            .as_ref()
            .unwrap()
            .requests
            .as_ref()
            .unwrap();
        assert_eq!(requests["cpu"].0, "100m");
        assert_eq!(requests["memory"].0, "64Mi");
    }

    #[test]
    fn test_build_node_job_pins_node_and_labels() {
        let template: Job = serde_saphyr::from_str(
            r#"
apiVersion: batch/v1
kind: Job
metadata:
  name: ignored
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: c
          image: busybox
"#,
        )
        .unwrap();

        let owner = OwnerReference {
            api_version: "batch/v1".to_string(),
            kind: "Job".to_string(),
            name: "dispatcher".to_string(),
            uid: "abc-123".to_string(),
            controller: Some(false),
            block_owner_deletion: Some(false),
        };

        let facts = NodeFacts {
            name: "node1".to_string(),
            container_runtime_version: Some("containerd://2.1.5".to_string()),
            machine_id: Some("machine-1".to_string()),
        };

        let tracking = TrackingLabels::default();
        let job = build_node_job(
            &template,
            "rollout-install-node1",
            "node1",
            "rollout-install",
            Some(&owner),
            Some(&facts),
            &tracking,
        );

        assert_eq!(job.metadata.name.as_deref(), Some("rollout-install-node1"));
        let labels = job.metadata.labels.unwrap();
        assert_eq!(
            labels.get(&tracking.owner).map(String::as_str),
            Some("rollout-install")
        );
        assert_eq!(
            labels.get(&tracking.node).map(String::as_str),
            Some("node1")
        );
        let annotations = job.metadata.annotations.unwrap();
        assert_eq!(
            annotations
                .get(&tracking.node_annotation)
                .map(String::as_str),
            Some("node1")
        );
        assert_eq!(job.metadata.owner_references.unwrap().len(), 1);
        let pod_spec = job.spec.unwrap().template.spec.unwrap();
        assert_eq!(pod_spec.node_name.as_deref(), Some("node1"));

        let env = pod_spec.containers[0].env.as_ref().unwrap();
        assert_eq!(
            env.iter()
                .find(|var| var.name == CONTAINER_RUNTIME_VERSION_ENV)
                .and_then(|var| var.value.clone())
                .as_deref(),
            Some("containerd://2.1.5")
        );
    }

    /// A consumer that can look the version up itself must see no value rather
    /// than an empty one.
    #[test]
    fn unknown_facts_inject_nothing() {
        let template: Job = serde_saphyr::from_str(
            r#"apiVersion: batch/v1
kind: Job
metadata:
  name: t
spec:
  template:
    spec:
      restartPolicy: Never
      initContainers:
        - name: i
          image: busybox
      containers:
        - name: c
          image: busybox
"#,
        )
        .unwrap();

        let no_version = NodeFacts {
            name: "node1".to_string(),
            container_runtime_version: None,
            machine_id: None,
        };

        for facts in [Some(&no_version), None] {
            let job = build_node_job(
                &template,
                "j",
                "node1",
                "owner",
                None,
                facts,
                &TrackingLabels::default(),
            );

            let pod_spec = job.spec.unwrap().template.spec.unwrap();
            assert!(pod_spec.containers[0].env.is_none());
            assert!(pod_spec.init_containers.unwrap()[0].env.is_none());
        }
    }

    #[test]
    fn facts_reach_init_containers() {
        let template: Job = serde_saphyr::from_str(
            r#"apiVersion: batch/v1
kind: Job
metadata:
  name: t
spec:
  template:
    spec:
      restartPolicy: Never
      initContainers:
        - name: host-check
          image: busybox
      containers:
        - name: cri
          image: busybox
"#,
        )
        .unwrap();

        let facts = NodeFacts {
            name: "node1".to_string(),
            container_runtime_version: Some("cri-o://1.31.0".to_string()),
            machine_id: Some("machine-1".to_string()),
        };
        let job = build_node_job(
            &template,
            "j",
            "node1",
            "owner",
            None,
            Some(&facts),
            &TrackingLabels::default(),
        );
        let pod_spec = job.spec.unwrap().template.spec.unwrap();

        for env in [
            pod_spec.init_containers.as_ref().unwrap()[0].env.as_ref(),
            pod_spec.containers[0].env.as_ref(),
        ] {
            let env = env.unwrap();
            assert_eq!(
                env.iter()
                    .find(|var| var.name == CONTAINER_RUNTIME_VERSION_ENV)
                    .and_then(|var| var.value.clone())
                    .as_deref(),
                Some("cri-o://1.31.0")
            );
            assert_eq!(
                env.iter()
                    .find(|var| var.name == NODE_MACHINE_ID_ENV)
                    .and_then(|var| var.value.as_deref()),
                Some("machine-1")
            );
        }
    }

    /// One fact missing must not keep the other from reaching the Job.
    #[test]
    fn facts_are_injected_independently() {
        let template: Job = serde_saphyr::from_str(
            r#"apiVersion: batch/v1
kind: Job
metadata:
  name: t
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: c
          image: busybox
"#,
        )
        .unwrap();

        let facts = NodeFacts {
            name: "node1".to_string(),
            container_runtime_version: None,
            machine_id: Some("machine-1".to_string()),
        };
        let job = build_node_job(
            &template,
            "j",
            "node1",
            "owner",
            None,
            Some(&facts),
            &TrackingLabels::default(),
        );

        let env = job.spec.unwrap().template.spec.unwrap().containers[0]
            .env
            .clone()
            .unwrap();
        assert_eq!(env.len(), 1);
        assert_eq!(env[0].name, NODE_MACHINE_ID_ENV);
    }

    fn job_with_status(status_yaml: &str) -> Job {
        let yaml = format!(
            "apiVersion: batch/v1\nkind: Job\nmetadata:\n  name: j\nstatus:\n{status_yaml}"
        );
        serde_saphyr::from_str(&yaml).unwrap()
    }

    #[rstest]
    #[case(
        "  conditions:\n    - type: Complete\n      status: \"True\"\n",
        JobOutcome::Succeeded
    )]
    #[case(
        "  conditions:\n    - type: Failed\n      status: \"True\"\n",
        JobOutcome::Failed
    )]
    #[case(
        "  conditions:\n    - type: Complete\n      status: \"False\"\n",
        JobOutcome::Running
    )]
    #[case("  succeeded: 1\n", JobOutcome::Succeeded)]
    fn test_interpret_status(#[case] status_yaml: &str, #[case] expected: JobOutcome) {
        assert_eq!(interpret_status(&job_with_status(status_yaml)), expected);
    }

    #[test]
    fn test_interpret_status_running_when_unset() {
        assert_eq!(interpret_status(&Job::default()), JobOutcome::Running);
    }
}
