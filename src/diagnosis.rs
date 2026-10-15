// Copyright (c) 2026 NVIDIA Corporation
//
// SPDX-License-Identifier: Apache-2.0

//! Why a per-node Job failed, in one line.
//!
//! A Job's status says that it failed and nothing else. The reason is in its
//! pod, which `ttlSecondsAfterFinished` deletes minutes later, so it is read
//! and carried on the node's result instead.

use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{ContainerStatus, Pod};
use kube::api::{Api, ListParams};
use log::debug;

/// Set since Kubernetes 1.27.
const JOB_NAME_LABEL: &str = "batch.kubernetes.io/job-name";

/// Deprecated, so consulted only when the current label matches nothing.
const LEGACY_JOB_NAME_LABEL: &str = "job-name";

/// A diagnosis goes into a node annotation and an Event message, neither of
/// which is the place for a container's last screenful.
const MAX_DIAGNOSIS: usize = 1024;

/// Waiting states that mean "starting normally" rather than "stuck".
const STARTING: &[&str] = &["PodInitializing", "ContainerCreating"];

const CRASH_LOOP: &str = "CrashLoopBackOff";

/// Why the Job that ran on this node failed, as far as the cluster can say.
///
/// Never fails: this already runs on a failure path, and a diagnosis that
/// cannot be read is not a second failure.
pub async fn why_the_job_failed(
    pods: &Api<Pod>,
    job: &Job,
    name: &str,
    last_seen: Option<&str>,
) -> String {
    let from_pod = match newest_pod(pods, name).await {
        Ok(Some(pod)) => describe_failed_pod(&pod),
        Ok(None) => None,
        Err(err) => {
            debug!("could not read the pods of job {name} to say why it failed: {err}");
            None
        }
    };

    // A Job that hit its deadline has had its pods deleted by the Job
    // controller, so what the run saw while it waited is all that is left.
    let from_pod = from_pod.or_else(|| last_seen.map(str::to_string));

    shorten(frame(from_pod, job_failure_condition(job)))
}

/// The Job's reason - a deadline, the retries - frames what the pod said.
fn frame(from_pod: Option<String>, from_job: Option<String>) -> String {
    match (from_pod, from_job) {
        (Some(pod), Some(job)) => format!("{pod} [{job}]"),
        (Some(pod), None) => pod,
        (None, Some(job)) => job,
        (None, None) => "the Job reports no reason and left no pod behind to ask".to_string(),
    }
}

/// What a Job that has not finished is stuck on. `None` means it is running.
///
/// A pod that never starts produces no Job failure until the deadline runs out,
/// which is an hour of silence on the default settings.
pub async fn what_the_job_waits_for(pods: &Api<Pod>, name: &str) -> Option<String> {
    let pod = match newest_pod(pods, name).await {
        Ok(Some(pod)) => pod,
        // Quota, a webhook, or a suspended Job: the Job exists and nothing runs.
        Ok(None) => return Some("no pod has been created for it".to_string()),
        Err(err) => {
            debug!("could not read the pods of job {name}: {err}");
            return None;
        }
    };

    let status = pod.status.as_ref()?;
    if status.phase.as_deref() == Some("Running") {
        return None;
    }

    if let Some(detail) = container_statuses(&pod).find_map(describe_container) {
        return Some(shorten(detail));
    }

    let unschedulable = status
        .conditions
        .iter()
        .flatten()
        .find(|condition| condition.type_ == "PodScheduled" && condition.status == "False")?;
    let detail = match trimmed(unschedulable.message.as_deref()) {
        Some(message) => format!("its pod is unschedulable: {message}"),
        None => format!(
            "its pod is unschedulable: {}",
            unschedulable.reason.as_deref().unwrap_or("no reason given")
        ),
    };
    Some(shorten(detail))
}

/// A retried Job has a pod per attempt, and the last one is the verdict.
async fn newest_pod(pods: &Api<Pod>, job: &str) -> anyhow::Result<Option<Pod>> {
    for label in [JOB_NAME_LABEL, LEGACY_JOB_NAME_LABEL] {
        let listed = pods
            .list(&ListParams::default().labels(&format!("{label}={job}")))
            .await?;
        if let Some(pod) = newest(listed.items) {
            return Ok(Some(pod));
        }
    }
    Ok(None)
}

fn newest(pods: Vec<Pod>) -> Option<Pod> {
    pods.into_iter().max_by(|left, right| {
        let stamp = |pod: &Pod| pod.metadata.creation_timestamp.clone();
        // Timestamps have second granularity; the name keeps ties stable.
        (stamp(left), left.metadata.name.clone()).cmp(&(stamp(right), right.metadata.name.clone()))
    })
}

fn container_statuses(pod: &Pod) -> impl Iterator<Item = &ContainerStatus> {
    let status = pod.status.as_ref();
    // Init containers run first, so the first of them to fail is the failure.
    status
        .into_iter()
        .flat_map(|status| status.init_container_statuses.iter().flatten())
        .chain(
            status
                .into_iter()
                .flat_map(|status| status.container_statuses.iter().flatten()),
        )
}

fn describe_failed_pod(pod: &Pod) -> Option<String> {
    if let Some(detail) = container_statuses(pod).find_map(describe_container) {
        return Some(detail);
    }

    // An eviction fails the pod without any container having had a say.
    let status = pod.status.as_ref()?;
    let reason = status.reason.as_deref()?;
    Some(match trimmed(status.message.as_deref()) {
        Some(message) => format!("its pod failed: {reason}: {message}"),
        None => format!("its pod failed: {reason}"),
    })
}

/// Preferring the termination message: that is where the stage itself says why,
/// and unlike its log it survives in the pod's status.
fn describe_container(status: &ContainerStatus) -> Option<String> {
    let state = status.state.as_ref()?;
    let name = &status.name;

    if let Some(terminated) = state.terminated.as_ref() {
        if terminated.exit_code == 0 {
            return None;
        }
        return Some(describe_exit(
            name,
            terminated.exit_code,
            terminated.reason.as_deref(),
            terminated.message.as_deref(),
        ));
    }

    let waiting = state.waiting.as_ref()?;
    let reason = waiting.reason.as_deref()?;
    if STARTING.contains(&reason) {
        return None;
    }

    // The backoff says nothing; the run before it says everything.
    if reason == CRASH_LOOP {
        if let Some(previous) = status
            .last_state
            .as_ref()
            .and_then(|last| last.terminated.as_ref())
        {
            return Some(format!(
                "{} and is now in {CRASH_LOOP}",
                describe_exit(
                    name,
                    previous.exit_code,
                    previous.reason.as_deref(),
                    previous.message.as_deref(),
                )
            ));
        }
    }

    Some(match trimmed(waiting.message.as_deref()) {
        Some(message) => format!("{name} never started: {reason}: {message}"),
        None => format!("{name} never started: {reason}"),
    })
}

fn describe_exit(name: &str, code: i32, reason: Option<&str>, message: Option<&str>) -> String {
    let mut detail = format!("{name} exited {code}");
    // "Error" only repeats the non-zero code; OOMKilled is the whole story.
    if let Some(reason) = reason.filter(|reason| *reason != "Error") {
        detail.push_str(&format!(" ({reason})"));
    }
    match trimmed(message) {
        Some(message) => format!("{detail}: {message}"),
        None => format!("{detail} without saying why"),
    }
}

fn job_failure_condition(job: &Job) -> Option<String> {
    let condition = job
        .status
        .as_ref()?
        .conditions
        .as_ref()?
        .iter()
        .find(|condition| condition.type_ == "Failed" && condition.status == "True")?;

    let reason = condition.reason.as_deref().unwrap_or("Failed");
    Some(match trimmed(condition.message.as_deref()) {
        Some(message) => format!("{reason}: {message}"),
        None => reason.to_string(),
    })
}

/// One line, since the summary and the node's result are line-oriented.
fn trimmed(message: Option<&str>) -> Option<String> {
    let joined = message?
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_string();
    Some(joined).filter(|message| !message.is_empty())
}

fn shorten(mut diagnosis: String) -> String {
    if diagnosis.len() <= MAX_DIAGNOSIS {
        return diagnosis;
    }

    const ELLIPSIS: &str = "... (truncated)";
    let keep = MAX_DIAGNOSIS - ELLIPSIS.len();
    let keep = (0..=keep)
        .rev()
        .find(|index| diagnosis.is_char_boundary(*index))
        .unwrap_or(0);
    diagnosis.truncate(keep);
    diagnosis.push_str(ELLIPSIS);
    diagnosis
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    fn pod(status_yaml: &str) -> Pod {
        let yaml =
            format!("apiVersion: v1\nkind: Pod\nmetadata:\n  name: p\nstatus:\n{status_yaml}");
        serde_saphyr::from_str(&yaml).unwrap()
    }

    fn job(status_yaml: &str) -> Job {
        let yaml = format!(
            "apiVersion: batch/v1\nkind: Job\nmetadata:\n  name: j\nstatus:\n{status_yaml}"
        );
        serde_saphyr::from_str(&yaml).unwrap()
    }

    #[test]
    fn a_stage_that_says_why_is_quoted_verbatim() {
        let pod = pod(r#"  initContainerStatuses:
    - name: install-stage-host-check
      state:
        terminated:
          exitCode: 1
          reason: Error
          message: "this node has no usable virtualization backend"
"#);

        assert_eq!(
            describe_failed_pod(&pod).as_deref(),
            Some(
                "install-stage-host-check exited 1: this node has no usable virtualization backend"
            )
        );
    }

    #[test]
    fn the_earliest_failing_stage_is_the_one_reported() {
        let pod = pod(r#"  initContainerStatuses:
    - name: load-kernel-modules
      state:
        terminated:
          exitCode: 0
    - name: host-check
      state:
        terminated:
          exitCode: 1
          message: "no dm_verity"
  containerStatuses:
    - name: cri
      state:
        waiting:
          reason: PodInitializing
"#);

        assert_eq!(
            describe_failed_pod(&pod).as_deref(),
            Some("host-check exited 1: no dm_verity")
        );
    }

    #[test]
    fn a_killed_stage_is_named_by_its_reason() {
        let pod = pod(r#"  containerStatuses:
    - name: artifacts
      state:
        terminated:
          exitCode: 137
          reason: OOMKilled
"#);

        assert_eq!(
            describe_failed_pod(&pod).as_deref(),
            Some("artifacts exited 137 (OOMKilled) without saying why")
        );
    }

    #[test]
    fn a_container_that_never_started_reports_what_stopped_it() {
        let pod = pod(r#"  containerStatuses:
    - name: cri
      state:
        waiting:
          reason: ImagePullBackOff
          message: "Back-off pulling image \"kata-deploy:latest\""
"#);

        assert_eq!(
            describe_failed_pod(&pod).as_deref(),
            Some("cri never started: ImagePullBackOff: Back-off pulling image \"kata-deploy:latest\"")
        );
    }

    #[test]
    fn a_crash_loop_is_explained_by_the_previous_run() {
        let pod = pod(r#"  containerStatuses:
    - name: cri
      lastState:
        terminated:
          exitCode: 2
          message: "containerd did not come back"
      state:
        waiting:
          reason: CrashLoopBackOff
"#);

        assert_eq!(
            describe_failed_pod(&pod).as_deref(),
            Some("cri exited 2: containerd did not come back and is now in CrashLoopBackOff")
        );
    }

    #[test]
    fn a_pod_killed_before_its_containers_still_reports() {
        let pod = pod("  phase: Failed\n  reason: Evicted\n  message: \"The node was low on resource: ephemeral-storage\"\n");

        assert_eq!(
            describe_failed_pod(&pod).as_deref(),
            Some("its pod failed: Evicted: The node was low on resource: ephemeral-storage")
        );
    }

    #[test]
    fn a_pod_with_nothing_to_say_says_nothing() {
        assert_eq!(describe_failed_pod(&Pod::default()), None);
        assert_eq!(describe_failed_pod(&pod("  phase: Failed\n")), None);
    }

    /// Otherwise a Job killed by its deadline reports the deadline and nothing
    /// about the pull or the taint that ran the clock down.
    #[test]
    fn a_deleted_pod_leaves_what_the_run_last_saw() {
        let stalled = "cri never started: ImagePullBackOff: Back-off pulling image";

        assert_eq!(
            frame(
                Some(stalled.to_string()),
                Some("DeadlineExceeded: Job was active longer than specified deadline".to_string())
            ),
            "cri never started: ImagePullBackOff: Back-off pulling image [DeadlineExceeded: Job \
             was active longer than specified deadline]"
        );
        assert_eq!(
            frame(None, Some("BackoffLimitExceeded".to_string())),
            "BackoffLimitExceeded"
        );
        assert_eq!(
            frame(None, None),
            "the Job reports no reason and left no pod behind to ask"
        );
    }

    #[rstest]
    #[case(
        "  conditions:\n    - type: Failed\n      status: \"True\"\n      reason: DeadlineExceeded\n      message: Job was active longer than specified deadline\n",
        Some("DeadlineExceeded: Job was active longer than specified deadline")
    )]
    #[case(
        "  conditions:\n    - type: Failed\n      status: \"True\"\n      reason: BackoffLimitExceeded\n",
        Some("BackoffLimitExceeded")
    )]
    // Not this Job's failure, so not its reason.
    #[case(
        "  conditions:\n    - type: Failed\n      status: \"False\"\n      reason: DeadlineExceeded\n",
        None
    )]
    #[case("  succeeded: 1\n", None)]
    fn the_jobs_own_reason_is_read_from_its_failed_condition(
        #[case] status_yaml: &str,
        #[case] expected: Option<&str>,
    ) {
        assert_eq!(
            job_failure_condition(&job(status_yaml)).as_deref(),
            expected
        );
    }

    #[test]
    fn a_running_pod_is_not_waiting_for_anything() {
        let pod = pod("  phase: Running\n  containerStatuses:\n    - name: cri\n      state:\n        running:\n          startedAt: \"2026-01-01T00:00:00Z\"\n");
        assert!(pod.status.as_ref().unwrap().phase.as_deref() == Some("Running"));
        assert_eq!(container_statuses(&pod).find_map(describe_container), None);
    }

    #[test]
    fn a_message_spread_over_lines_is_folded_into_one() {
        assert_eq!(
            trimmed(Some("0/3 nodes are available:\n  1 node(s) had  taint\n")).as_deref(),
            Some("0/3 nodes are available: 1 node(s) had taint")
        );
        assert_eq!(trimmed(Some("   \n ")), None);
        assert_eq!(trimmed(None), None);
    }

    #[test]
    fn an_over_long_diagnosis_is_marked_as_truncated() {
        let long = shorten("größe ".repeat(1000));

        assert!(long.len() <= MAX_DIAGNOSIS);
        assert!(long.ends_with("... (truncated)"));
        assert!(long.starts_with("größe"));
    }

    #[test]
    fn the_newest_pod_is_the_one_the_last_retry_left() {
        let attempt = |name: &str, stamp: &str| -> Pod {
            serde_saphyr::from_str(&format!(
                "apiVersion: v1\nkind: Pod\nmetadata:\n  name: {name}\n  creationTimestamp: {stamp}\n"
            ))
            .unwrap()
        };

        let latest = newest(vec![
            attempt("first", "2026-01-01T00:00:00Z"),
            attempt("third", "2026-01-01T00:02:00Z"),
            attempt("second", "2026-01-01T00:01:00Z"),
        ]);

        assert_eq!(latest.unwrap().metadata.name.as_deref(), Some("third"));
        assert!(newest(vec![]).is_none());
    }
}
