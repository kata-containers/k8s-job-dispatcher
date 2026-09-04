// Copyright (c) 2026 NVIDIA Corporation
//
// SPDX-License-Identifier: Apache-2.0

//! A run's result as an Event against the Node.
//!
//! The dispatcher's log goes with its pod and the per-node Jobs go with their
//! TTL, so this is addressed to the Node instead - which is also what
//! `kubectl describe node` shows and what event pipelines already collect.

use crate::job::sanitize_node;
use k8s_openapi::api::core::v1::{Event, EventSource, Node, ObjectReference};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, Time};
use k8s_openapi::jiff::Timestamp;
use kube::api::{Api, PostParams};
use kube::Client;
use log::{debug, warn};
use std::sync::atomic::{AtomicBool, Ordering};

const FAILED: &str = "JobFailed";
const SUCCEEDED: &str = "JobSucceeded";
const WAITING: &str = "JobPending";

const WARNING: &str = "Warning";
const NORMAL: &str = "Normal";

/// A Node has no namespace, and the apiserver rejects an Event whose
/// `involvedObject` has none unless the Event itself is in `default`. It is also
/// where the kubelet's own Node events go.
const EVENT_NAMESPACE: &str = "default";

/// Every run reports; there is no flag for it. A rollout that has to be asked
/// to explain itself is one nobody asked.
pub struct Reporter {
    events: Api<Event>,
    component: String,
    event_failure_reported: AtomicBool,
}

impl Reporter {
    pub fn new(client: &Client, component: &str) -> Self {
        Self {
            events: Api::namespaced(client.clone(), EVENT_NAMESPACE),
            component: component.to_string(),
            event_failure_reported: AtomicBool::new(false),
        }
    }

    pub async fn node_failed(&self, node: &Node, reason: &str) {
        self.emit(node, WARNING, FAILED, reason).await;
    }

    pub async fn node_succeeded(&self, node: &Node) {
        self.emit(node, NORMAL, SUCCEEDED, "the node's Job completed")
            .await;
    }

    pub async fn node_waiting(&self, node: &Node, detail: &str) {
        self.emit(node, WARNING, WAITING, detail).await;
    }

    async fn emit(&self, node: &Node, kind: &str, reason: &str, message: &str) {
        let Some(name) = node.metadata.name.as_deref() else {
            return;
        };

        let event = build(&self.component, node, name, kind, reason, message);
        if let Err(err) = self.events.create(&PostParams::default(), &event).await {
            // A cluster that refuses one of these refuses all of them.
            if !self.event_failure_reported.swap(true, Ordering::Relaxed) {
                warn!(
                    "could not record an Event against node {name} ({err}); the run's results will \
                     be in this log and on the nodes themselves only"
                );
            } else {
                debug!("could not record an Event against node {name}: {err}");
            }
        }
    }
}

fn build(
    component: &str,
    node: &Node,
    name: &str,
    kind: &str,
    reason: &str,
    message: &str,
) -> Event {
    let now = Time(Timestamp::now());

    Event {
        metadata: ObjectMeta {
            // The apiserver's suffix keeps two results for one node apart.
            generate_name: Some(format!("{}.", sanitize_node(name))),
            ..Default::default()
        },
        involved_object: ObjectReference {
            api_version: Some("v1".to_string()),
            kind: Some("Node".to_string()),
            name: Some(name.to_string()),
            uid: node.metadata.uid.clone(),
            ..Default::default()
        },
        reason: Some(reason.to_string()),
        message: Some(message.to_string()),
        type_: Some(kind.to_string()),
        // No `eventTime`: it makes the apiserver validate this as an
        // events.k8s.io Event, which wants more than a legacy recorder has.
        first_timestamp: Some(now.clone()),
        last_timestamp: Some(now),
        count: Some(1),
        source: Some(EventSource {
            component: Some(component.to_string()),
            host: Some(name.to_string()),
        }),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kube::api::ObjectMeta as KubeObjectMeta;

    fn node(name: &str) -> Node {
        Node {
            metadata: KubeObjectMeta {
                name: Some(name.to_string()),
                uid: Some("uid-1".to_string()),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    const COMPONENT: &str = "k8s-job-dispatcher";

    #[test]
    fn the_event_is_addressed_to_the_node_itself() {
        let event = build(
            COMPONENT,
            &node("worker-0"),
            "worker-0",
            WARNING,
            FAILED,
            "host-check exited 1",
        );

        assert_eq!(event.involved_object.kind.as_deref(), Some("Node"));
        assert_eq!(event.involved_object.name.as_deref(), Some("worker-0"));
        assert_eq!(event.involved_object.uid.as_deref(), Some("uid-1"));
        assert_eq!(event.type_.as_deref(), Some("Warning"));
        assert_eq!(event.reason.as_deref(), Some("JobFailed"));
        assert_eq!(event.message.as_deref(), Some("host-check exited 1"));
        assert_eq!(event.count, Some(1));
        assert!(event.first_timestamp.is_some());
        assert!(event.last_timestamp.is_some());
    }

    #[test]
    fn the_event_carries_no_event_time() {
        let event = build(
            COMPONENT,
            &node("worker-0"),
            "worker-0",
            NORMAL,
            SUCCEEDED,
            "done",
        );
        assert!(event.event_time.is_none());
    }

    #[test]
    fn the_event_name_is_derived_safely_from_the_node() {
        let event = build(
            COMPONENT,
            &node("Worker.Example.COM"),
            "Worker.Example.COM",
            WARNING,
            FAILED,
            "failed",
        );

        assert_eq!(
            event.metadata.generate_name.as_deref(),
            Some("worker-example-com.")
        );
        assert!(event.metadata.name.is_none());
    }
}
