use std::{
    collections::{BTreeMap, btree_map::Entry},
    sync::Mutex,
    time::Duration,
};

use anyhow::{Context, ensure};
use chrono::{DateTime, TimeDelta, Utc};
use sys_analyzer_shared::{AgentDetail, AgentRecord, AgentStatus};
use uuid::Uuid;

#[derive(Clone, Copy)]
pub(super) struct HeartbeatSettings {
    interval_ms: u64,
    lease_timeout: TimeDelta,
}

impl HeartbeatSettings {
    pub(super) fn new(interval_ms: u64, lease_timeout_ms: u64) -> anyhow::Result<Self> {
        ensure!(interval_ms > 0, "heartbeat interval must be positive");
        ensure!(
            lease_timeout_ms > interval_ms,
            "lease timeout must be longer than the heartbeat interval"
        );
        Ok(Self {
            interval_ms,
            lease_timeout: TimeDelta::from_std(Duration::from_millis(lease_timeout_ms))
                .context("lease timeout is out of range")?,
        })
    }

    pub(super) fn lease_expires_at(
        self,
        now: DateTime<Utc>,
    ) -> Result<DateTime<Utc>, RegistryError> {
        now.checked_add_signed(self.lease_timeout)
            .ok_or(RegistryError::LeaseExpirationOutOfRange)
    }

    fn lease_timeout_ms(self) -> u64 {
        self.lease_timeout
            .num_milliseconds()
            .try_into()
            .expect("lease timeout is positive")
    }
}

pub(super) struct Registration {
    pub(super) agent_id: String,
    pub(super) hostname: String,
    pub(super) pid: u32,
    pub(super) version: String,
    pub(super) labels: Vec<String>,
    pub(super) metadata: BTreeMap<String, String>,
    pub(super) attempt_id: String,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct RegisteredSession {
    pub(super) id: Uuid,
    pub(super) heartbeat_interval_ms: u64,
    pub(super) lease_timeout_ms: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum Heartbeat {
    Renewed { lease_expires_at: DateTime<Utc> },
    Reregister,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum RegistryError {
    BlankAgentId,
    AlreadyRegistered(String),
    LeaseExpirationOutOfRange,
    InvalidSessionId,
    UnknownSession(String),
}

struct RegisteredAgent {
    record: AgentRecord,
    session_id: Uuid,
    registration_attempt_id: String,
    lease_expires_at: DateTime<Utc>,
    offline_since: Option<DateTime<Utc>>,
}

impl RegisteredAgent {
    fn has_active_lease(&self, now: DateTime<Utc>) -> bool {
        self.record.status == AgentStatus::Online && now < self.lease_expires_at
    }
}

pub(super) struct Registry {
    agents: Mutex<BTreeMap<String, RegisteredAgent>>,
    heartbeat: HeartbeatSettings,
    clock: Box<dyn Fn() -> DateTime<Utc> + Send + Sync>,
}

impl Registry {
    pub(super) fn new(
        heartbeat: HeartbeatSettings,
        clock: impl Fn() -> DateTime<Utc> + Send + Sync + 'static,
    ) -> Self {
        Self {
            agents: Mutex::default(),
            heartbeat,
            clock: Box::new(clock),
        }
    }

    pub(super) fn register(
        &self,
        request: Registration,
    ) -> Result<RegisteredSession, RegistryError> {
        if request.agent_id.trim().is_empty() {
            return Err(RegistryError::BlankAgentId);
        }

        let mut agents = self.agents.lock().expect("Agent registry lock poisoned");
        let registered_at = (self.clock)();
        let online_agent = agents
            .get(&request.agent_id)
            .filter(|agent| agent.has_active_lease(registered_at));
        let session_id = if let Some(agent) = online_agent {
            if request.attempt_id.is_empty() || request.attempt_id != agent.registration_attempt_id
            {
                return Err(RegistryError::AlreadyRegistered(request.agent_id));
            }
            agent.session_id
        } else {
            let lease_expires_at = self.heartbeat.lease_expires_at(registered_at)?;
            let session_id = Uuid::new_v4();
            agents.insert(
                request.agent_id.clone(),
                RegisteredAgent {
                    record: AgentRecord {
                        agent_id: request.agent_id,
                        hostname: request.hostname,
                        pid: request.pid,
                        version: request.version,
                        labels: request.labels,
                        metadata: request.metadata,
                        status: AgentStatus::Online,
                        registered_at,
                        last_heartbeat_at: None,
                    },
                    session_id,
                    registration_attempt_id: request.attempt_id,
                    lease_expires_at,
                    offline_since: None,
                },
            );
            session_id
        };

        Ok(RegisteredSession {
            id: session_id,
            heartbeat_interval_ms: self.heartbeat.interval_ms,
            lease_timeout_ms: self.heartbeat.lease_timeout_ms(),
        })
    }

    pub(super) fn heartbeat(&self, agent_id: &str) -> Result<Heartbeat, RegistryError> {
        let mut agents = self.agents.lock().expect("Agent registry lock poisoned");
        let Some(agent) = agents.get_mut(agent_id) else {
            return Ok(Heartbeat::Reregister);
        };

        // Read Controller time while holding the lock so renewal follows mutation order.
        let received_at = (self.clock)();
        if !agent.has_active_lease(received_at) {
            return Ok(Heartbeat::Reregister);
        }
        let lease_expires_at = self.heartbeat.lease_expires_at(received_at)?;
        agent.record.last_heartbeat_at = Some(received_at);
        agent.lease_expires_at = lease_expires_at;
        Ok(Heartbeat::Renewed { lease_expires_at })
    }

    pub(super) fn sweep(&self, offline_retention: TimeDelta) {
        let mut agents = self.agents.lock().expect("Agent registry lock poisoned");
        let now = (self.clock)();
        agents.retain(|_, agent| match agent.record.status {
            AgentStatus::Online => {
                if now >= agent.lease_expires_at {
                    agent.record.status = AgentStatus::Offline;
                    agent.offline_since = Some(now);
                }
                true
            }
            AgentStatus::Offline => agent
                .offline_since
                .is_none_or(|offline_since| now - offline_since < offline_retention),
            AgentStatus::Disabled => true,
        });
    }

    pub(super) fn deregister(&self, agent_id: &str, session_id: &str) -> Result<(), RegistryError> {
        if agent_id.trim().is_empty() {
            return Err(RegistryError::BlankAgentId);
        }
        let session_id =
            Uuid::parse_str(session_id).map_err(|_| RegistryError::InvalidSessionId)?;
        let mut agents = self.agents.lock().expect("Agent registry lock poisoned");
        match agents.entry(agent_id.to_owned()) {
            Entry::Occupied(entry) if entry.get().session_id == session_id => {
                entry.remove();
                Ok(())
            }
            _ => Err(RegistryError::UnknownSession(agent_id.to_owned())),
        }
    }

    pub(super) fn snapshot(&self, status: Option<AgentStatus>) -> Vec<AgentRecord> {
        let agents = self.agents.lock().expect("Agent registry lock poisoned");
        agents
            .values()
            .filter(|agent| status.is_none_or(|status| agent.record.status == status))
            .map(|agent| agent.record.clone())
            .collect()
    }

    pub(super) fn detail(&self, agent_id: &str) -> Option<AgentDetail> {
        let agents = self.agents.lock().expect("Agent registry lock poisoned");
        agents.get(agent_id).map(|agent| AgentDetail {
            record: agent.record.clone(),
            lease_expires_at: agent.lease_expires_at,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};
    use std::thread;

    use super::*;

    fn registry(start: DateTime<Utc>) -> (Registry, Arc<Mutex<DateTime<Utc>>>) {
        let now = Arc::new(Mutex::new(start));
        let clock = Arc::clone(&now);
        let settings = HeartbeatSettings::new(25, 1_000).unwrap();
        (Registry::new(settings, move || *clock.lock().unwrap()), now)
    }

    fn registration(attempt_id: &str) -> Registration {
        Registration {
            agent_id: "test-agent".to_owned(),
            hostname: "original-host".to_owned(),
            pid: 42,
            version: "0.1.0".to_owned(),
            labels: vec!["dev".to_owned()],
            metadata: BTreeMap::from([("zone".to_owned(), "local".to_owned())]),
            attempt_id: attempt_id.to_owned(),
        }
    }

    #[test]
    fn registration_replay_returns_the_original_session_and_preserves_the_record() {
        let start = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let (registry, now) = registry(start);
        let original_session = registry.register(registration("first-attempt")).unwrap();
        *now.lock().unwrap() = start + TimeDelta::milliseconds(25);
        registry.heartbeat("test-agent").unwrap();
        let original_snapshot = registry.snapshot(None);

        *now.lock().unwrap() = start + TimeDelta::milliseconds(500);
        let mut retry = registration("first-attempt");
        retry
            .metadata
            .insert("zone".to_owned(), "remote".to_owned());
        retry.hostname = "different-host".to_owned();
        assert_eq!(registry.register(retry).unwrap(), original_session);
        assert_eq!(registry.snapshot(None), original_snapshot);
    }

    #[test]
    fn heartbeat_at_the_lease_deadline_requires_reregistration() {
        let start = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let (registry, now) = registry(start);
        let session = registry.register(registration("first-attempt")).unwrap();
        assert_eq!(session.lease_timeout_ms, 1_000);

        let received_at = start + TimeDelta::seconds(1);
        *now.lock().unwrap() = received_at;
        let before = registry.snapshot(None);
        assert_eq!(before.len(), 1);
        assert_eq!(before[0].status, AgentStatus::Online);
        assert_eq!(before[0].registered_at, start);
        assert_eq!(before[0].last_heartbeat_at, None);
        assert_eq!(
            registry.heartbeat("test-agent").unwrap(),
            Heartbeat::Reregister
        );
        assert_eq!(registry.snapshot(None), before);
    }

    #[test]
    fn lease_expiration_overflow_preserves_the_registry() {
        let (registry, now) = registry(DateTime::<Utc>::MAX_UTC);
        assert_eq!(
            registry.register(registration("first-attempt")),
            Err(RegistryError::LeaseExpirationOutOfRange)
        );
        assert!(registry.snapshot(None).is_empty());

        let start = DateTime::<Utc>::MAX_UTC - TimeDelta::milliseconds(1_050);
        *now.lock().unwrap() = start;
        registry.register(registration("second-attempt")).unwrap();
        let first_heartbeat_at = start + TimeDelta::milliseconds(25);
        *now.lock().unwrap() = first_heartbeat_at;
        registry.heartbeat("test-agent").unwrap();
        let before = registry.snapshot(None);
        assert_eq!(before[0].last_heartbeat_at, Some(first_heartbeat_at));

        *now.lock().unwrap() = DateTime::<Utc>::MAX_UTC - TimeDelta::milliseconds(500);
        assert_eq!(
            registry.heartbeat("test-agent"),
            Err(RegistryError::LeaseExpirationOutOfRange)
        );
        assert_eq!(registry.snapshot(None), before);
    }

    #[test]
    fn unknown_agent_heartbeat_requests_reregistration_without_inserting_a_record() {
        let start = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let (registry, _) = registry(start);
        assert_eq!(
            registry.heartbeat("unknown-agent"),
            Ok(Heartbeat::Reregister)
        );
        assert!(registry.snapshot(None).is_empty());
    }

    #[test]
    fn concurrent_registration_with_different_attempts_admits_one_agent() {
        let start = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let (registry, _) = registry(start);
        let barrier = Barrier::new(2);
        let register = |attempt_id| {
            barrier.wait();
            registry.register(registration(attempt_id))
        };
        let outcomes = thread::scope(|scope| {
            let first = scope.spawn(|| register("first-attempt"));
            let second = register("second-attempt");
            [first.join().unwrap(), second]
        });

        assert_eq!(outcomes.iter().filter(|outcome| outcome.is_ok()).count(), 1);
        assert!(outcomes.contains(&Err(RegistryError::AlreadyRegistered(
            "test-agent".to_owned()
        ))));
        assert_eq!(registry.snapshot(None).len(), 1);
    }
}
