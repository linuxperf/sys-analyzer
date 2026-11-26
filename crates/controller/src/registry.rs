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
    Stop,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum RegistryError {
    BlankAgentId,
    AlreadyRegistered(String),
    AgentDisabled(String),
    LeaseExpirationOutOfRange,
    InvalidSessionId,
    UnknownSession(String),
}

pub(super) enum ManagementAction {
    Disable,
    Enable,
}

#[derive(Debug)]
pub(super) enum ManagementError {
    Missing,
    AlreadyDisabled,
    NotDisabled,
}

enum RecordState {
    Registered {
        session_id: Uuid,
    },
    Offline {
        since: DateTime<Utc>,
        session_id: Option<Uuid>,
    },
    Disabled,
}

struct RegisteredAgent {
    registration: Registration,
    registered_at: DateTime<Utc>,
    last_heartbeat_at: Option<DateTime<Utc>>,
    lease_expires_at: DateTime<Utc>,
    state: RecordState,
}

impl RegisteredAgent {
    fn new(
        registration: Registration,
        session_id: Uuid,
        registered_at: DateTime<Utc>,
        lease_expires_at: DateTime<Utc>,
    ) -> Self {
        Self {
            registration,
            registered_at,
            last_heartbeat_at: None,
            lease_expires_at,
            state: RecordState::Registered { session_id },
        }
    }

    fn active_session(&self, now: DateTime<Utc>) -> Option<Uuid> {
        match self.state {
            RecordState::Registered { session_id } if now < self.lease_expires_at => {
                Some(session_id)
            }
            _ => None,
        }
    }

    fn matches_deregistration(&self, id: Uuid) -> bool {
        match self.state {
            RecordState::Registered { session_id } => session_id == id,
            RecordState::Offline { session_id, .. } => session_id == Some(id),
            RecordState::Disabled => false,
        }
    }

    fn replay(&self, attempt_id: &str, now: DateTime<Utc>) -> Result<Option<Uuid>, RegistryError> {
        if matches!(self.state, RecordState::Disabled) {
            return Err(RegistryError::AgentDisabled(
                self.registration.agent_id.clone(),
            ));
        }
        let Some(session_id) = self.active_session(now) else {
            return Ok(None);
        };
        if attempt_id.is_empty() || attempt_id != self.registration.attempt_id {
            return Err(RegistryError::AlreadyRegistered(
                self.registration.agent_id.clone(),
            ));
        }
        Ok(Some(session_id))
    }

    fn heartbeat(
        &mut self,
        session_id: Option<&str>,
        received_at: DateTime<Utc>,
        heartbeat: HeartbeatSettings,
    ) -> Result<Heartbeat, RegistryError> {
        if matches!(self.state, RecordState::Disabled) {
            return Ok(Heartbeat::Stop);
        }
        let Some(active_id) = self.active_session(received_at) else {
            return Ok(Heartbeat::Reregister);
        };
        // Older Agents omit the session ID and can renew by Agent ID alone.
        if session_id.is_some_and(|id| Uuid::parse_str(id).ok() != Some(active_id)) {
            return Ok(Heartbeat::Reregister);
        }
        let lease_expires_at = heartbeat.lease_expires_at(received_at)?;
        self.last_heartbeat_at = Some(received_at);
        self.lease_expires_at = lease_expires_at;
        Ok(Heartbeat::Renewed { lease_expires_at })
    }

    fn retain(&mut self, now: DateTime<Utc>, offline_retention: TimeDelta) -> bool {
        match self.state {
            RecordState::Registered { session_id } if self.active_session(now).is_none() => {
                // Retention starts when a sweep first observes expiry. The original
                // session can still explicitly deregister until revoked or replaced.
                self.state = RecordState::Offline {
                    since: now,
                    session_id: Some(session_id),
                };
                true
            }
            RecordState::Offline { since, .. } => now - since < offline_retention,
            RecordState::Registered { .. } | RecordState::Disabled => true,
        }
    }

    fn manage(
        &mut self,
        action: ManagementAction,
        now: DateTime<Utc>,
    ) -> Result<AgentDetail, ManagementError> {
        self.state = match (action, &self.state) {
            (ManagementAction::Disable, RecordState::Disabled) => {
                return Err(ManagementError::AlreadyDisabled);
            }
            (ManagementAction::Disable, _) => RecordState::Disabled,
            (ManagementAction::Enable, RecordState::Disabled) => RecordState::Offline {
                since: now,
                session_id: None,
            },
            (ManagementAction::Enable, _) => return Err(ManagementError::NotDisabled),
        };
        Ok(self.detail(now))
    }

    fn status(&self, now: DateTime<Utc>) -> AgentStatus {
        match self.state {
            RecordState::Registered { .. } if self.active_session(now).is_some() => {
                AgentStatus::Online
            }
            RecordState::Registered { .. } | RecordState::Offline { .. } => AgentStatus::Offline,
            RecordState::Disabled => AgentStatus::Disabled,
        }
    }

    fn record(&self, status: AgentStatus) -> AgentRecord {
        AgentRecord {
            agent_id: self.registration.agent_id.clone(),
            hostname: self.registration.hostname.clone(),
            pid: self.registration.pid,
            version: self.registration.version.clone(),
            labels: self.registration.labels.clone(),
            metadata: self.registration.metadata.clone(),
            status,
            registered_at: self.registered_at,
            last_heartbeat_at: self.last_heartbeat_at,
        }
    }

    fn detail(&self, now: DateTime<Utc>) -> AgentDetail {
        AgentDetail {
            record: self.record(self.status(now)),
            lease_expires_at: self.lease_expires_at,
        }
    }
}

pub(super) struct Registry {
    agents: Mutex<BTreeMap<String, RegisteredAgent>>,
    heartbeat: HeartbeatSettings,
    offline_retention: TimeDelta,
    clock: Box<dyn Fn() -> DateTime<Utc> + Send + Sync>,
}

impl Registry {
    pub(super) fn new(
        heartbeat: HeartbeatSettings,
        offline_retention: TimeDelta,
        clock: impl Fn() -> DateTime<Utc> + Send + Sync + 'static,
    ) -> Self {
        Self {
            agents: Mutex::default(),
            heartbeat,
            offline_retention,
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
        let replayed_session = match agents.get(&request.agent_id) {
            Some(agent) => agent.replay(&request.attempt_id, registered_at)?,
            None => None,
        };
        let session_id = match replayed_session {
            Some(id) => id,
            None => {
                let lease_expires_at = self.heartbeat.lease_expires_at(registered_at)?;
                let id = Uuid::new_v4();
                agents.insert(
                    request.agent_id.clone(),
                    RegisteredAgent::new(request, id, registered_at, lease_expires_at),
                );
                id
            }
        };

        Ok(RegisteredSession {
            id: session_id,
            heartbeat_interval_ms: self.heartbeat.interval_ms,
            lease_timeout_ms: self.heartbeat.lease_timeout_ms(),
        })
    }

    pub(super) fn heartbeat(
        &self,
        agent_id: &str,
        session_id: Option<&str>,
    ) -> Result<Heartbeat, RegistryError> {
        let mut agents = self.agents.lock().expect("Agent registry lock poisoned");
        let Some(agent) = agents.get_mut(agent_id) else {
            return Ok(Heartbeat::Reregister);
        };
        // Read Controller time under the lock so renewal follows mutation order.
        agent.heartbeat(session_id, (self.clock)(), self.heartbeat)
    }

    pub(super) fn sweep(&self) {
        let mut agents = self.agents.lock().expect("Agent registry lock poisoned");
        let now = (self.clock)();
        agents.retain(|_, agent| agent.retain(now, self.offline_retention));
    }

    pub(super) fn deregister(&self, agent_id: &str, session_id: &str) -> Result<(), RegistryError> {
        if agent_id.trim().is_empty() {
            return Err(RegistryError::BlankAgentId);
        }
        let session_id =
            Uuid::parse_str(session_id).map_err(|_| RegistryError::InvalidSessionId)?;
        let mut agents = self.agents.lock().expect("Agent registry lock poisoned");
        match agents.entry(agent_id.to_owned()) {
            Entry::Occupied(entry) if entry.get().matches_deregistration(session_id) => {
                entry.remove();
                Ok(())
            }
            _ => Err(RegistryError::UnknownSession(agent_id.to_owned())),
        }
    }

    pub(super) fn snapshot(&self, status: Option<AgentStatus>) -> Vec<AgentRecord> {
        let agents = self.agents.lock().expect("Agent registry lock poisoned");
        let now = (self.clock)();
        agents
            .values()
            .filter_map(|agent| {
                let effective_status = agent.status(now);
                status
                    .is_none_or(|status| status == effective_status)
                    .then(|| agent.record(effective_status))
            })
            .collect()
    }

    pub(super) fn detail(&self, agent_id: &str) -> Option<AgentDetail> {
        let agents = self.agents.lock().expect("Agent registry lock poisoned");
        let agent = agents.get(agent_id)?;
        Some(agent.detail((self.clock)()))
    }

    pub(super) fn manage(
        &self,
        agent_id: &str,
        action: ManagementAction,
    ) -> Result<AgentDetail, ManagementError> {
        let mut agents = self.agents.lock().expect("Agent registry lock poisoned");
        let agent = agents.get_mut(agent_id).ok_or(ManagementError::Missing)?;
        agent.manage(action, (self.clock)())
    }

    pub(super) fn remove(&self, agent_id: &str) -> Result<(), ManagementError> {
        let mut agents = self.agents.lock().expect("Agent registry lock poisoned");
        agents
            .remove(agent_id)
            .map(|_| ())
            .ok_or(ManagementError::Missing)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};
    use std::thread;

    use super::*;

    const RETENTION: TimeDelta = TimeDelta::seconds(2);

    fn registry(start: DateTime<Utc>) -> (Registry, Arc<Mutex<DateTime<Utc>>>) {
        let now = Arc::new(Mutex::new(start));
        let clock = Arc::clone(&now);
        let settings = HeartbeatSettings::new(25, 1_000).unwrap();
        (
            Registry::new(settings, RETENTION, move || *clock.lock().unwrap()),
            now,
        )
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
        registry.heartbeat("test-agent", None).unwrap();
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
        assert_eq!(before[0].status, AgentStatus::Offline);
        assert_eq!(before[0].registered_at, start);
        assert_eq!(before[0].last_heartbeat_at, None);
        assert_eq!(
            registry.heartbeat("test-agent", None).unwrap(),
            Heartbeat::Reregister
        );
        assert_eq!(registry.snapshot(None), before);
    }

    #[test]
    fn retention_starts_when_sweep_first_observes_the_expired_lease() {
        let start = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let (registry, now) = registry(start);
        registry.register(registration("attempt")).unwrap();

        let expired_at = start + TimeDelta::seconds(1);
        *now.lock().unwrap() = expired_at;
        let expired = registry.detail("test-agent").unwrap();
        assert_eq!(expired.record.status, AgentStatus::Offline);
        assert_eq!(expired.lease_expires_at, expired_at);
        assert_eq!(
            registry.snapshot(Some(AgentStatus::Offline)).as_slice(),
            std::slice::from_ref(&expired.record)
        );

        let first_sweep = expired_at + RETENTION;
        *now.lock().unwrap() = first_sweep;
        registry.sweep();
        assert_eq!(registry.detail("test-agent"), Some(expired.clone()));

        *now.lock().unwrap() = first_sweep + RETENTION - TimeDelta::milliseconds(1);
        registry.sweep();
        assert_eq!(registry.detail("test-agent"), Some(expired));

        *now.lock().unwrap() = first_sweep + RETENTION;
        registry.sweep();
        assert!(registry.detail("test-agent").is_none());
    }

    #[test]
    fn sweeping_keeps_the_session_for_deregistration_until_it_is_revoked() {
        let start = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let (registry, now) = registry(start);
        let first = registry.register(registration("first")).unwrap();
        *now.lock().unwrap() = start + TimeDelta::seconds(1);
        registry.sweep();
        registry
            .deregister("test-agent", &first.id.to_string())
            .unwrap();

        let second = registry.register(registration("second")).unwrap();
        let session_id = second.id.to_string();
        *now.lock().unwrap() = start + TimeDelta::seconds(2);
        registry.sweep();
        registry
            .manage("test-agent", ManagementAction::Disable)
            .unwrap();
        assert_eq!(
            registry.heartbeat("test-agent", Some(&session_id)),
            Ok(Heartbeat::Stop)
        );

        *now.lock().unwrap() += RETENTION;
        registry.sweep();
        let enabled = registry
            .manage("test-agent", ManagementAction::Enable)
            .unwrap();
        assert_eq!(enabled.record.status, AgentStatus::Offline);
        assert_eq!(
            registry.deregister("test-agent", &session_id),
            Err(RegistryError::UnknownSession("test-agent".to_owned()))
        );
        registry.sweep();
        assert_eq!(registry.detail("test-agent"), Some(enabled));

        *now.lock().unwrap() += RETENTION;
        registry.sweep();
        assert!(registry.detail("test-agent").is_none());
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
        registry.heartbeat("test-agent", None).unwrap();
        let before = registry.snapshot(None);
        assert_eq!(before[0].last_heartbeat_at, Some(first_heartbeat_at));

        *now.lock().unwrap() = DateTime::<Utc>::MAX_UTC - TimeDelta::milliseconds(500);
        assert_eq!(
            registry.heartbeat("test-agent", None),
            Err(RegistryError::LeaseExpirationOutOfRange)
        );
        assert_eq!(registry.snapshot(None), before);
    }

    #[test]
    fn unknown_agent_heartbeat_requests_reregistration_without_inserting_a_record() {
        let start = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let (registry, _) = registry(start);
        assert_eq!(
            registry.heartbeat("unknown-agent", None),
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
