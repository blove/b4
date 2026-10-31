//! Selected native Agent Protocol DTOs and transport-independent SSE framing.

use crate::ids::validate_opaque;
use crate::{ApprovalDecision, IdError, InterruptId, RouteKey, ThreadId};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{json, Map, Value};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunRequest {
    pub route: RouteKey,
    #[serde(default = "empty_input")]
    pub input: Value,
}

fn empty_input() -> Value {
    Value::Object(Map::new())
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResumeRequest {
    pub route: RouteKey,
    #[serde(deserialize_with = "nonempty_resume")]
    pub resume: Vec<ResumeEntry>,
}

fn nonempty_resume<'de, D>(deserializer: D) -> Result<Vec<ResumeEntry>, D::Error>
where
    D: Deserializer<'de>,
{
    let entries = Vec::<ResumeEntry>::deserialize(deserializer)?;
    if entries.is_empty() {
        Err(serde::de::Error::custom(
            "resume requires at least one decision",
        ))
    } else {
        Ok(entries)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResumeEntry {
    Resolved {
        #[serde(rename = "interruptId")]
        interrupt_id: InterruptId,
        payload: ApprovalDecision,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "optional_grant"
        )]
        grant: Option<ApprovalGrant>,
    },
    Cancelled {
        #[serde(rename = "interruptId")]
        interrupt_id: InterruptId,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "optional_grant"
        )]
        grant: Option<ApprovalGrant>,
    },
}

impl ResumeEntry {
    pub fn interrupt_id(&self) -> &InterruptId {
        match self {
            Self::Resolved { interrupt_id, .. } | Self::Cancelled { interrupt_id, .. } => {
                interrupt_id
            }
        }
    }

    pub fn decision(&self) -> ApprovalDecision {
        match self {
            Self::Resolved { payload, .. } => *payload,
            Self::Cancelled { .. } => ApprovalDecision::Deny,
        }
    }
}

fn optional_grant<'de, D>(deserializer: D) -> Result<Option<ApprovalGrant>, D::Error>
where
    D: Deserializer<'de>,
{
    ApprovalGrant::deserialize(deserializer).map(Some)
}

/// Sensitive opaque input. Structural validation does not authenticate a grant.
#[derive(Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct ApprovalGrant(String);

impl ApprovalGrant {
    pub fn new(value: impl Into<String>) -> Result<Self, IdError> {
        let value = value.into();
        validate_opaque(&value)?;
        Ok(Self(value))
    }

    /// Explicit access for the future authorized grant-validation boundary.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ApprovalGrant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ApprovalGrant([redacted])")
    }
}

impl<'de> Deserialize<'de> for ApprovalGrant {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThreadStatus {
    Idle,
    Busy,
    Interrupted,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Thread {
    pub thread_id: ThreadId,
    pub created_at: String,
    pub updated_at: String,
    pub metadata: Map<String, Value>,
    pub status: ThreadStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorKind {
    RequestError,
    ExecutionError,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApError {
    pub kind: ErrorKind,
    pub message: String,
    pub details: Map<String, Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    #[serde(rename = "docsUrl", skip_serializing_if = "Option::is_none")]
    pub docs_url: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ErrorEnvelope {
    pub error: ApError,
}

/// Event names are fixed; payload strings cannot become SSE control fields.
#[derive(Debug, Clone, PartialEq)]
pub enum SseEvent {
    Chunk(Value),
    ToolCall {
        id: String,
        name: String,
        input: Value,
    },
    ToolResult {
        id: String,
        name: String,
        output: Value,
    },
    Done {
        output: Value,
    },
}

impl SseEvent {
    pub fn encode(&self) -> Result<String, serde_json::Error> {
        let (name, payload) = match self {
            Self::Chunk(value) => ("chunk", value.clone()),
            Self::ToolCall { id, name, input } => (
                "tool_call",
                json!({ "id": id, "name": name, "input": input }),
            ),
            Self::ToolResult { id, name, output } => (
                "tool_result",
                json!({ "id": id, "name": name, "output": output }),
            ),
            Self::Done { output } => ("done", json!({ "output": output })),
        };
        Ok(format!(
            "event: {name}\ndata: {}\n\n",
            serde_json::to_string(&payload)?
        ))
    }
}

pub fn heartbeat() -> &'static str {
    ": ping\n\n"
}
