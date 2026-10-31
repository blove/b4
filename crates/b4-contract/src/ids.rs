use serde::{Deserialize, Deserializer, Serialize};
use std::fmt;

/// Maximum UTF-8 byte length of a portable opaque identity.
pub const MAX_ID_BYTES: usize = 256;

/// Identity validation errors intentionally exclude the rejected value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdError {
    Empty,
    TooLong { bytes: usize },
    ControlCharacter,
    InvalidRoute,
    InvalidGeneration,
}

impl fmt::Display for IdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("identity must not be empty"),
            Self::TooLong { bytes } => {
                write!(
                    f,
                    "identity is {bytes} UTF-8 bytes; maximum is {MAX_ID_BYTES}"
                )
            }
            Self::ControlCharacter => f.write_str("identity must not contain control characters"),
            Self::InvalidGeneration => {
                f.write_str("generation must contain 64 lowercase hexadecimal characters")
            }
            Self::InvalidRoute => {
                f.write_str("route must start with / and end with #agent or #workflow")
            }
        }
    }
}

impl std::error::Error for IdError {}

pub(crate) fn validate_opaque(value: &str) -> Result<(), IdError> {
    if value.is_empty() {
        return Err(IdError::Empty);
    }
    if value.len() > MAX_ID_BYTES {
        return Err(IdError::TooLong { bytes: value.len() });
    }
    if value.chars().any(char::is_control) {
        return Err(IdError::ControlCharacter);
    }
    Ok(())
}

fn validate_route(value: &str) -> Result<(), IdError> {
    validate_opaque(value)?;
    let Some((path, kind)) = value.split_once('#') else {
        return Err(IdError::InvalidRoute);
    };
    if !path.starts_with('/') || !matches!(kind, "agent" | "workflow") {
        return Err(IdError::InvalidRoute);
    }
    Ok(())
}

macro_rules! opaque_id {
    ($name:ident, $validate:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, IdError> {
                let value = value.into();
                $validate(&value)?;
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// Returns the UTF-8 byte length of this identity.
            pub fn byte_len(&self) -> usize {
                self.0.len()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
            }
        }
    };
}

opaque_id!(ThreadId, validate_opaque);
opaque_id!(ExecutionId, validate_opaque);
opaque_id!(TurnId, validate_opaque);
opaque_id!(TaskId, validate_opaque);
opaque_id!(EffectId, validate_opaque);
opaque_id!(AttemptId, validate_opaque);
opaque_id!(InterruptId, validate_opaque);
opaque_id!(ExecutionRevision, validate_opaque);
opaque_id!(RouteKey, validate_route);

fn validate_generation(value: &str) -> Result<(), IdError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(IdError::InvalidGeneration);
    }
    Ok(())
}

// An immutable, server-generated 256-bit thread incarnation.
opaque_id!(ThreadGeneration, validate_generation);
