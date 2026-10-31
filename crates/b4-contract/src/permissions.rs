//! Identity-only contracts for the explicit permission profile scaffold.

use serde::{Deserialize, Deserializer, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PermissionIdentityError;

impl fmt::Display for PermissionIdentityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid_permission_identity")
    }
}

impl std::error::Error for PermissionIdentityError {}

macro_rules! stable_identity {
    ($name:ident) => {
        #[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, PermissionIdentityError> {
                let value = value.into();
                if value.is_empty()
                    || value.len() > 128
                    || !value
                        .as_bytes()
                        .iter()
                        .all(|byte| (0x21..=0x7e).contains(byte))
                {
                    return Err(PermissionIdentityError);
                }
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(concat!(stringify!($name), "([REDACTED])"))
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                Self::new(String::deserialize(deserializer)?)
                    .map_err(|_| serde::de::Error::custom("invalid_permission_identity"))
            }
        }
    };
}

stable_identity!(StableToolId);
stable_identity!(StableToolRevision);
stable_identity!(ToolConstraintRevision);

// Fixed-shape identities for record evidence; a valid ID grants no authority.
macro_rules! record_identity {
    ($name:ident, $prefix:literal) => {
        #[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, PermissionIdentityError> {
                let value = value.into();
                if value.len() != 69
                    || !value.starts_with($prefix)
                    || !value.as_bytes()[5..]
                        .iter()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
                {
                    return Err(PermissionIdentityError);
                }
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(concat!(stringify!($name), "([REDACTED])"))
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                Self::new(String::deserialize(deserializer)?)
                    .map_err(|_| serde::de::Error::custom("invalid_permission_identity"))
            }
        }
    };
}

record_identity!(PermissionId, "b4pd_");
record_identity!(PermissionUseId, "b4pu_");
