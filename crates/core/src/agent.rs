// SPDX-License-Identifier: Apache-2.0

use std::fmt;

use serde::{Deserialize, Serialize};

/// Coding-agent harness whose request headers the load generator emulates.
///
/// A dialect sends only the headers its harness sends; Dynamo derives session
/// identity from them. The dialect never changes traffic shape; shape comes
/// from the profile.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[cfg_attr(feature = "clap", derive(clap::ValueEnum))]
#[serde(rename_all = "kebab-case")]
pub enum HeaderDialect {
    ClaudeCode,
    Codex,
}

impl HeaderDialect {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ClaudeCode => "claude-code",
            Self::Codex => "codex",
        }
    }
}

impl fmt::Display for HeaderDialect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
