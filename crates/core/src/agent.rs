// SPDX-License-Identifier: Apache-2.0

use std::fmt;

use serde::{Deserialize, Serialize};

/// Wire dialect for per-request agent headers.
///
/// Every dialect sends the generic Dynamo session headers. Harness dialects add
/// the harness-specific session, child, and compaction headers on top. The
/// dialect never changes traffic shape; shape comes from the trace or profile.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[cfg_attr(feature = "clap", derive(clap::ValueEnum))]
#[serde(rename_all = "kebab-case")]
pub enum HeaderDialect {
    #[default]
    Dynamo,
    ClaudeCode,
    Codex,
    Opencode,
}

impl HeaderDialect {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Dynamo => "dynamo",
            Self::ClaudeCode => "claude-code",
            Self::Codex => "codex",
            Self::Opencode => "opencode",
        }
    }
}

impl fmt::Display for HeaderDialect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
