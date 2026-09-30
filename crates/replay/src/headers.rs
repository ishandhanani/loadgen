// SPDX-License-Identifier: Apache-2.0

//! Coding-agent harness headers.
//!
//! Headers label requests the way Claude Code or Codex would. They never
//! change which requests are sent or when.

use std::collections::HashMap;
use std::fmt::Write;

use anyhow::{Result, bail};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

use agent_loadgen_core::{AgentContext, HeaderDialect};

pub(crate) type Headers = Vec<(String, String)>;

/// Header names agent-loadgen renders; `--header` cannot override them.
const MANAGED_HEADERS: &[&str] = &[
    "x-request-id",
    "x-dynamo-session-id",
    "x-dynamo-parent-session-id",
    "x-dynamo-session-final",
    "x-claude-code-session-id",
    "x-claude-code-agent-id",
    "x-claude-code-parent-agent-id",
    "x-claude-code-request-class",
    "x-claude-code-agent-type",
    "x-claude-code-compaction",
    "x-claude-code-context-compacted",
    "x-claude-code-prev-tool-durations",
    "x-claude-code-prompt-id",
    "originator",
    "session-id",
    "thread-id",
    "x-client-request-id",
    "x-codex-window-id",
    "x-codex-turn-metadata",
    "x-codex-parent-thread-id",
    "x-openai-subagent",
];

pub(crate) fn is_managed_header(name: &str) -> bool {
    MANAGED_HEADERS
        .iter()
        .any(|header| name.eq_ignore_ascii_case(header))
}

/// One request as the header renderer sees it. Requests are in causal order.
pub(crate) struct HeaderInput<'a> {
    pub(crate) context: &'a AgentContext,
    /// Earlier requests in the same slice that this request waits for.
    pub(crate) dependencies: &'a [usize],
    /// Tool calls launched by this request's response as `(name, milliseconds)`.
    /// Empty when per-call durations are unknown.
    pub(crate) launched_tools: Vec<(&'a str, u64)>,
}

/// Render every request's harness headers.
///
/// A request whose captured `agent_headers` already contain this harness's
/// headers replays them verbatim. Otherwise the headers are derived from
/// session lineage, compaction metadata, input triggers, and tool timings.
pub(crate) fn render(dialect: HeaderDialect, requests: &[HeaderInput<'_>]) -> Result<Vec<Headers>> {
    let lineage = Lineage::new(requests)?;
    let mut sessions: HashMap<&str, SessionState> = HashMap::new();
    let mut facts = Vec::with_capacity(requests.len());
    for (index, request) in requests.iter().enumerate() {
        let context = request.context;
        let compaction = context.compaction.is_some();
        let user_turn = context.input_trigger.as_deref() == Some("user_message");
        let first = !sessions.contains_key(context.session_id.as_str());
        let inherited_prompt = if first && context.parent_session_id.is_some() {
            spawning_prompt(requests, index, &facts, &sessions)
        } else {
            None
        };
        let state = sessions.entry(context.session_id.as_str()).or_default();
        if first || (user_turn && !compaction) {
            state.turns += 1;
            state.turn_id = stable_uuid(&[&context.session_id, "turn", &state.turns.to_string()]);
            if first || context.parent_session_id.is_none() {
                state.prompt_id = inherited_prompt.unwrap_or_else(|| state.turn_id.clone());
            }
        }
        let mut context_compacted = None;
        if compaction {
            state.compacting = Some(compaction_trigger(context));
            state.pending_tools.clear();
        } else if let Some(trigger) = state.compacting.take() {
            state.window += 1;
            if context.parent_session_id.is_none() {
                context_compacted = trigger;
            }
        }
        let tools = if !compaction && context.input_trigger.as_deref() == Some("tool_result") {
            std::mem::take(&mut state.pending_tools)
        } else {
            Vec::new()
        };
        state.pending_tools = request
            .launched_tools
            .iter()
            .map(|(name, ms)| ((*name).to_string(), *ms))
            .collect();
        facts.push(RequestFacts {
            turn_id: state.turn_id.clone(),
            prompt_id: state.prompt_id.clone(),
            window: state.window,
            context_compacted,
            tools,
        });
    }

    requests
        .iter()
        .zip(&facts)
        .map(|(request, facts)| {
            if let Some(captured) = captured_headers(dialect, request.context) {
                return Ok(captured);
            }
            match dialect {
                HeaderDialect::ClaudeCode => claude(request.context, facts, &lineage),
                HeaderDialect::Codex => Ok(codex(request.context, facts, &lineage)),
            }
        })
        .collect()
}

#[derive(Default)]
struct SessionState {
    turns: usize,
    turn_id: String,
    prompt_id: String,
    window: usize,
    /// Set while a compaction run is in progress, holding its known trigger.
    compacting: Option<Option<&'static str>>,
    pending_tools: Vec<(String, u64)>,
}

struct RequestFacts {
    turn_id: String,
    prompt_id: String,
    window: usize,
    context_compacted: Option<&'static str>,
    tools: Vec<(String, u64)>,
}

/// A child's first request serves the prompt of the parent request that spawned it.
fn spawning_prompt(
    requests: &[HeaderInput<'_>],
    index: usize,
    facts: &[RequestFacts],
    sessions: &HashMap<&str, SessionState>,
) -> Option<String> {
    let parent = requests[index].context.parent_session_id.as_deref()?;
    requests[index]
        .dependencies
        .iter()
        .rev()
        .find(|dependency| requests[**dependency].context.session_id == parent)
        .map(|dependency| facts[*dependency].prompt_id.clone())
        .or_else(|| sessions.get(parent).map(|state| state.prompt_id.clone()))
}

/// The trigger vocabulary Claude Code and Codex share: `auto`, `manual`, `reactive`.
fn compaction_trigger(context: &AgentContext) -> Option<&'static str> {
    match context.compaction.as_ref()?.get("trigger")?.as_str()? {
        "auto" => Some("auto"),
        "manual" => Some("manual"),
        "reactive" => Some("reactive"),
        _ => None,
    }
}

fn captured_headers(dialect: HeaderDialect, context: &AgentContext) -> Option<Headers> {
    let owned = |name: &str| match dialect {
        HeaderDialect::ClaudeCode => name.starts_with("x-claude-code-"),
        HeaderDialect::Codex => {
            name.starts_with("x-codex-")
                || name.starts_with("x-openai-")
                || matches!(name, "thread-id" | "session-id")
        }
    };
    let headers: Headers = context
        .agent_headers
        .iter()
        .filter(|(name, _)| owned(name))
        .flat_map(|(name, values)| values.iter().map(|value| (name.clone(), value.clone())))
        .collect();
    (!headers.is_empty()).then_some(headers)
}

fn claude(context: &AgentContext, facts: &RequestFacts, lineage: &Lineage<'_>) -> Result<Headers> {
    let root = lineage.root(&context.session_id)?;
    let mut headers = vec![header("x-claude-code-session-id", root)];
    let parent = context.parent_session_id.as_deref();
    if let Some(parent) = parent {
        headers.push(header("x-claude-code-agent-id", &context.session_id));
        if parent != root {
            headers.push(header("x-claude-code-parent-agent-id", parent));
        }
    }
    let compaction = context.compaction.is_some();
    let class = match (compaction, parent) {
        (true, _) => "compaction",
        (false, Some(_)) => "subagent",
        (false, None) => "main",
    };
    headers.push(header("x-claude-code-request-class", class));
    if class == "subagent" {
        // The Task tool's default agent; generated children have no roles.
        headers.push(header("x-claude-code-agent-type", "general-purpose"));
    }
    if compaction && let Some(trigger) = compaction_trigger(context) {
        headers.push(header("x-claude-code-compaction", trigger));
    }
    if let Some(trigger) = facts.context_compacted {
        headers.push(header("x-claude-code-context-compacted", trigger));
    }
    if let Some(value) = tool_durations(&facts.tools) {
        headers.push(header("x-claude-code-prev-tool-durations", &value));
    }
    headers.push(header("x-claude-code-prompt-id", &facts.prompt_id));
    Ok(headers)
}

fn codex(context: &AgentContext, facts: &RequestFacts, lineage: &Lineage<'_>) -> Headers {
    let thread = context.session_id.as_str();
    let session = lineage.known_root(thread);
    let window_id = format!("{thread}:{}", facts.window);
    let mut metadata = Map::new();
    metadata.insert("session_id".into(), json!(session));
    metadata.insert("thread_id".into(), json!(thread));
    metadata.insert("turn_id".into(), json!(facts.turn_id));
    metadata.insert("window_id".into(), json!(window_id));
    metadata.insert("window_number".into(), json!(facts.window));
    let mut headers = vec![
        header("originator", "codex_cli_rs"),
        header("session-id", session),
        header("thread-id", thread),
        header("x-client-request-id", thread),
        header("x-codex-window-id", &window_id),
    ];
    if let Some(parent) = &context.parent_session_id {
        headers.push(header("x-codex-parent-thread-id", parent));
        headers.push(header("x-openai-subagent", "collab_spawn"));
        metadata.insert("parent_thread_id".into(), json!(parent));
        metadata.insert("subagent_kind".into(), json!("thread_spawn"));
    }
    if let Some(compaction) = &context.compaction {
        metadata.insert("request_kind".into(), json!("compaction"));
        metadata.insert("compaction".into(), codex_compaction(compaction));
    } else {
        metadata.insert("request_kind".into(), json!("turn"));
    }
    headers.push(header(
        "x-codex-turn-metadata",
        &Value::Object(metadata).to_string(),
    ));
    headers
}

/// Keep only Codex's compaction vocabulary; captured traces may carry it verbatim.
fn codex_compaction(compaction: &Value) -> Value {
    let mut fields = Map::new();
    for key in ["trigger", "reason", "implementation", "phase", "strategy"] {
        if let Some(value) = compaction.get(key) {
            fields.insert(key.into(), value.clone());
        }
    }
    Value::Object(fields)
}

fn header(name: &str, value: &str) -> (String, String) {
    (name.to_string(), value.to_string())
}

/// Claude Code's names for the generic profile tool classes.
fn claude_tool_name(class: &str) -> &str {
    match class {
        "read" => "Read",
        "search" => "Grep",
        "shell" => "Bash",
        "patch" => "Edit",
        "network" => "WebFetch",
        "orchestration" => "TodoWrite",
        other => other,
    }
}

/// `<name>=<ms>;...` with percent-encoded names, at most 32 entries and 4096 bytes.
fn tool_durations(tools: &[(String, u64)]) -> Option<String> {
    let mut value = String::new();
    for (name, duration_ms) in tools.iter().take(32) {
        let mut entry = String::new();
        for byte in claude_tool_name(name).bytes() {
            if (b'!'..=b'~').contains(&byte) && !matches!(byte, b'%' | b';' | b'=' | b',') {
                entry.push(char::from(byte));
            } else {
                write!(entry, "%{byte:02X}").expect("write to String");
            }
        }
        write!(entry, "={duration_ms}").expect("write to String");
        let separator = usize::from(!value.is_empty());
        if value.len() + separator + entry.len() > 4096 {
            break;
        }
        if separator != 0 {
            value.push(';');
        }
        value.push_str(&entry);
    }
    (!value.is_empty()).then_some(value)
}

/// A random-format UUID derived from stable request identity.
fn stable_uuid(parts: &[&str]) -> String {
    let mut digest = Sha256::new();
    digest.update(b"agent-loadgen/harness-id/v1");
    for part in parts {
        digest.update((part.len() as u64).to_le_bytes());
        digest.update(part.as_bytes());
    }
    let bytes: [u8; 16] = digest.finalize()[..16]
        .try_into()
        .expect("the digest prefix is sixteen bytes");
    uuid::Builder::from_random_bytes(bytes)
        .into_uuid()
        .to_string()
}

/// Session parents for one workload, used to find each tree's root session.
struct Lineage<'a> {
    parents: HashMap<&'a str, Option<&'a str>>,
}

impl<'a> Lineage<'a> {
    fn new(requests: &[HeaderInput<'a>]) -> Result<Self> {
        let mut parents = HashMap::new();
        for request in requests {
            let session = request.context.session_id.as_str();
            let parent = request.context.parent_session_id.as_deref();
            if parents
                .insert(session, parent)
                .is_some_and(|previous| previous != parent)
            {
                bail!("session {session} has conflicting parents");
            }
        }
        Ok(Self { parents })
    }

    /// The root of `session`'s tree. A missing ancestor is ambiguous, because
    /// it might itself be a subagent.
    fn root(&self, session: &'a str) -> Result<&'a str> {
        let mut current = session;
        for _ in 0..=self.parents.len() {
            match self.parents.get(current) {
                Some(Some(parent)) => current = parent,
                Some(None) => return Ok(current),
                None => bail!(
                    "cannot resolve the Claude Code session root of {session}: ancestor {current} is not in the workload; include the complete session tree or use --headers codex"
                ),
            }
        }
        bail!("session lineage contains a cycle at {session}")
    }

    /// The topmost ancestor present in this workload.
    fn known_root(&self, session: &'a str) -> &'a str {
        let mut current = session;
        for _ in 0..=self.parents.len() {
            match self.parents.get(current) {
                Some(Some(parent)) if self.parents.contains_key(parent) => current = parent,
                _ => break,
            }
        }
        current
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn context(session: &str, parent: Option<&str>, trigger: &str) -> AgentContext {
        AgentContext {
            session_id: session.to_string(),
            parent_session_id: parent.map(str::to_string),
            compaction: None,
            input_trigger: Some(trigger.to_string()),
            agent_headers: BTreeMap::new(),
        }
    }

    fn value<'a>(headers: &'a Headers, name: &str) -> Option<&'a str> {
        headers
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }

    fn inputs<'a>(
        contexts: &'a [AgentContext],
        dependencies: &'a [Vec<usize>],
    ) -> Vec<HeaderInput<'a>> {
        contexts
            .iter()
            .zip(dependencies)
            .map(|(context, dependencies)| HeaderInput {
                context,
                dependencies,
                launched_tools: Vec::new(),
            })
            .collect()
    }

    #[test]
    fn claude_lineage_uses_the_root_session_and_nested_parents_only() {
        let contexts = [
            context("root", None, "user_message"),
            context("child", Some("root"), "user_message"),
            context("nested", Some("child"), "user_message"),
            context("root", None, "tool_result"),
            context("other", None, "user_message"),
        ];
        let dependencies = [vec![], vec![0], vec![1], vec![0], vec![]];
        let headers = render(HeaderDialect::ClaudeCode, &inputs(&contexts, &dependencies)).unwrap();
        let expected = [
            ("root", None, None, "main"),
            ("root", Some("child"), None, "subagent"),
            ("root", Some("nested"), Some("child"), "subagent"),
            ("root", None, None, "main"),
            ("other", None, None, "main"),
        ];
        for (headers, (session, agent, parent, class)) in headers.iter().zip(expected) {
            assert_eq!(value(headers, "x-claude-code-session-id"), Some(session));
            assert_eq!(value(headers, "x-claude-code-agent-id"), agent);
            assert_eq!(value(headers, "x-claude-code-parent-agent-id"), parent);
            assert_eq!(value(headers, "x-claude-code-request-class"), Some(class));
            assert_eq!(
                value(headers, "x-claude-code-agent-type"),
                agent.map(|_| "general-purpose")
            );
            assert!(
                headers
                    .iter()
                    .all(|(name, _)| !name.starts_with("x-dynamo-"))
            );
        }
        // Children serve the prompt that spawned them; another root has its own.
        let prompt = value(&headers[0], "x-claude-code-prompt-id").unwrap();
        assert!(uuid::Uuid::parse_str(prompt).is_ok());
        for headers in &headers[1..4] {
            assert_eq!(value(headers, "x-claude-code-prompt-id"), Some(prompt));
        }
        assert_ne!(value(&headers[4], "x-claude-code-prompt-id"), Some(prompt));
    }

    #[test]
    fn claude_rejects_ambiguous_lineage_but_codex_uses_the_known_root() {
        let contexts = [context("child", Some("root"), "user_message")];
        let dependencies = [vec![]];
        let requests = inputs(&contexts, &dependencies);
        let error = render(HeaderDialect::ClaudeCode, &requests).unwrap_err();
        assert!(error.to_string().contains("ancestor root"));
        let headers = render(HeaderDialect::Codex, &requests).unwrap();
        assert_eq!(value(&headers[0], "session-id"), Some("child"));
        assert_eq!(value(&headers[0], "x-codex-parent-thread-id"), Some("root"));

        let conflicting = [
            context("child", Some("root"), "user_message"),
            context("child", None, "tool_result"),
        ];
        let dependencies = [vec![], vec![0]];
        assert!(
            render(HeaderDialect::Codex, &inputs(&conflicting, &dependencies))
                .unwrap_err()
                .to_string()
                .contains("conflicting parents")
        );
    }

    #[test]
    fn compaction_marks_the_attempts_and_the_next_main_turn_once() {
        let mut attempt = context("root", None, "other");
        attempt.compaction =
            Some(json!({"trigger": "auto", "phase": "pre_turn", "operation_id": "op"}));
        let contexts = [
            context("root", None, "user_message"),
            attempt.clone(),
            attempt,
            context("root", None, "other"),
            context("root", None, "user_message"),
        ];
        let dependencies = [vec![], vec![0], vec![1], vec![2], vec![3]];
        let requests = inputs(&contexts, &dependencies);
        let claude = render(HeaderDialect::ClaudeCode, &requests).unwrap();
        for index in [1, 2] {
            assert_eq!(
                value(&claude[index], "x-claude-code-request-class"),
                Some("compaction")
            );
            assert_eq!(
                value(&claude[index], "x-claude-code-compaction"),
                Some("auto")
            );
        }
        let compacted = claude
            .iter()
            .map(|headers| value(headers, "x-claude-code-context-compacted"))
            .collect::<Vec<_>>();
        assert_eq!(compacted, [None, None, None, Some("auto"), None]);

        let codex = render(HeaderDialect::Codex, &requests).unwrap();
        let windows = codex
            .iter()
            .map(|headers| value(headers, "x-codex-window-id").unwrap())
            .collect::<Vec<_>>();
        assert_eq!(windows, ["root:0", "root:0", "root:0", "root:1", "root:1"]);
        let metadata: Value =
            serde_json::from_str(value(&codex[1], "x-codex-turn-metadata").unwrap()).unwrap();
        assert_eq!(metadata["request_kind"], "compaction");
        assert_eq!(
            metadata["compaction"],
            json!({"trigger": "auto", "phase": "pre_turn"})
        );
        let turn: Value =
            serde_json::from_str(value(&codex[3], "x-codex-turn-metadata").unwrap()).unwrap();
        assert_eq!(turn["request_kind"], "turn");
        assert_eq!(turn["window_id"], "root:1");
        // A compaction continues the current turn; a user message starts the next.
        assert_eq!(metadata["turn_id"], turn["turn_id"]);
        let next: Value =
            serde_json::from_str(value(&codex[4], "x-codex-turn-metadata").unwrap()).unwrap();
        assert_ne!(next["turn_id"], turn["turn_id"]);
    }

    #[test]
    fn tool_durations_follow_the_consuming_request_only() {
        let contexts = [
            context("root", None, "user_message"),
            context("root", None, "tool_result"),
            context("root", None, "user_message"),
        ];
        let dependencies = [vec![], vec![0], vec![1]];
        let mut requests = inputs(&contexts, &dependencies);
        requests[0].launched_tools = vec![("shell", 742), ("read", 9)];
        requests[1].launched_tools = vec![("search", 30)];
        let headers = render(HeaderDialect::ClaudeCode, &requests).unwrap();
        assert_eq!(
            value(&headers[0], "x-claude-code-prev-tool-durations"),
            None
        );
        assert_eq!(
            value(&headers[1], "x-claude-code-prev-tool-durations"),
            Some("Bash=742;Read=9")
        );
        // A new user prompt never carries the previous batch.
        assert_eq!(
            value(&headers[2], "x-claude-code-prev-tool-durations"),
            None
        );
        assert_ne!(
            value(&headers[1], "x-claude-code-prompt-id"),
            value(&headers[2], "x-claude-code-prompt-id")
        );
    }

    #[test]
    fn tool_durations_encode_names_and_cap_whole_entries() {
        let tools = vec![("custom tool".to_string(), 1), ("% ;=,é\n".to_string(), 10)];
        assert_eq!(
            tool_durations(&tools).unwrap(),
            "custom%20tool=1;%25%20%3B%3D%2C%C3%A9%0A=10"
        );
        let many = vec![("shell".to_string(), 1); 40];
        assert_eq!(tool_durations(&many).unwrap().split(';').count(), 32);
        let exact = ("x".repeat(4094), 1);
        assert_eq!(
            tool_durations(&[exact, many[0].clone()]).unwrap().len(),
            4096
        );
        assert!(tool_durations(&[("x".repeat(4095), 1)]).is_none());
        assert!(tool_durations(&[]).is_none());
    }

    #[test]
    fn captured_harness_headers_replay_verbatim() {
        let mut captured = context("child", Some("root"), "tool_result");
        captured.agent_headers = BTreeMap::from([
            (
                "x-claude-code-session-id".to_string(),
                vec!["root".to_string()],
            ),
            (
                "x-claude-code-agent-id".to_string(),
                vec!["child".to_string()],
            ),
            (
                "x-claude-code-future".to_string(),
                vec!["a".to_string(), "b".to_string()],
            ),
            ("thread-id".to_string(), vec!["thread".to_string()]),
        ]);
        let contexts = [context("root", None, "user_message"), captured];
        let dependencies = [vec![], vec![0]];
        let requests = inputs(&contexts, &dependencies);
        let claude = render(HeaderDialect::ClaudeCode, &requests).unwrap();
        assert_eq!(
            claude[1],
            vec![
                header("x-claude-code-agent-id", "child"),
                header("x-claude-code-future", "a"),
                header("x-claude-code-future", "b"),
                header("x-claude-code-session-id", "root"),
            ]
        );
        let codex = render(HeaderDialect::Codex, &requests).unwrap();
        assert_eq!(codex[1], vec![header("thread-id", "thread")]);
        assert_eq!(value(&codex[0], "thread-id"), Some("root"));
    }

    #[test]
    fn codex_children_carry_spawn_lineage() {
        let contexts = [
            context("root", None, "user_message"),
            context("child", Some("root"), "user_message"),
        ];
        let dependencies = [vec![], vec![0]];
        let headers = render(HeaderDialect::Codex, &inputs(&contexts, &dependencies)).unwrap();
        assert_eq!(value(&headers[0], "originator"), Some("codex_cli_rs"));
        assert_eq!(value(&headers[0], "x-openai-subagent"), None);
        let child = &headers[1];
        assert_eq!(value(child, "session-id"), Some("root"));
        assert_eq!(value(child, "thread-id"), Some("child"));
        assert_eq!(value(child, "x-client-request-id"), Some("child"));
        assert_eq!(value(child, "x-codex-parent-thread-id"), Some("root"));
        assert_eq!(value(child, "x-openai-subagent"), Some("collab_spawn"));
        let metadata: Value =
            serde_json::from_str(value(child, "x-codex-turn-metadata").unwrap()).unwrap();
        assert_eq!(metadata["session_id"], "root");
        assert_eq!(metadata["parent_thread_id"], "root");
        assert_eq!(metadata["subagent_kind"], "thread_spawn");
    }

    #[test]
    fn managed_header_names_are_case_insensitive() {
        assert!(is_managed_header("X-Request-ID"));
        assert!(is_managed_header("X-Claude-Code-Prompt-ID"));
        assert!(is_managed_header("thread-id"));
        assert!(!is_managed_header("x-claude-code-future-hint"));
        assert!(!is_managed_header("authorization"));
    }
}
