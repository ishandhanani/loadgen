# Harness headers

`--headers {claude-code|codex}` is required on `generate`. It makes each request carry the headers that Claude Code or Codex sends, so a Dynamo deployment sees the same session identity, subagent lineage, compaction labels, and routing hints it would see from the real harness. The dialect never changes which requests are sent, their token shape, or their timing, and it is not part of any plan digest.

Neither dialect sends `x-dynamo-session-id`. Dynamo maps the harness headers to its own session and parent-session identity, and `x-dynamo-session-id` would override that mapping. `--dynamo-session-final` adds `x-dynamo-session-final: true` to the last request of each session for deployments that consume that Dynamo extension, such as the ThunderAgent router. No harness sends it.

Requests still go to Dynamo's OpenAI Chat Completions endpoint with `nvext.token_data`, because that endpoint accepts exact token IDs together with `ignore_eos`, which fixes the output length. Claude Code itself uses `/v1/messages` and Codex uses `/v1/responses`; Dynamo converts both into the same preprocessed request and derives agent identity from the same headers on every endpoint. Supply authentication or other target-specific headers with `--header NAME=VALUE`. The header names below are reserved and cannot be overridden.

## Claude Code

The reference is Anthropic's [gateway request headers](https://code.claude.com/docs/en/llm-gateway-protocol#request-headers), including the gateway hints added in Claude Code 2.1.273 and the prompt ID added in 2.1.283. Claude Code sends the hints to custom endpoints only when `CLAUDE_CODE_GATEWAY_HINT_HEADERS=1`; the dialect always sends them, as a Claude Code with that setting would.

| Header | Value |
|---|---|
| `x-claude-code-session-id` | The root session of the request's session tree. Every subagent shares it. |
| `x-claude-code-agent-id` | The requesting subagent's session. Absent on root-session requests. |
| `x-claude-code-parent-agent-id` | The parent subagent. Present only on nested subagents, not on children of the root. |
| `x-claude-code-request-class` | `compaction` on compaction attempts, otherwise `subagent` in a child session and `main` in a root session. |
| `x-claude-code-agent-type` | `general-purpose`, the Task tool's default agent, on a subagent's own turns. Generated children have no roles. |
| `x-claude-code-compaction` | The compaction trigger on compaction attempts: always `auto` for generated traffic. |
| `x-claude-code-context-compacted` | The same trigger, once, on the first root-session request after a compaction. |
| `x-claude-code-prev-tool-durations` | `<name>=<ms>;...` for the tool calls whose results the request carries. |
| `x-claude-code-prompt-id` | A UUID per user prompt. Every request serving that prompt shares it, including the turns of subagents the prompt started. |

Dynamo uses `x-claude-code-agent-id` as a child's session ID and `x-claude-code-parent-agent-id`, or else the root session, as its parent.

Previous tool durations follow the documented format: one entry per call in result order, whole milliseconds, percent-encoded names, and at most 32 entries and 4096 bytes. Generated profile tool classes are renamed to Claude Code tools: `read` to `Read`, `search` to `Grep`, `shell` to `Bash`, `patch` to `Edit`, `network` to `WebFetch`, and `orchestration` to `TodoWrite`. Other class names pass through. The durations are the sampled planned latencies, not measurements. The header is omitted after a batch that contains a retried call, because a retry aggregates two attempts, and on compaction attempts, subagent joins, and new user prompts.

## Codex

The reference is the Codex Rust client's Responses API headers.

| Header | Value |
|---|---|
| `originator` | `codex_cli_rs` |
| `session-id` | The root session of the request's session tree. |
| `thread-id`, `x-client-request-id` | The requesting session. |
| `x-codex-window-id` | `<thread>:<n>`, where `n` counts the compactions this session has completed. |
| `x-codex-parent-thread-id` | The parent session of a spawned child thread. |
| `x-openai-subagent` | `collab_spawn` on a spawned child thread. |
| `x-codex-turn-metadata` | JSON with `session_id`, `thread_id`, `turn_id`, `window_id`, `window_number`, and `request_kind` (`turn` or `compaction`); children add `parent_thread_id` and `subagent_kind: thread_spawn`. Compaction attempts add a `compaction` object with Codex's `trigger`, `reason`, `implementation`, `phase`, and `strategy` fields. |

A Codex turn starts at each user message in a session, including a child thread's delegated task, and a compaction continues the current turn. Dynamo uses `thread-id` as the session ID and `x-codex-parent-thread-id` as its parent.
