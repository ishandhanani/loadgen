#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Convert an agent-loadgen plan to DynoSim agentic Mooncake JSONL."""

from __future__ import annotations

import argparse
import hashlib
import json
from itertools import chain
from pathlib import Path
from typing import TextIO


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Convert trees.jsonl from `agent-loadgen export-plan` to the "
            "dynamo.agentic_mooncake v2 format."
        )
    )
    parser.add_argument("--plan-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument(
        "--play-id-dynosim",
        default="agent-task",
        help="Program-ID label for DynoSim; each tree's number is appended.",
    )
    return parser.parse_args()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def read_trees(source: TextIO):
    for line in source:
        if line.strip():
            yield json.loads(line)


def depends_on(nodes: list[dict], index: int, ancestor: int) -> bool:
    pending = list(nodes[index]["dependencies"])
    visited = set()
    while pending:
        dependency = pending.pop()
        if dependency == ancestor:
            return True
        if dependency > ancestor and dependency not in visited:
            visited.add(dependency)
            pending.extend(nodes[dependency]["dependencies"])
    return False


def compaction_transitions(
    tree: dict, play_id: str,
) -> tuple[set[tuple[int, int]], list[dict], dict[int, int]]:
    """Validate planned adoption; do not infer runtime success from a header.

    Codex awaits compaction before continuing this session, and installs the
    replacement history only after a completed compaction response:
    https://github.com/openai/codex/blob/rust-v0.162.1/codex-rs/core/src/session/turn.rs#L618-L652
    https://github.com/openai/codex/blob/rust-v0.162.1/codex-rs/core/src/compact_remote_v2.rs#L294-L368
    """
    transitions = set()
    records = []
    previous_by_session = {}
    operation_ids = set()
    completion_waits = {}
    nodes = tree["nodes"]
    for index, node in enumerate(nodes):
        if any(type(dep) is not int or not 0 <= dep < index for dep in node["dependencies"]):
            raise ValueError("dependencies must reference earlier nodes")
    for index, node in enumerate(nodes):
        request = node["request"]
        session_id = request["agent_context"]["session_id"]
        request_id = request["source_request_id"]
        attempt = node.get("compaction_attempt")
        epoch = node.get("window_epoch", 0)
        if type(epoch) is not int or epoch < 0:
            raise ValueError(f"request {request_id} has an invalid window_epoch")
        common = {
            "play_id": play_id, "session_id": session_id, "request_id": request_id,
        }
        if attempt is not None:
            # The v2 replay schema has no cancellation deadline or duplicate
            # lifecycle. A zero logical output is not a zero-cost attempt.
            if (attempt["expected_effect"] != "apply_once"
                    or attempt["attempt"] != 1
                    or attempt.get("abort_after_ms") is not None):
                raise ValueError(
                    f"compaction request {request_id} requires replay lifecycle support "
                    "for aborted/duplicate attempts; use abort_probability=0 and "
                    "retry_probability=0 for this converter"
                )
            if "window_epoch" not in node:
                raise ValueError(f"compaction request {request_id} is missing window_epoch")
            operation_id = attempt["operation_id"]
            if not operation_id or operation_id in operation_ids:
                raise ValueError(f"duplicate or empty compaction operation: {operation_id!r}")
            operation_ids.add(operation_id)
            if (request["output_tokens"] <= 0
                    or request["output_tokens"] != node["output_budget_tokens"]):
                raise ValueError(f"compaction request {request_id} has an inconsistent output budget")
            records.append({
                "event": "compaction_attempt", **common, "window_epoch": epoch,
                "compaction_attempt": attempt,
                "output_budget_tokens": node["output_budget_tokens"],
            })

        previous_index = previous_by_session.get(session_id)
        if previous_index is None:
            if epoch != 0:
                raise ValueError(f"session {session_id} starts after an unrecorded compaction")
        else:
            previous = nodes[previous_index]
            previous_epoch = previous.get("window_epoch", 0)
            previous_attempt = previous.get("compaction_attempt")
            if attempt is not None and previous_index not in node["dependencies"]:
                if not depends_on(nodes, index, previous_index):
                    raise ValueError(f"compaction request {request_id} must wait for its predecessor")
                # A blocking child join can provide the causal path. Spawn
                # edges export as dispatch, so also preserve parent completion
                # before compaction (Codex's awaited continuation above).
                completion_waits[index] = previous_index
            if previous_attempt is not None or epoch != previous_epoch:
                if (previous_attempt is None or epoch != previous_epoch + 1
                        or previous_index not in node["dependencies"]):
                    raise ValueError(f"request {request_id} has an unproven compaction transition")
                transitions.add((previous_index, index))
                records.append({
                    "event": "context_transition", **common,
                    "operation_id": previous_attempt["operation_id"],
                    "compaction_request_id": previous["request"]["source_request_id"],
                    "old_window_epoch": previous_epoch, "new_window_epoch": epoch,
                })
        previous_by_session[session_id] = index
    return transitions, records, completion_waits


def main() -> None:
    args = parse_args()
    summary = json.loads((args.plan_dir / "plan-summary.json").read_text())
    tree_path = args.plan_dir / "trees.jsonl"
    source_digest = file_sha256(tree_path)
    tree_count = 0
    request_count = 0
    input_tokens = 0
    output_tokens = 0
    full_prefix_edges = 0
    compaction_transition_count = 0
    compaction_metadata = args.output.with_name(args.output.name + ".compaction.jsonl")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    try:
        with (tree_path.open() as source, args.output.open("w") as sink,
              compaction_metadata.open("w") as metadata_sink):
            trees = read_trees(source)
            first_tree = next(trees, None)
            if first_tree is None or not first_tree["nodes"]:
                raise ValueError("the exported plan contains no requests")
            block_size = int(
                first_tree["nodes"][0]["request"]["trace_block_size"]
            )
            header = {
                "schema": "dynamo.agentic_mooncake",
                "version": 2,
                "block_size": block_size,
                "hash_id_scope": "local",
                "source": {
                    "format": "agent-loadgen-plan-v1",
                    "digest": source_digest,
                },
            }
            sink.write(json.dumps(header, separators=(",", ":")) + "\n")
            metadata_sink.write(json.dumps({
                "schema": "agent_loadgen.compaction", "version": 1,
                "source": header["source"],
            }, separators=(",", ":")) + "\n")

            for expected_tree_index, tree in enumerate(chain([first_tree], trees)):
                tree_index = int(tree["index"])
                if tree_index != expected_tree_index:
                    raise ValueError(
                        "tree indexes must be contiguous: "
                        f"expected {expected_tree_index}, found {tree_index}"
                    )
                nodes = tree["nodes"]
                play_id = f"{args.play_id_dynosim}-{tree_index:04d}"
                transitions, records, completion_waits = compaction_transitions(tree, play_id)
                compaction_transition_count += len(transitions)
                for record in records:
                    metadata_sink.write(json.dumps(record, separators=(",", ":")) + "\n")
                request_ids = [
                    node["request"]["source_request_id"] for node in nodes
                ]

                for node_index, node in enumerate(nodes):
                    request = node["request"]
                    context = request.get("agent_context") or {}
                    tool_events = node.get("tool_events") or []
                    hashes = [
                        int(value) for value in request["input_sequence_hashes"]
                    ]
                    node_block_size = int(request["trace_block_size"])
                    if node_block_size != block_size:
                        raise ValueError(
                            "mixed source block sizes: "
                            f"{block_size} and {node_block_size}"
                        )
                    if int(request["input_tokens"]) != len(hashes) * block_size:
                        raise ValueError(
                            f"request {request_ids[node_index]} input length does not "
                            "match its full source-block sequence"
                        )

                    dependency_indexes = [
                        int(value) for value in node["dependencies"]
                    ]
                    if node_index in completion_waits:
                        dependency_indexes.append(completion_waits[node_index])
                    dependencies = []
                    for dependency_index in dependency_indexes:
                        dependency_node = nodes[dependency_index]
                        dependency_context = (
                            dependency_node["request"].get("agent_context") or {}
                        )
                        spawned = set(
                            dependency_node.get("spawned_session_ids", [])
                        )
                        is_spawn = (
                            context.get("parent_session_id")
                            == dependency_context.get("session_id")
                            and context.get("session_id") in spawned
                        )
                        dependencies.append(
                            {
                                "request_id": request_ids[dependency_index],
                                "trigger": "dispatch" if is_spawn else "completion",
                                "delay_ms": float(
                                    node["delay_after_dependencies_ms"]
                                ),
                                "relation": (
                                    "spawn"
                                    if is_spawn
                                    else "join"
                                    if len(dependency_indexes) > 1
                                    else "sequence"
                                ),
                            }
                        )

                        if (
                            dependency_context.get("session_id")
                            == context.get("session_id")
                            and (dependency_index, node_index) not in transitions
                        ):
                            prior_hashes = dependency_node["request"][
                                "input_sequence_hashes"
                            ]
                            if hashes[: len(prior_hashes)] != prior_hashes:
                                raise ValueError(
                                    f"request {request_ids[node_index]} is not a "
                                    "cumulative full prompt after "
                                    f"{request_ids[dependency_index]}"
                                )
                            full_prefix_edges += 1

                    row = {
                        "request_id": request_ids[node_index],
                        "play_id": play_id,
                        "source_play_ordinal": tree_index,
                        "session_id": context["session_id"],
                        "model": args.model,
                        "input_length": int(request["input_tokens"]),
                        "output_length": int(request["output_tokens"]),
                        "hash_ids": hashes,
                        "not_before_ms": 0.0,
                        "dependencies": dependencies,
                    }
                    if len(tool_events) == 1:
                        row["policy_class"] = str(tool_events[0]["class"])
                    elif len(tool_events) > 1:
                        row["policy_class"] = "parallel"
                    sink.write(json.dumps(row, separators=(",", ":")) + "\n")
                    request_count += 1
                    input_tokens += row["input_length"]
                    output_tokens += row["output_length"]
                tree_count += 1

        checks = {}
        top_level_sessions = summary.get("top_level_sessions")
        if top_level_sessions is not None:
            checks["trees"] = (tree_count, int(top_level_sessions))
        if summary.get("sampled_trees") == top_level_sessions:
            expected = summary["expected_totals"]
            checks.update(
                {
                    "requests": (request_count, round(expected["requests"])),
                    "input_tokens": (
                        input_tokens,
                        round(expected["input_tokens"]),
                    ),
                    "output_tokens": (
                        output_tokens,
                        round(expected["output_tokens"]),
                    ),
                }
            )
        mismatches = {
            name: values for name, values in checks.items() if values[0] != values[1]
        }
        if mismatches:
            raise ValueError(f"converted totals differ from source plan: {mismatches}")
    except Exception:
        args.output.unlink(missing_ok=True)
        compaction_metadata.unlink(missing_ok=True)
        raise

    print(
        json.dumps(
            {
                "output": str(args.output),
                "source_sha256": source_digest,
                "trace_sha256": file_sha256(args.output),
                "compaction_metadata": str(compaction_metadata),
                "compaction_metadata_sha256": file_sha256(compaction_metadata),
                "compaction_transitions": compaction_transition_count,
                "trees": tree_count,
                "requests": request_count,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "full_prefix_edges_validated": full_prefix_edges,
                "block_size": block_size,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
