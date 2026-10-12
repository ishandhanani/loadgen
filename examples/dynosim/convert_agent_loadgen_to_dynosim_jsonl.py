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

    args.output.parent.mkdir(parents=True, exist_ok=True)
    try:
        with tree_path.open() as source, args.output.open("w") as sink:
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

            for expected_tree_index, tree in enumerate(chain([first_tree], trees)):
                tree_index = int(tree["index"])
                if tree_index != expected_tree_index:
                    raise ValueError(
                        "tree indexes must be contiguous: "
                        f"expected {expected_tree_index}, found {tree_index}"
                    )
                nodes = tree["nodes"]
                play_id = f"{args.play_id_dynosim}-{tree_index:04d}"
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
                            and not node.get("compaction_attempt")
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
        raise

    print(
        json.dumps(
            {
                "output": str(args.output),
                "source_sha256": source_digest,
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
