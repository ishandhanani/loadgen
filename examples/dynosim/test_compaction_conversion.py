# SPDX-License-Identifier: Apache-2.0
"""Exercise the converter CLI at the exported-tree boundary."""

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


CONVERTER = Path(__file__).with_name("convert_agent_loadgen_to_dynosim_jsonl.py")


def node(name, hashes, dependencies, epoch=0, session="s", attempt=None):
    return {
        "dependencies": dependencies,
        "delay_after_dependencies_ms": 0,
        "window_epoch": epoch,
        "compaction_attempt": attempt,
        "output_budget_tokens": 16,
        "request": {
            "source_request_id": name,
            "trace_block_size": 16,
            "input_tokens": len(hashes) * 16,
            "output_tokens": 16,
            "input_sequence_hashes": hashes,
            "agent_context": {"session_id": session},
        },
    }


def compacted_tree():
    return {
        "index": 0,
        "nodes": [
            node("before", [1, 2, 3], []),
            node("compact", [1, 2, 3, 4], [0], attempt={
                "operation_id": "c1", "phase": "pre_turn", "attempt": 1,
                "expected_effect": "apply_once", "abort_after_ms": None,
            }),
            # Independent work has no dependency on the compaction request.
            node("other", [1, 8], [], session="other"),
            node("after", [1, 9], [1], epoch=1),
            node("later", [1, 9, 10], [3], epoch=1),
        ],
    }


class CompactionConversionTests(unittest.TestCase):
    def convert(self, tree):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        (root / "trees.jsonl").write_text(json.dumps(tree) + "\n")
        (root / "plan-summary.json").write_text(json.dumps({
            "top_level_sessions": 1, "sampled_trees": 0,
        }))
        output = root / "trace.jsonl"
        result = subprocess.run([
            sys.executable, str(CONVERTER), "--plan-dir", str(root),
            "--output", str(output), "--model", "dummy",
        ], capture_output=True, text=True)
        return result, output

    def test_successful_compaction_preserves_prompts_and_session_order(self):
        result, output = self.convert(compacted_tree())
        self.assertEqual(result.returncode, 0, result.stderr)
        rows = [json.loads(line) for line in output.read_text().splitlines()][1:]
        self.assertEqual([r["hash_ids"] for r in rows],
                         [[1, 2, 3], [1, 2, 3, 4], [1, 8], [1, 9], [1, 9, 10]])
        self.assertEqual(rows[1]["dependencies"][0]["request_id"], "before")
        self.assertEqual(rows[3]["dependencies"], [{
            "request_id": "compact", "trigger": "completion", "delay_ms": 0.0,
            "relation": "sequence",
        }])
        self.assertEqual(rows[2]["dependencies"], [])
        metadata = [json.loads(line) for line in
                    output.with_name(output.name + ".compaction.jsonl").read_text().splitlines()]
        self.assertEqual(metadata[0]["schema"], "agent_loadgen.compaction")
        self.assertEqual(metadata[1]["request_id"], "compact")
        self.assertEqual(metadata[1]["output_budget_tokens"], 16)
        self.assertEqual(metadata[2], {
            "event": "context_transition", "play_id": "agent-task-0000",
            "session_id": "s", "request_id": "after", "operation_id": "c1",
            "compaction_request_id": "compact", "old_window_epoch": 0,
            "new_window_epoch": 1,
        })

    def test_rejects_unexplained_prefix_change(self):
        tree = compacted_tree()
        tree["nodes"][4]["request"]["input_sequence_hashes"] = [1, 11, 10]
        result, output = self.convert(tree)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("not a cumulative full prompt", result.stderr)
        self.assertFalse(output.exists())
        self.assertFalse(output.with_name(output.name + ".compaction.jsonl").exists())

    def test_rejects_unproven_or_skipped_window_transition(self):
        for mutation in ("missing_attempt", "wrong_epoch", "no_dependency"):
            with self.subTest(mutation=mutation):
                tree = compacted_tree()
                if mutation == "missing_attempt":
                    tree["nodes"][1]["compaction_attempt"] = None
                elif mutation == "wrong_epoch":
                    tree["nodes"][3]["window_epoch"] = 2
                else:
                    tree["nodes"][3]["dependencies"] = []
                result, _ = self.convert(tree)
                self.assertNotEqual(result.returncode, 0)

    def test_rejects_abort_and_duplicate_instead_of_modeling_zero_output(self):
        for effect in ("no_mutation_aborted", "duplicate_noop"):
            with self.subTest(effect=effect):
                tree = compacted_tree()
                tree["nodes"][1]["compaction_attempt"]["expected_effect"] = effect
                tree["nodes"][1]["request"]["output_tokens"] = 0
                result, _ = self.convert(tree)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("requires replay lifecycle support", result.stderr)

    def test_rejects_missing_epoch_and_mismatched_output_budget(self):
        for mutation in ("missing_epoch", "budget"):
            with self.subTest(mutation=mutation):
                tree = compacted_tree()
                if mutation == "missing_epoch":
                    del tree["nodes"][1]["window_epoch"]
                else:
                    tree["nodes"][1]["output_budget_tokens"] = 32
                result, _ = self.convert(tree)
                self.assertNotEqual(result.returncode, 0)

    def test_compaction_must_consume_the_old_prefix(self):
        tree = compacted_tree()
        tree["nodes"][1]["request"]["input_sequence_hashes"] = [1, 9, 3, 4]
        result, _ = self.convert(tree)
        self.assertNotEqual(result.returncode, 0)

    def test_compaction_must_wait_for_its_predecessor(self):
        tree = compacted_tree()
        tree["nodes"][1]["dependencies"] = []
        result, _ = self.convert(tree)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must wait for its predecessor", result.stderr)

    def test_compaction_after_child_join_waits_for_child_and_parent(self):
        tree = compacted_tree()
        child = node("child", [1, 8], [0], session="child")
        child["request"]["agent_context"]["parent_session_id"] = "s"
        tree["nodes"][0]["spawned_session_ids"] = ["child"]
        compact = tree["nodes"][1]
        compact["dependencies"] = [1]
        after = tree["nodes"][3]
        after["dependencies"] = [2]
        tree["nodes"] = [tree["nodes"][0], child, compact, after]
        result, output = self.convert(tree)
        self.assertEqual(result.returncode, 0, result.stderr)
        rows = [json.loads(line) for line in output.read_text().splitlines()][1:]
        self.assertEqual(rows[1]["dependencies"][0]["trigger"], "dispatch")
        self.assertEqual({d["request_id"]: d["trigger"] for d in rows[2]["dependencies"]},
                         {"child": "completion", "before": "completion"})

    def test_distinct_trace_extensions_have_distinct_sidecars(self):
        result, output = self.convert(compacted_tree())
        self.assertEqual(result.returncode, 0, result.stderr)
        first_sidecar = Path(json.loads(result.stdout)["compaction_metadata"])
        original = first_sidecar.read_bytes()
        second = subprocess.run([
            sys.executable, str(CONVERTER), "--plan-dir", str(output.parent),
            "--output", str(output.with_suffix(".json")), "--model", "dummy",
        ], capture_output=True, text=True)
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertNotEqual(first_sidecar, Path(json.loads(second.stdout)["compaction_metadata"]))
        self.assertEqual(first_sidecar.read_bytes(), original)

    def test_ordinary_trace_has_no_compaction_records(self):
        tree = {"index": 0, "nodes": [node("a", [1], []), node("b", [1, 2], [0])]}
        result, output = self.convert(tree)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["full_prefix_edges_validated"], 1)
        self.assertEqual(len(output.with_name(output.name + ".compaction.jsonl").read_text().splitlines()), 1)


if __name__ == "__main__":
    unittest.main()
