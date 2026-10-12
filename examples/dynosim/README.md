# Export an agent-loadgen plan for DynoSim

This example converts a finite agent-loadgen plan into DynoSim's version 2 `agentic_mooncake` JSONL format. The output preserves request dependencies, session lineage, cumulative block hashes, dependency delays, tool classes, and deterministic source ordering through `source_play_ordinal`.

First export every planned session tree:

```bash
agent-loadgen export-plan \
  --config profiles/swe-bench.toml \
  --output artifacts/swe-bench-plan
```

Then convert `trees.jsonl`:

```bash
python examples/dynosim/convert_agent_loadgen_to_dynosim_jsonl.py \
  --plan-dir artifacts/swe-bench-plan \
  --output artifacts/swe-bench-agentic-mooncake.jsonl \
  --model nvidia/Llama-3.1-70B-Instruct-FP8
```

Use `--play-id-prefix` when a consumer needs a stable corpus-specific program ID prefix.

Pass the resulting file to Dynamo replay with `trace_format="agentic_mooncake"`. DynoSim reads the source block size from the first JSONL row and reblocks the synthesized prompt into the configured engine block size. For example:

```python
from dynamo.replay import run_trace_replay

report = run_trace_replay(
    "artifacts/swe-bench-agentic-mooncake.jsonl",
    trace_format="agentic_mooncake",
    agentic_lanes=32,
    model_name="nvidia/Llama-3.1-70B-Instruct-FP8",
    execution_model="nvidia/Llama-3.1-70B-Instruct-FP8",
    # Add the deployment and replay arguments for your DynoSim run.
)
```

The converter targets the `agentic_mooncake` replay format specifically. It does not produce the ordinary `mooncake`, `mooncake-delta`, WEKA, or Dynamo request-trace formats.

For finite stream profiles, set `agentic_lanes` to the desired closed-loop concurrency.

## TODO

- Preserve per-stream `restart_delay_ms`. DynoSim currently assigns a new play immediately when an agentic lane becomes free; this omits the 1–5 second restart delay configured by `profiles/swe-bench.toml`.
- Define and validate compaction conversion, including explicit compaction signals, aborted and duplicate attempts, output budgets, and the post-compaction lineage transition. Compaction behavior has not been tested with the SWE-bench profile.
- Validate subagent conversion, including whether child dependencies should release on parent dispatch or completion and how spawn/join timing maps into AgenticMooncake. Subagent behavior has not been tested with the SWE-bench profile.
