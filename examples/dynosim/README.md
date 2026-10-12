# Export an agent-loadgen plan for DynoSim

> [!NOTE]
> The converter targets the `agentic_mooncake` replay format specifically. It has been correctness-tested with the agent-loadgen SWE-bench preset @ [`119f6480fae6`](https://github.com/NVIDIA-dev/agent-loadgen/blob/119f6480fae6d84244f70ae5387c754d2f430deb/profiles/swe-bench.toml); the preset does not include subagents nor compaction events. Also see the TODO list at the end of this README.

This example converts a finite agent-loadgen plan into DynoSim's `agentic_mooncake` JSONL format. The first row declares `"schema": "dynamo.agentic_mooncake"` and `"version": 2`, the trace schema version required by the replay parser. The output preserves request dependencies, session lineage, cumulative block hashes, dependency delays, tool classes, and deterministic source ordering through `source_play_ordinal`.


## Step 1: Export agent-loadgen data as session trees:

```bash
agent-loadgen export-plan \
  --config profiles/swe-bench.toml \
  --output artifacts/swe-bench-plan
```

## Step 2: Convert `trees.jsonl` to DynoSim `AgenticMooncake` format:

```bash
python examples/dynosim/convert_agent_loadgen_to_dynosim_jsonl.py \
  --plan-dir artifacts/swe-bench-plan \
  --output artifacts/swe-bench-agentic-mooncake.jsonl \
  --model nvidia/Llama-3.1-70B-Instruct-FP8
```

| Converter flag | Required? | Purpose / default |
| --- | --- | --- |
| `--plan-dir` | Required | Input directory containing `trees.jsonl` and `plan-summary.json`. |
| `--output` | Required | Output JSONL file path. |
| `--model` | Required | Model name written into DynoSim request metadata. |
| `--play-id-dynosim` | Optional | Program-ID label; defaults to `agent-task`. |

DynoSim's `agentic_mooncake` format uses the `play_id` field to group requests into a program. The converter assigns one ID per tree by appending its number to the label supplied by `--play-id-dynosim` (default: `agent-task`). For example, `--play-id-dynosim swe-task` produces `swe-task-0000`, `swe-task-0001`, etc.

`--model` sets the `model` field on each JSONL request for compatibility with DynoSim's replay format. The converter copies this value simply as metadata. Set it to the model name used by your DynoSim replay configuration.


## Step 3: Run with DynoSim:

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
For finite stream profiles, set `agentic_lanes` to the desired closed-loop concurrency.

## TODO

- Preserve per-stream `restart_delay_ms`. DynoSim currently assigns a new play immediately when an agentic lane becomes free; this omits the 1–5 second restart delay configured by `profiles/swe-bench.toml`.
- Define and validate compaction conversion, including explicit compaction signals, aborted and duplicate attempts, output budgets, and the post-compaction lineage transition. Compaction behavior has not been tested with the SWE-bench profile.
- Validate subagent conversion, including whether child dependencies should release on parent dispatch or completion and how spawn/join timing maps into AgenticMooncake. Subagent behavior has not been tested with the SWE-bench profile.
