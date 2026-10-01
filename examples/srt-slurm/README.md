# agent-loadgen as an srt-slurm benchmark

This example runs agent-loadgen as the `custom` benchmark of an [srt-slurm](https://github.com/NVIDIA/srt-slurm) job: the job starts a model replica behind a Dynamo frontend with the KV router, and the benchmark step sends generated coding-agent traffic to that frontend from inside the job.

- `recipe.yaml`: one MiniMax-M3 NVFP4 replica on SGLang (TP4, one B200 node) behind the Dynamo KV router, with request tracing into the job's log directory. Dynamo is built from a `main` commit whose request traces record coding-agent headers.
- `run.sh`: the benchmark command. It runs every line of the run list against `SRT_FRONTEND_HOST:SRT_FRONTEND_PORT`, which srt-slurm injects, adding `--model`, `--target`, `--tokenizer /model`, and `--output`, and writes each run under `artifacts/<slurm job id>/<name>/`.
- `runs.txt`: one run per line: a name, then `generate --config <profile> --headers <dialect>` and any other flags. The job reads it when the benchmark step starts, so it can still be edited while the model loads.

## Set up a session directory

The recipe mounts one directory at `/loadgen`. Put the binary, profiles, script, and run list there:

```bash
S=/path/to/session            # on the cluster's shared filesystem
mkdir -p $S/bin $S/profiles $S/artifacts
gh release download -R NVIDIA-dev/agent-loadgen --pattern 'agent-loadgen-x86_64-unknown-linux-gnu.tar.gz' -O - \
  | tar xz --strip-components=1 -C $S/bin agent-loadgen-x86_64-unknown-linux-gnu/agent-loadgen
cp profiles/*.toml $S/profiles/
cp examples/srt-slurm/run.sh examples/srt-slurm/runs.txt $S/
```

Use the `aarch64-unknown-linux-gnu` archive on Grace nodes, or `cargo build --release` for a source build. Release binaries link only glibc (2.34 or newer) and need no OpenSSL, so they run in current SGLang and Dynamo containers. Edit the recipe's `model`, `extra_mount`, and `LOADGEN_MODEL`, and write the profiles and load files that `runs.txt` names, such as a profile with `duration_minutes` added to its `[load]` section or a `[load.arrivals]` schedule; see [Describe a workload](../../docs/workloads.md). Then submit from the srt-slurm checkout:

```bash
uv run srtctl dry-run -f recipe.yaml
uv run srtctl apply -f recipe.yaml
```

## Read the results

Each run directory holds `run.json`, `timeline.csv`, and `requests.jsonl`, and `plan-summary.json`; `stdout.json` and `stderr.log` capture the command's output, and the job directory keeps the binary and run list it used. The job's own log directory holds the Dynamo request trace (`request-trace*.jsonl.gz`), whose `agent_context` shows the session identity Dynamo derived from the harness headers, and each request's `x_request_id` matches a `replay_request_id` in `requests.jsonl`.
