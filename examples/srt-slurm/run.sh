#!/usr/bin/env bash
# agent-loadgen as an srt-slurm custom benchmark. Each line of the run list is
# "name subcommand flags...", such as `smoke generate --config ... --headers codex`.
# The script adds the target, model, tokenizer, and output directory.
set -uo pipefail
L=/loadgen
RUNS="${1:-$L/runs.txt}"
BIN="${LOADGEN_BIN:-$L/bin/agent-loadgen}"
TARGET="http://${SRT_FRONTEND_HOST:?}:${SRT_FRONTEND_PORT:?}"
OUT="$L/artifacts/${SLURM_JOB_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "$OUT"
cp "$BIN" "$OUT/agent-loadgen"
cp "$RUNS" "$OUT/runs.txt"
while read -r name subcommand args; do
  [[ -z "$name" || "$name" == \#* ]] && continue
  mkdir -p "$OUT/$name"
  echo "=== start $name $(date -u +%FT%TZ) $subcommand $args"
  # shellcheck disable=SC2086
  "$OUT/agent-loadgen" $subcommand $args \
    --model "${LOADGEN_MODEL:?}" --target "$TARGET" --tokenizer /model \
    --output "$OUT/$name" --max-in-flight 4096 \
    --max-dispatch-p99-ms 100 --max-dispatch-max-ms 2000 \
    > "$OUT/$name/stdout.json" 2> "$OUT/$name/stderr.log"
  echo "=== end $name exit=$? $(date -u +%FT%TZ)"
done < "$RUNS"
