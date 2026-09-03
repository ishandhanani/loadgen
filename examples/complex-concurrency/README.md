# Complex concurrent plan

This deliberately topology-rich profile is a visual example, not a calibrated production preset. It uses six finite top-level session trees over three closed-loop streams, tool and parallel-tool turns, single-child delegation, swarms, blocking and non-blocking continuations, compaction attempts, and stream restarts. Shape comes only from the TOML; the header dialect is chosen at run time with `--headers`.

Regenerate the graph, and `plan.svg` when Graphviz is installed:

```bash
cargo run -- plan \
  --config examples/complex-concurrency/complex-concurrency.toml \
  --output /tmp/complex-concurrency-plan
dot -Tsvg /tmp/complex-concurrency-plan/plan.dot -o plan.svg
```

The included `plan.dot` is the canonical graph source. The seeded plan contains 111 requests across 28 protocol sessions: six configured top-level trees and 22 generated child sessions. It has 15 logical compactions, each with one physical attempt because synthetic abort and retry injection are disabled. Its five multi-child joins converge on diamonds so the post-join delay is shown exactly once.
