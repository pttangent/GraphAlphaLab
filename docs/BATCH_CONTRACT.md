# Batch reporting contract

| Batch | Expected graph contracts | Primary output |
|---|---:|---|
| `implemented27` | 27 layer-scale contracts | Node baselines, graph variants, Alpha and robustness |
| `remaining14` | 14 layer-scale contracts | Node baselines, graph variants, Alpha and robustness |
| `theme_discovery` | 1 consensus membership output; 10 core Similarity inputs | Purity and structure; optional Alpha |
| `all41` | 41 layer-scale contracts | Hashed compact merge of 27 + 14 |

A layer-scale contract may contain multiple variants. The contract count is therefore based on unique `layer_id × scale_minutes`, not factor-row count.

The minimum governed Interaction variants are:

```text
node_baseline
graph_forward
graph_reverse_placebo
```

`node_baseline` must never be described as network Alpha.

Reports with fewer contracts are rejected unless `--allow-partial` is explicit. Compact merges reject partial or missing `_SUCCESS` bundles.
