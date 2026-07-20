# Architecture

GraphAlphaLab is intentionally read-only with respect to upstream governed stores.

```text
NFF gff_core + trades_core
          │
          ▼
reusable PIT-safe alpha_base cache
          │
          ├── P0 node_projection
          ├── P0 edges
          └── P1 memberships
                    │
                    ▼
traditional controls → Node → Graph → P1 hierarchy
                    │
                    ▼
resumable partition shards → daily/monthly reports
```

The scheduler isolates work by `date × layer × scale`, applies RAM/disk gates before execution, and records failures without deleting successful shards.
