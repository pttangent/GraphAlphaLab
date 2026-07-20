# Implementation status

Branch: `agent/monthly-alpha-research`

Implemented:

- governed monthly partition discovery for NFF/P0/P1
- reusable PIT-safe alpha-base cache
- Node, one-hop Graph, strict leave-one-out P1 and hierarchy residual experiments
- direction and absolute-return risk targets at 5m, 15m and 30m
- partition-level resume with config/input fingerprints
- RAM and disk resource gates
- governance findings for successful-but-empty graphs and missing core files
- per-partition shards, dashboard, progress JSON and monthly report outputs
- Windows/Linux CI across Python 3.11 and 3.13

Local validation before push:

- `python -m py_compile ...`: pass
- `python -m pytest -q`: 4 passed
- CLI `--help`: pass

Known boundary: this commit provides the monthly research engine and reporting contract. It does not claim that a full production month has already been executed on the user's local warehouse.
