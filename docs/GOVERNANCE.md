# Governance rules

- `_SUCCESS` does not override missing core files.
- Non-empty eligible nodes with zero edges are reported as a governance failure.
- P1 evaluation requires governed memberships and the corresponding P0 partition.
- Resume requires a matching configuration and upstream manifest fingerprint.
- GraphAlphaLab never repairs or rewrites upstream NFF/P0/P1 artifacts.
