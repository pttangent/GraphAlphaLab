# Known limitations

- The baseline engine currently evaluates one primary numeric node score per partition.
- Directed-edge semantics are normalized into a symmetric one-hop graph in the generic hierarchy runner; layer-specific lead-lag adapters remain a future extension.
- Full-month execution results depend on the user's local governed warehouse and are not bundled in the repository.
- Automated candidate discovery is not yet enabled in this initial framework release.
