from __future__ import annotations

from pathlib import Path
import pandas as pd

from .governance import atomic_write_frame, atomic_write_json, atomic_write_text, sha256_file


def read_frame(path: str | Path) -> pd.DataFrame:
    resolved = Path(path).expanduser().resolve()
    suffix = resolved.suffix.lower()
    if suffix in {".parquet", ".pq"}:
        return pd.read_parquet(resolved)
    if suffix in {".csv", ".tsv"}:
        return pd.read_csv(resolved, sep="\t" if suffix == ".tsv" else ",")
    if suffix in {".jsonl", ".ndjson"}:
        return pd.read_json(resolved, lines=True)
    raise ValueError(f"Unsupported table format: {resolved}")


def write_frame(frame: pd.DataFrame, path: str | Path) -> None:
    atomic_write_frame(frame, path)


__all__ = [
    "atomic_write_frame",
    "atomic_write_json",
    "atomic_write_text",
    "read_frame",
    "sha256_file",
    "write_frame",
]
