from __future__ import annotations

from dataclasses import asdict, dataclass
from functools import lru_cache
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import subprocess
from typing import Any, Iterable

import pandas as pd


@lru_cache(maxsize=2048)
def _sha256_cached(path_text: str, size: int, mtime_ns: int, chunk_size: int) -> str:
    digest = hashlib.sha256()
    with Path(path_text).open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_file(path: str | Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    resolved = Path(path).expanduser().resolve()
    stat = resolved.stat()
    return _sha256_cached(str(resolved), stat.st_size, stat.st_mtime_ns, chunk_size)


def sha256_json(payload: Any) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(canonical).hexdigest()


def atomic_write_text(path: str | Path, text: str) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{os.getpid()}.part")
    temporary.write_text(text, encoding="utf-8")
    os.replace(temporary, target)


def atomic_write_json(path: str | Path, payload: Any) -> None:
    atomic_write_text(path, json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n")


def atomic_write_frame(frame: pd.DataFrame, path: str | Path) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{os.getpid()}.part")
    suffix = target.suffix.lower()
    if suffix == ".csv":
        frame.to_csv(temporary, index=False)
    elif suffix in {".parquet", ".pq"}:
        frame.to_parquet(temporary, index=False)
    else:
        raise ValueError(f"Unsupported atomic frame format: {target}")
    os.replace(temporary, target)


def git_state(root: str | Path | None = None) -> dict[str, object]:
    cwd = Path(root).resolve() if root else Path.cwd()
    expected = os.getenv("GAL_EXPECTED_GIT_COMMIT")
    try:
        commit = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=cwd, text=True, stderr=subprocess.DEVNULL
        ).strip()
        dirty = bool(
            subprocess.check_output(
                ["git", "status", "--porcelain"], cwd=cwd, text=True, stderr=subprocess.DEVNULL
            ).strip()
        )
    except (OSError, subprocess.CalledProcessError):
        commit = expected or "unknown"
        dirty = None
    return {"git_commit": commit, "git_dirty": dirty, "expected_git_commit": expected}


def runtime_versions() -> dict[str, str]:
    packages = ("numpy", "pandas", "pyarrow", "duckdb", "scipy", "scikit-learn", "psutil")
    result: dict[str, str] = {}
    for package in packages:
        try:
            result[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            result[package] = "missing"
    result.update(
        {
            "python": platform.python_version(),
            "python_implementation": platform.python_implementation(),
            "platform": platform.platform(),
            "machine": platform.machine(),
        }
    )
    return result


@dataclass(frozen=True)
class ResourceBudget:
    memory_limit_gb: float = 24.0
    threads: int = 8
    temp_directory: str | None = None

    def validate(self) -> None:
        if self.memory_limit_gb <= 0:
            raise ValueError("memory_limit_gb must be positive")
        if self.threads <= 0:
            raise ValueError("threads must be positive")

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def configure_duckdb(connection: Any, budget: ResourceBudget) -> None:
    budget.validate()
    connection.execute(f"PRAGMA threads={int(budget.threads)}")
    connection.execute(f"PRAGMA memory_limit='{float(budget.memory_limit_gb):g}GB'")
    connection.execute("PRAGMA preserve_insertion_order=false")
    if budget.temp_directory:
        temp = Path(budget.temp_directory).expanduser().resolve()
        temp.mkdir(parents=True, exist_ok=True)
        escaped = str(temp).replace("'", "''")
        connection.execute(f"PRAGMA temp_directory='{escaped}'")


def file_record(path: str | Path) -> dict[str, object]:
    resolved = Path(path).expanduser().resolve()
    stat = resolved.stat()
    return {
        "path": str(resolved),
        "size_bytes": int(stat.st_size),
        "mtime_ns": int(stat.st_mtime_ns),
        "sha256": sha256_file(resolved),
    }


def directory_parquet_records(path: str | Path) -> list[dict[str, object]]:
    root = Path(path).expanduser().resolve()
    files = [root] if root.is_file() else sorted(root.rglob("*.parquet"))
    return [file_record(file) for file in files]


def implementation_manifest(
    *,
    operation: str,
    parameters: dict[str, object],
    inputs: Iterable[dict[str, object]],
    resource_budget: ResourceBudget,
    repository_root: str | Path | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "operation": operation,
        "parameters": parameters,
        "inputs": list(inputs),
        "resource_budget": resource_budget.as_dict(),
        "runtime_versions": runtime_versions(),
        **git_state(repository_root),
    }
    payload["contract_hash"] = sha256_json(payload)
    return payload


def enforce_git_lineage(manifest: dict[str, object], *, expected_commit: str | None, require_clean: bool) -> None:
    actual = str(manifest.get("git_commit", "unknown"))
    dirty = manifest.get("git_dirty")
    if expected_commit and actual != expected_commit:
        raise ValueError(f"GAL Git commit mismatch: expected {expected_commit}, got {actual}")
    if require_clean and dirty is not False:
        raise ValueError(f"A governed run requires a clean checkout; git_dirty={dirty!r}")
