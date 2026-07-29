from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
from typing import Iterable

import pandas as pd
import pyarrow.parquet as pq

from .governance import atomic_write_frame, atomic_write_json, sha256_file, sha256_json


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_key(value: object, *, prefix: str = "unit") -> str:
    return f"{prefix}-{sha256_json(value)[:20]}"


def file_signature(path: str | Path) -> dict[str, object]:
    resolved = Path(path).expanduser().resolve()
    stat = resolved.stat()
    return {
        "path": str(resolved),
        "size_bytes": int(stat.st_size),
        "mtime_ns": int(stat.st_mtime_ns),
        "sha256": sha256_file(resolved),
    }


def signatures(paths: Iterable[str | Path]) -> list[dict[str, object]]:
    return [file_signature(path) for path in paths]


def write_progress(
    output_root: str | Path,
    *,
    stage: str,
    total: int,
    completed: int,
    reused: int = 0,
    failed: int = 0,
    current: str | None = None,
    status: str = "running",
    units: Iterable[dict[str, object]] = (),
    extra: dict[str, object] | None = None,
) -> dict[str, object]:
    output = Path(output_root).expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    payload: dict[str, object] = {
        "stage": stage,
        "status": status,
        "total_units": int(total),
        "completed_units": int(completed),
        "reused_units": int(reused),
        "failed_units": int(failed),
        "remaining_units": max(0, int(total) - int(completed)),
        "progress_pct": round(100.0 * completed / total, 3) if total else 100.0,
        "current_unit": current,
        "updated_at": utc_now(),
        "units": list(units),
    }
    if extra:
        payload.update(extra)
    atomic_write_json(output / "progress.json", payload)
    lines = [
        f"# {stage} progress",
        "",
        f"- Status: **{status}**",
        f"- Progress: **{completed}/{total} ({payload['progress_pct']}%)**",
        f"- Reused checkpoints: {reused}",
        f"- Failed units: {failed}",
        f"- Current: {current or '-'}",
        f"- Updated: {payload['updated_at']}",
        "",
        "| Unit | Status | Attempt | Detail |",
        "|---|---:|---:|---|",
    ]
    for unit in payload["units"]:
        lines.append(
            f"| {unit.get('unit', '')} | {unit.get('status', '')} | {unit.get('attempt', '')} | {unit.get('detail', '')} |"
        )
    temporary = output / f".DASHBOARD.md.{os.getpid()}.part"
    temporary.write_text("\n".join(lines) + "\n", encoding="utf-8")
    os.replace(temporary, output / "DASHBOARD.md")
    return payload


@dataclass(frozen=True)
class CheckpointSpec:
    stage: str
    unit: str
    contract_hash: str
    source_hash: str

    def as_dict(self) -> dict[str, str]:
        return {
            "stage": self.stage,
            "unit": self.unit,
            "contract_hash": self.contract_hash,
            "source_hash": self.source_hash,
        }


def checkpoint_valid(
    root: str | Path,
    spec: CheckpointSpec,
    *,
    required_files: Iterable[str] = (),
) -> bool:
    path = Path(root)
    marker = path / "checkpoint.json"
    if not marker.exists():
        return False
    try:
        payload = json.loads(marker.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    if payload.get("status") != "complete":
        return False
    for key, value in spec.as_dict().items():
        if key in ("contract_hash", "source_hash"):
            continue
        if str(payload.get(key)) != str(value):
            return False
    records = payload.get("files", [])
    indexed = {
        str(record.get("path")): record
        for record in records
        if isinstance(record, dict)
    }
    for name in required_files:
        file_path = path / name
        if not file_path.exists():
            return False
        record = indexed.get(name)
        if record is None:
            return False
        stat = file_path.stat()
        if int(record.get("size_bytes", -1)) != int(stat.st_size):
            return False
        if str(record.get("sha256")) != sha256_file(file_path):
            return False
        expected_rows = record.get("rows")
        if expected_rows is not None and file_path.suffix.lower() in {".parquet", ".pq"}:
            try:
                actual_rows = int(pq.ParquetFile(file_path).metadata.num_rows)
            except Exception:
                return False
            if int(expected_rows) != actual_rows:
                return False
    return True


def commit_frames(
    root: str | Path,
    spec: CheckpointSpec,
    frames: dict[str, pd.DataFrame],
    *,
    metadata: dict[str, object] | None = None,
) -> Path:
    target = Path(root).expanduser().resolve()
    pending = target.with_name(f".{target.name}.{os.getpid()}.pending")
    if pending.exists():
        shutil.rmtree(pending, ignore_errors=True)
    pending.mkdir(parents=True, exist_ok=False)
    records: list[dict[str, object]] = []
    try:
        for name, frame in frames.items():
            destination = pending / name
            atomic_write_frame(frame, destination)
            record = file_signature(destination)
            record["path"] = name
            record["rows"] = int(len(frame))
            records.append(record)
        payload: dict[str, object] = {
            **spec.as_dict(),
            "status": "complete",
            "completed_at": utc_now(),
            "files": records,
            "metadata": metadata or {},
        }
        atomic_write_json(pending / "checkpoint.json", payload)
        if target.exists():
            shutil.rmtree(target, ignore_errors=True)
        os.replace(pending, target)
    except Exception:
        shutil.rmtree(pending, ignore_errors=True)
        raise
    return target


def load_checkpoint_frames(
    root: str | Path,
    names: Iterable[str],
) -> dict[str, pd.DataFrame]:
    path = Path(root)
    result: dict[str, pd.DataFrame] = {}
    for name in names:
        file_path = path / name
        result[name] = (
            pd.read_parquet(file_path) if file_path.exists() else pd.DataFrame()
        )
    return result
