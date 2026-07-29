from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from . import forward_volatility as core
from . import forward_volatility_report_pack as report_pack
from . import forward_volatility_report_streaming as streaming
from .governance import (
    atomic_write_json,
    atomic_write_text,
    file_record,
    sha256_file,
    sha256_json,
)


REPORT_PREFLIGHT_VERSION = "GAL_FORWARD_VOLATILITY_REPORT_PREFLIGHT_V4"
_ORIGINAL_FINALIZER = core._finalize_volatility_report
_SMALL_HASH_LIMIT = 8 * 1024 * 1024


def _fast_source_record(path: str | Path) -> dict[str, object]:
    resolved = Path(path).expanduser().resolve()
    stat = resolved.stat()
    if int(stat.st_size) <= _SMALL_HASH_LIMIT:
        digest = sha256_file(resolved)
        mode = "sha256"
    else:
        digest = sha256_json(
            {
                "path": str(resolved),
                "size_bytes": int(stat.st_size),
                "mtime_ns": int(stat.st_mtime_ns),
            }
        )
        mode = "size_mtime_anchor"
    return {
        "path": str(resolved),
        "size_bytes": int(stat.st_size),
        "mtime_ns": int(stat.st_mtime_ns),
        "sha256": digest,
        "fingerprint_mode": mode,
    }


def _raw_anchor(raw_root: Path, config) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for horizon_value in config.horizons:
        horizon = int(horizon_value)
        for target_value in config.targets:
            target = str(target_value)
            name = report_pack._horizon_name(target, horizon)
            root = raw_root / f"horizon={name}"
            for filename in report_pack.RAW_EVIDENCE_FILES:
                path = root / filename
                if not path.exists():
                    continue
                record = _fast_source_record(path)
                record.update(
                    {
                        "stage": "raw_volatility_diagnostic",
                        "target_kind": target,
                        "horizon_minutes": horizon,
                        "evidence_file": filename,
                    }
                )
                records.append(record)
    return records


def _label_anchor(output_root: Path) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for data_path in sorted((output_root / "labels").glob("trade_date=*/data.parquet")):
        checkpoint = data_path.parent / "checkpoint.json"
        if not checkpoint.exists():
            raise FileNotFoundError(f"Missing governed label checkpoint: {checkpoint}")
        checkpoint_record = file_record(checkpoint)
        checkpoint_record.update(
            {
                "stage": "forward_volatility_label_checkpoint",
                "trade_date": data_path.parent.name.removeprefix("trade_date="),
            }
        )
        records.append(checkpoint_record)
        data_record = _fast_source_record(data_path)
        data_record.update(
            {
                "stage": "forward_volatility_label_data",
                "trade_date": data_path.parent.name.removeprefix("trade_date="),
            }
        )
        records.append(data_record)
    if not records:
        raise FileNotFoundError(f"No forward-volatility labels found under {output_root / 'labels'}")
    return records


def _source_anchor(output_root: Path, raw_root: Path, config) -> list[dict[str, object]]:
    return [*_raw_anchor(raw_root, config), *_label_anchor(output_root)]


def _checkpoint_valid(report: Path, contract_hash: str) -> bool:
    marker = report / "checkpoint.json"
    success = report / "_SUCCESS"
    if not marker.exists() or not success.exists():
        return False
    try:
        payload = json.loads(marker.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    return (
        payload.get("status") == "complete"
        and payload.get("contract_hash") == contract_hash
        and report_pack._records_match(report, payload.get("artifacts"))
    )


def _write_preflight(
    output_root: Path,
    *,
    status: str,
    contract_hash: str,
    sources: list[dict[str, object]],
    reused: bool,
    detail: str | None = None,
) -> None:
    root = output_root / "_checkpoints" / "forward_volatility"
    root.mkdir(parents=True, exist_ok=True)
    atomic_write_json(
        root / "report_preflight.json",
        {
            "version": REPORT_PREFLIGHT_VERSION,
            "status": status,
            "contract_hash": contract_hash,
            "source_count": len(sources),
            "reused": bool(reused),
            "detail": detail,
        },
    )


def _artifact_records(report: Path) -> list[dict[str, object]]:
    return [
        report_pack._artifact_record(path, report)
        for path in sorted(report.rglob("*"))
        if path.is_file() and path.name not in {"checkpoint.json", "_SUCCESS"}
    ]


def _finalize_with_preflight(output_root, raw_root, config):
    output = Path(output_root).expanduser().resolve()
    raw = Path(raw_root).expanduser().resolve()
    report = output / "prediction_report"
    sources = _source_anchor(output, raw, config)
    contract_hash = sha256_json(
        {
            "version": REPORT_PREFLIGHT_VERSION,
            "config": config.as_dict(),
            "sources": [
                {
                    "path": str(record.get("path")),
                    "size_bytes": int(record.get("size_bytes", -1)),
                    "mtime_ns": int(record.get("mtime_ns", -1)),
                    "sha256": str(record.get("sha256")),
                    "fingerprint_mode": str(record.get("fingerprint_mode", "sha256")),
                }
                for record in sources
            ],
        }
    )
    if _checkpoint_valid(report, contract_hash):
        _write_preflight(
            output,
            status="complete",
            contract_hash=contract_hash,
            sources=sources,
            reused=True,
            detail="complete report artifacts reused before reading raw evidence",
        )
        return report

    _write_preflight(
        output,
        status="building",
        contract_hash=contract_hash,
        sources=sources,
        reused=False,
        detail="source anchor changed or report artifacts were incomplete",
    )
    try:
        report = _ORIGINAL_FINALIZER(output, raw, config)
        success = report / "_SUCCESS"
        success.unlink(missing_ok=True)
        artifacts = _artifact_records(report)
        atomic_write_json(
            report / "checkpoint.json",
            {
                "status": "complete",
                "contract_hash": contract_hash,
                "version": REPORT_PREFLIGHT_VERSION,
                "sources": sources,
                "artifacts": artifacts,
            },
        )
        atomic_write_text(success, core._utc_now() + "\n")
        _write_preflight(
            output,
            status="complete",
            contract_hash=contract_hash,
            sources=sources,
            reused=False,
            detail="report rebuilt and all artifact fingerprints committed",
        )
        return report
    except Exception as exc:
        _write_preflight(
            output,
            status="failed",
            contract_hash=contract_hash,
            sources=sources,
            reused=False,
            detail=repr(exc),
        )
        raise


def install() -> None:
    streaming.file_record = _fast_source_record
    core._finalize_volatility_report = _finalize_with_preflight


__all__ = ["REPORT_PREFLIGHT_VERSION", "install"]
