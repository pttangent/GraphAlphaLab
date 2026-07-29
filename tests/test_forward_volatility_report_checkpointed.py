from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from graphalphalab.forward_volatility import ForwardVolatilityConfig
import graphalphalab.forward_volatility_report_checkpointed as checkpointed


def _sources(output: Path) -> Path:
    raw = output / "raw_volatility_diagnostic"
    horizon = raw / "horizon=forward_log_rv_30m"
    horizon.mkdir(parents=True)
    for filename in (
        "_SUCCESS",
        "alpha_metrics.csv",
        "daily_ic.csv",
        "quantile_returns.csv",
        "stability_slices.csv",
        "ic_series.csv",
        "summary.json",
        "run_manifest.json",
        "horizon_checkpoint.json",
    ):
        (horizon / filename).write_text("x\n", encoding="utf-8")
    label_root = output / "labels" / "trade_date=2026-07-01"
    label_root.mkdir(parents=True)
    pd.DataFrame(
        {
            "trade_date": ["2026-07-01"],
            "decision_time": [pd.Timestamp("2026-07-01T15:00:00Z")],
            "symbol_id": [1],
            "horizon_minutes": [30],
            "forward_log_rv": [-8.0],
        }
    ).to_parquet(label_root / "data.parquet", index=False)
    (label_root / "checkpoint.json").write_text(
        json.dumps({"status": "complete", "files": []}) + "\n",
        encoding="utf-8",
    )
    return raw


def test_report_preflight_returns_before_rebuilding(tmp_path: Path, monkeypatch) -> None:
    output = tmp_path / "output"
    raw = _sources(output)
    config = ForwardVolatilityConfig(horizons=(30,), targets=("forward_log_rv",))
    calls = {"count": 0}

    def fake_finalizer(output_root, raw_root, supplied_config):
        calls["count"] += 1
        report = Path(output_root) / "prediction_report"
        report.mkdir(parents=True, exist_ok=True)
        (report / "REPORT.md").write_text("complete\n", encoding="utf-8")
        (report / "_SUCCESS").write_text("temporary\n", encoding="utf-8")
        return report

    monkeypatch.setattr(checkpointed, "_ORIGINAL_FINALIZER", fake_finalizer)

    first = checkpointed._finalize_with_preflight(output, raw, config)
    second = checkpointed._finalize_with_preflight(output, raw, config)

    assert first == second
    assert calls["count"] == 1
    assert (first / "_SUCCESS").exists()
    marker = json.loads((first / "checkpoint.json").read_text(encoding="utf-8"))
    assert marker["status"] == "complete"
    assert marker["version"] == checkpointed.REPORT_PREFLIGHT_VERSION
    preflight = json.loads(
        (output / "_checkpoints" / "forward_volatility" / "report_preflight.json").read_text(
            encoding="utf-8"
        )
    )
    assert preflight["reused"] is True
    assert "before reading raw evidence" in preflight["detail"]


def test_large_source_uses_fast_stat_anchor(tmp_path: Path) -> None:
    path = tmp_path / "large.csv"
    with path.open("wb") as handle:
        handle.seek(9 * 1024 * 1024)
        handle.write(b"x")

    record = checkpointed._fast_source_record(path)

    assert record["fingerprint_mode"] == "size_mtime_anchor"
    assert int(record["size_bytes"]) > 8 * 1024 * 1024
    assert len(str(record["sha256"])) == 64
