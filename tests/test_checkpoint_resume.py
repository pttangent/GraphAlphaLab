from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from graphalphalab.checkpoint import (
    CheckpointSpec,
    checkpoint_valid,
    commit_frames,
    load_checkpoint_frames,
    write_progress,
)


def test_checkpoint_reuse_requires_matching_contract_source_and_file_hash(tmp_path: Path) -> None:
    root = tmp_path / "factor"
    spec = CheckpointSpec("alpha-factor", "factor=a", "contract-a", "source-a")
    frame = pd.DataFrame({"factor_id": ["a"], "value": [1.25]})

    commit_frames(root, spec, {"metrics.parquet": frame}, metadata={"ordinal": 1})

    assert checkpoint_valid(root, spec, required_files=("metrics.parquet",))
    assert not checkpoint_valid(
        root,
        CheckpointSpec("alpha-factor", "factor=a", "contract-b", "source-a"),
        required_files=("metrics.parquet",),
    )
    assert not checkpoint_valid(
        root,
        CheckpointSpec("alpha-factor", "factor=a", "contract-a", "source-b"),
        required_files=("metrics.parquet",),
    )

    loaded = load_checkpoint_frames(root, ("metrics.parquet",))["metrics.parquet"]
    pd.testing.assert_frame_equal(loaded, frame)

    pd.DataFrame({"factor_id": ["a"], "value": [9.99]}).to_parquet(
        root / "metrics.parquet", index=False
    )
    assert not checkpoint_valid(root, spec, required_files=("metrics.parquet",))


def test_checkpoint_rejects_incorrect_recorded_parquet_row_count(tmp_path: Path) -> None:
    root = tmp_path / "factor"
    spec = CheckpointSpec("alpha-factor", "factor=a", "contract-a", "source-a")
    commit_frames(
        root,
        spec,
        {"metrics.parquet": pd.DataFrame({"value": [1, 2, 3]})},
    )

    marker = root / "checkpoint.json"
    payload = json.loads(marker.read_text(encoding="utf-8"))
    payload["files"][0]["rows"] = 2
    marker.write_text(json.dumps(payload), encoding="utf-8")

    assert not checkpoint_valid(root, spec, required_files=("metrics.parquet",))


def test_progress_writes_machine_and_human_readable_dashboards(tmp_path: Path) -> None:
    payload = write_progress(
        tmp_path,
        stage="alpha-factor-checkpoints",
        total=4,
        completed=2,
        reused=1,
        current="factor=b",
        units=[
            {"unit": "factor=a", "status": "reused", "attempt": 0, "detail": "ok"},
            {"unit": "factor=b", "status": "running", "attempt": 1, "detail": "work"},
        ],
    )

    assert payload["progress_pct"] == 50.0
    progress = json.loads((tmp_path / "progress.json").read_text(encoding="utf-8"))
    assert progress["completed_units"] == 2
    assert progress["reused_units"] == 1
    dashboard = (tmp_path / "DASHBOARD.md").read_text(encoding="utf-8")
    assert "2/4 (50.0%)" in dashboard
    assert "factor=b" in dashboard


def test_atomic_checkpoint_replaces_stale_partial_directory(tmp_path: Path) -> None:
    root = tmp_path / "date=2026-07-01"
    root.mkdir(parents=True)
    (root / "stale.txt").write_text("stale", encoding="utf-8")
    spec = CheckpointSpec("p1-date", "2026-07-01", "contract", "source")

    commit_frames(root, spec, {"summary.parquet": pd.DataFrame({"rows": [3]})})

    assert not (root / "stale.txt").exists()
    assert checkpoint_valid(root, spec, required_files=("summary.parquet",))
