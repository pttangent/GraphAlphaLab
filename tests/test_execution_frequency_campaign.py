from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from graphalphalab.execution_frequency_campaign import (
    build_frequency_tasks,
)
from graphalphalab.governance import ResourceBudget


def test_build_frequency_tasks_uses_candidate_horizon_units(
    tmp_path: Path,
) -> None:
    signals = tmp_path / "signals"
    signals.mkdir()
    (signals / "_SUCCESS").write_text("{}\n", encoding="utf-8")
    (signals / "export_manifest.json").write_text(
        "{}\n",
        encoding="utf-8",
    )

    labels = tmp_path / "labels.parquet"
    pd.DataFrame(
        {
            "trade_date": ["2026-01-05"],
            "decision_time": [
                pd.Timestamp("2026-01-05T14:30:00Z")
            ],
            "symbol_id": ["A"],
            "label_id": ["forward_30m"],
            "target_return": [0.01],
            "entry_time": [
                pd.Timestamp("2026-01-05T14:31:00Z")
            ],
            "exit_time": [
                pd.Timestamp("2026-01-05T15:01:00Z")
            ],
            "label_available_time": [
                pd.Timestamp("2026-01-05T15:01:00Z")
            ],
        }
    ).to_parquet(labels, index=False)
    contract = tmp_path / "contract.json"
    contract.write_text(
        json.dumps(
            {
                "label_id": "forward_30m",
                "horizon_minutes": 30,
                "entry_lag_minutes": 1,
            }
        ),
        encoding="utf-8",
    )
    horizons = tmp_path / "horizons.json"
    horizons.write_text(
        json.dumps(
            {
                "horizons": [
                    {
                        "name": "30m",
                        "labels": str(labels),
                        "label_contract": str(contract),
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    candidate_rows = []
    for scope, family in (
        ("global", "shared_global"),
        ("inter_theme", "momentum_state"),
    ):
        candidate_rows.append(
            {
                "scope": scope,
                "theme_family": family,
                "factor_id": (
                    f"{scope}::{family}::"
                    "momentum_state_to_return::graph_forward"
                ),
                "layer_id": "momentum_state_to_return",
                "scale_minutes": 30,
                "variant_id": "graph_forward",
                "horizon": "30m",
                "horizon_minutes": 30,
            }
        )
    candidates = tmp_path / "candidates.json"
    candidates.write_text(
        json.dumps(
            {
                "version": "GAL_EXECUTION_CANDIDATES_V1",
                "candidate_count": 2,
                "candidates": candidate_rows,
            }
        ),
        encoding="utf-8",
    )

    tasks = build_frequency_tasks(
        signals_root=signals,
        horizon_manifest=horizons,
        candidate_manifest=candidates,
        output_root=tmp_path / "output",
        resource_budget=ResourceBudget(
            60.0,
            12,
            str(tmp_path / "spill"),
        ),
        workers=6,
        min_theme_size=5,
        min_theme_cross_section=5,
        min_direction_train_dates=20,
        direction_rolling_dates=60,
        control_columns=("own_score",),
        quantiles=5,
        carry_overnight=False,
    )
    assert len(tasks) == 2
    assert {task.candidate["scope"] for task in tasks} == {
        "global",
        "inter_theme",
    }
    assert all(
        task.worker_memory_limit_gb == 10.0 for task in tasks
    )
    assert all(task.worker_threads == 2 for task in tasks)
    assert all("horizon=30m" in task.unit for task in tasks)
