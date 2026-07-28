from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from graphalphalab.alpha import evaluate_alpha
from graphalphalab.contracts import LabelContract
from graphalphalab.dual_theme_global_dag import FactorTask, _run_factor_task
from graphalphalab.intraday_labels_v2 import IntradayLabelSpec, build_intraday_labels_v2
from tests.test_parallel_workers import _write_label_inputs


def _factor_task_fixture(tmp_path: Path) -> tuple[FactorTask, pd.DataFrame]:
    rng = np.random.default_rng(11)
    dates = ["2026-07-06", "2026-07-07", "2026-07-08"]
    decisions = [pd.Timestamp(f"{d}T13:45:00", tz="UTC") + pd.Timedelta(minutes=5 * i) for d in dates for i in range(3)]
    symbols = [f"S{i:03d}" for i in range(30)]
    signal_rows = []
    label_rows = []
    for decision in decisions:
        trade_date = str(decision.date())
        scores = rng.normal(size=len(symbols))
        returns = 0.02 * scores + rng.normal(scale=0.005, size=len(symbols))
        for symbol, score, target in zip(symbols, scores, returns):
            signal_rows.append(
                {
                    "batch_id": "dual_theme_igc",
                    "factor_id": "global::shared_global::L1::graph_forward",
                    "layer_id": "L1",
                    "scale_minutes": 15,
                    "variant_id": "graph_forward",
                    "trade_date": trade_date,
                    "decision_time": decision,
                    "symbol_id": symbol,
                    "score": score,
                    "own_score": score * 0.5,
                    "signal_available_time": decision,
                }
            )
            label_rows.append(
                {
                    "trade_date": trade_date,
                    "decision_time": decision,
                    "symbol_id": symbol,
                    "label_id": "forward_return_5m",
                    "target_return": target,
                    "entry_time": decision + pd.Timedelta(minutes=1),
                    "exit_time": decision + pd.Timedelta(minutes=6),
                    "label_available_time": decision + pd.Timedelta(minutes=7),
                }
            )
    signals = tmp_path / "signals" / "part"
    signals.mkdir(parents=True)
    pd.DataFrame(signal_rows).to_parquet(signals / "data.parquet", index=False)
    labels = tmp_path / "labels" / "part"
    labels.mkdir(parents=True)
    label_frame = pd.DataFrame(label_rows)
    label_frame.to_parquet(labels / "data.parquet", index=False)
    contract_path = tmp_path / "contract.json"
    contract_path.write_text(
        json.dumps(LabelContract(label_id="forward_return_5m", horizon_minutes=5, entry_lag_minutes=1).as_dict()),
        encoding="utf-8",
    )
    signal_frame = pd.DataFrame(signal_rows)
    reference = signal_frame.merge(
        label_frame,
        on=["trade_date", "decision_time", "symbol_id"],
        how="inner",
    )
    identity = {
        "batch_id": "dual_theme_igc",
        "factor_id": "global::shared_global::L1::graph_forward",
        "layer_id": "L1",
        "scale_minutes": 15,
        "variant_id": "graph_forward",
    }
    task = FactorTask(
        horizon="5m",
        horizon_minutes=5,
        scope="global",
        ordinal=1,
        signal_path=str(tmp_path / "signals"),
        labels_path=str(tmp_path / "labels"),
        label_contract_path=str(contract_path),
        checkpoint_root=str(tmp_path / "ckpt"),
        checkpoint_contract_hash="test",
        factor_keys=("batch_id", "factor_id", "layer_id", "scale_minutes", "variant_id"),
        factor_identity=identity,
        pit_audit={"passed": True},
        signal_columns=tuple(signal_frame.columns),
        join_keys=("trade_date", "decision_time", "symbol_id"),
        metadata_path=None,
        metadata_id="symbol_id",
        metadata_signal_id="symbol_id",
        slice_columns=(),
        score_column="score",
        symbol_column="symbol_id",
        quantiles=5,
        min_cross_section=10,
        min_theme_size=2,
        min_theme_cross_section=2,
        direction_column="expected_direction",
        default_direction="auto",
        control_columns=("own_score",),
        annualization_factor=None,
        allow_legacy_signals=False,
        worker_memory_limit_gb=4.0,
        worker_threads=1,
        temp_directory=None,
    )
    return task, reference


def test_factor_task_diet_matches_full_projection(tmp_path: Path) -> None:
    task, reference = _factor_task_fixture(tmp_path)
    result = _run_factor_task(task)
    assert result["status"] == "complete"

    ckpt_metrics = pd.read_parquet(Path(result["checkpoint"]) / "metrics.parquet")
    expected = evaluate_alpha(
        reference,
        score_column="score",
        label_column="target_return",
        min_cross_section=10,
        quantiles=5,
        control_columns=("own_score",),
        direction_column="expected_direction",
        default_direction="auto",
        label_overlapping=True,
    )
    assert len(ckpt_metrics) == len(expected.metrics) == 1
    got = ckpt_metrics.iloc[0]
    want = expected.metrics.iloc[0]
    for column in ("factor_id", "layer_id", "variant_id", "batch_id"):
        assert str(got[column]) == str(want[column])
    for column in ("mean_spearman_ic", "oriented_long_short_mean", "mean_turnover", "observations", "decision_count"):
        assert float(got[column]) == float(want[column])

    again = _run_factor_task(task)
    assert again["status"] == "reused"


def test_label_checkpoints_survive_export_budget_change(tmp_path: Path) -> None:
    dates = ["2026-07-06", "2026-07-07", "2026-07-08"]
    campaign, signals, bars = _write_label_inputs(tmp_path, dates, dates)
    specs = (IntradayLabelSpec("5m", 5),)
    output = tmp_path / "intra"
    kwargs = dict(
        gff_campaign_root=campaign,
        signals_root=signals,
        bars_root=bars,
        output_root=output,
        start_date=dates[0],
        end_date=dates[-1],
        specs=specs,
        threads=2,
        memory_limit_gb=4.0,
    )
    build_intraday_labels_v2(**kwargs)
    manifest = signals / "export_manifest.json"
    manifest.write_text(json.dumps({"resource_budget": {"memory_limit_gb": 64, "threads": 12}}), encoding="utf-8")
    build_intraday_labels_v2(**kwargs)
    progress = json.loads((output / "diagnostics" / "progress.json").read_text(encoding="utf-8"))
    assert progress["reused_units"] == len(dates)
    manifest.write_text(json.dumps({"resource_budget": {"memory_limit_gb": 120, "threads": 24}}), encoding="utf-8")
    build_intraday_labels_v2(**kwargs)
    progress = json.loads((output / "diagnostics" / "progress.json").read_text(encoding="utf-8"))
    assert progress["reused_units"] == len(dates)
