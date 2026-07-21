from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from graphalphalab.alpha import evaluate_alpha
from graphalphalab.contracts import LabelContract, validate_label_frame, validate_signal_frame
from graphalphalab.gff_export import export_gff_signals
from graphalphalab.governance import ResourceBudget
from graphalphalab.labels import LabelSpec, build_forward_return_labels
from graphalphalab.streaming import evaluate_alpha_streaming


def test_negative_direction_is_consistent_and_oriented() -> None:
    rows = []
    rng = np.random.default_rng(3)
    symbols = np.arange(40)
    for date_index in range(20):
        day = pd.Timestamp("2026-06-01T13:30:00Z") + pd.Timedelta(days=date_index)
        for decision_index in range(5):
            decision = day + pd.Timedelta(minutes=5 * decision_index)
            score = rng.normal(size=len(symbols))
            target = -0.02 * score + rng.normal(scale=0.001, size=len(symbols))
            for symbol, s, y in zip(symbols, score, target):
                rows.append(
                    {
                        "batch_id": "implemented27",
                        "factor_id": "negative_factor",
                        "layer_id": "L1",
                        "scale_minutes": 15,
                        "variant_id": "graph_forward",
                        "trade_date": str(decision.date()),
                        "decision_time": decision,
                        "symbol_id": int(symbol),
                        "score": float(s),
                        "target_return": float(y),
                        "expected_direction": -1,
                    }
                )
    result = evaluate_alpha(
        pd.DataFrame(rows),
        min_cross_section=20,
        annualization_factor=19656,
        label_overlapping=False,
    )
    row = result.metrics.iloc[0]
    assert row["mean_spearman_ic"] < -0.9
    assert row["daily_ic_sign_consistency"] == 1.0
    assert bool(row["direction_consistent"])
    assert bool(row["direction_predeclared"])
    assert row["oriented_long_short_mean"] > 0
    assert row["research_status"] == "candidate"
    assert not result.daily_ic.empty
    assert not result.portfolio_returns.empty


def test_clock_time_labels_do_not_use_row_shift() -> None:
    prices = pd.DataFrame(
        {
            "symbol_id": [1, 1, 1],
            "timestamp": pd.to_datetime(
                ["2026-07-01T14:30:00Z", "2026-07-01T14:31:00Z", "2026-07-01T14:36:00Z"]
            ),
            "available_time": pd.to_datetime(
                ["2026-07-01T14:30:01Z", "2026-07-01T14:31:01Z", "2026-07-01T14:36:01Z"]
            ),
            "price": [100.0, 101.0, 106.0],
        }
    )
    decisions = pd.DataFrame(
        {"symbol_id": [1], "decision_time": pd.to_datetime(["2026-07-01T14:30:00Z"])}
    )
    labels = build_forward_return_labels(
        prices,
        [LabelSpec("fwd_5m_next_bar", 5, entry_lag_minutes=1, max_exit_delay_seconds=1)],
        decision_times=decisions,
    )
    assert len(labels) == 1
    row = labels.iloc[0]
    assert row["entry_time"] == pd.Timestamp("2026-07-01T14:31:00Z")
    assert row["exit_time"] == pd.Timestamp("2026-07-01T14:36:00Z")
    assert row["target_return"] == pytest.approx(106.0 / 101.0 - 1.0)


def _synthetic_gff_partition(root: Path) -> Path:
    part = root / "p0" / "date=2026-07-01" / "layer=L1" / "scale=15"
    part.mkdir(parents=True)
    decision = pd.Timestamp("2026-07-01T14:30:00Z")
    nodes = pd.DataFrame(
        {
            "trade_date": ["2026-07-01"] * 3,
            "decision_time": [decision] * 3,
            "layer_id": ["L1"] * 3,
            "scale_minutes": [15] * 3,
            "p0_snapshot_id": ["snap"] * 3,
            "symbol": ["A", "B", "C"],
            "symbol_id": [1, 2, 3],
            "security_entity_id": ["A", "B", "C"],
            "node_score": [1.0, 2.0, 4.0],
        }
    )
    edges = pd.DataFrame(
        {
            "trade_date": ["2026-07-01", "2026-07-01"],
            "decision_time": [decision, decision],
            "layer_id": ["L1", "L1"],
            "scale_minutes": [15, 15],
            "p0_snapshot_id": ["snap", "snap"],
            "src_symbol_id": [1, 2],
            "dst_symbol_id": [3, 3],
            "edge_weight": [1.0, 1.0],
            "edge_available_time": [decision, decision],
        }
    )
    nodes.to_parquet(part / "node_projection.parquet", index=False)
    edges.to_parquet(part / "edges.parquet", index=False)
    return part


def test_export_gff_signals_builds_real_graph_score(tmp_path: Path) -> None:
    _synthetic_gff_partition(tmp_path)
    output = tmp_path / "signals"
    summary = export_gff_signals(
        tmp_path,
        output,
        batch_id="implemented27",
        resource_budget=ResourceBudget(memory_limit_gb=1, threads=1, temp_directory=str(tmp_path / "duck")),
    )
    assert summary.edge_pit_violations == 0
    forward_path = next(output.rglob("variant_id=graph_forward/**/data.parquet"))
    forward = pd.read_parquet(forward_path)
    row = forward.loc[forward.symbol_id == 3].iloc[0]
    assert row.score == pytest.approx(1.5)
    assert row.own_score == pytest.approx(4.0)
    assert row.edge_count == 2
    manifest = json.loads((output / "export_manifest.json").read_text())
    assert manifest["node_baseline_is_network_alpha"] is False
    assert "graph_forward" in manifest["graph_aggregated_variants"]


def test_pit_contract_rejects_same_time_entry() -> None:
    decision = pd.Timestamp("2026-07-01T14:30:00Z")
    labels = pd.DataFrame(
        {
            "trade_date": ["2026-07-01"],
            "decision_time": [decision],
            "symbol_id": [1],
            "label_id": ["fwd5"],
            "entry_time": [decision],
            "exit_time": [decision + pd.Timedelta(minutes=5)],
            "label_available_time": [decision + pd.Timedelta(minutes=5)],
            "target_return": [0.01],
        }
    )
    contract = LabelContract("fwd5", 5, 0)
    audit = validate_label_frame(labels, contract, join_keys=["trade_date", "decision_time", "symbol_id"])
    assert not audit.passed
    assert audit.entry_not_after_decision == 1
    signals = pd.DataFrame({"decision_time": [decision], "signal_available_time": [decision + pd.Timedelta(seconds=1)]})
    signal_audit = validate_signal_frame(signals)
    assert signal_audit.signal_after_decision == 1


def test_streaming_strict_pit_and_label_contract(tmp_path: Path) -> None:
    signal_rows = []
    label_rows = []
    rng = np.random.default_rng(9)
    for d in range(3):
        date = pd.Timestamp("2026-07-01T14:30:00Z") + pd.Timedelta(days=d)
        for t in range(2):
            decision = date + pd.Timedelta(minutes=5 * t)
            for symbol in range(20):
                score = rng.normal()
                signal_rows.append(
                    {
                        "batch_id": "implemented27",
                        "factor_id": "L1__graph_forward",
                        "layer_id": "L1",
                        "scale_minutes": 15,
                        "variant_id": "graph_forward",
                        "trade_date": str(decision.date()),
                        "decision_time": decision,
                        "symbol_id": symbol,
                        "score": score,
                        "own_score": rng.normal(),
                        "signal_available_time": decision,
                        "expected_direction": 1,
                    }
                )
                entry = decision + pd.Timedelta(minutes=1)
                exit_time = entry + pd.Timedelta(minutes=5)
                label_rows.append(
                    {
                        "trade_date": str(decision.date()),
                        "decision_time": decision,
                        "symbol_id": symbol,
                        "label_id": "fwd5",
                        "entry_time": entry,
                        "exit_time": exit_time,
                        "label_available_time": exit_time,
                        "target_return": 0.01 * score + rng.normal(scale=0.001),
                    }
                )
    signals_path = tmp_path / "signals.parquet"
    labels_path = tmp_path / "labels.parquet"
    pd.DataFrame(signal_rows).to_parquet(signals_path, index=False)
    pd.DataFrame(label_rows).to_parquet(labels_path, index=False)
    result = evaluate_alpha_streaming(
        signals_path,
        labels_path,
        label_contract=LabelContract("fwd5", 5, 1, overlapping=True, rebalance_minutes=5),
        min_cross_section=10,
        resource_budget=ResourceBudget(memory_limit_gb=1, threads=1, temp_directory=str(tmp_path / "temp")),
        correlation_sample_modulus=1,
    )
    assert result.governance["pit_audit"]["passed"]
    assert result.metrics.iloc[0]["annualization_valid"] == False
    assert not result.score_correlation.empty or result.metrics.factor_id.nunique() == 1
