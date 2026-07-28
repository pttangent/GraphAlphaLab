from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd

from graphalphalab.daily_labels_v2 import build_daily_labels_v2
from graphalphalab.dual_theme import export_dual_theme_signals
from graphalphalab.governance import ResourceBudget
from graphalphalab.intraday_labels_v2 import IntradayLabelSpec, build_intraday_labels_v2


def _write_gff_partition(root: Path, *, trade_date: str) -> None:
    root.mkdir(parents=True, exist_ok=True)
    decision = pd.Timestamp(f"{trade_date}T15:00:00", tz="UTC")
    edges = pd.DataFrame(
        {
            "trade_date": [trade_date],
            "decision_time": [decision],
            "edge_available_time": [decision],
            "layer_id": ["momentum_state_to_return"],
            "scale_minutes": [30],
            "p0_snapshot_id": ["snap"],
            "src_symbol_id": [1],
            "dst_symbol_id": [2],
            "edge_weight": [0.5],
        }
    )
    nodes = pd.DataFrame(
        {
            "trade_date": [trade_date, trade_date],
            "decision_time": [decision, decision],
            "layer_id": ["momentum_state_to_return", "momentum_state_to_return"],
            "scale_minutes": [30, 30],
            "p0_snapshot_id": ["snap", "snap"],
            "symbol": ["AAA", "BBB"],
            "symbol_id": [1, 2],
            "security_entity_id": ["entity:1", "entity:2"],
            "node_score": [1.0, 2.0],
        }
    )
    edges.to_parquet(root / "edges.parquet", index=False)
    nodes.to_parquet(root / "node_projection.parquet", index=False)


def _make_campaign(tmp_path: Path, dates: list[str]) -> Path:
    campaign = tmp_path / "campaign=DUAL"
    (campaign / "runs").mkdir(parents=True, exist_ok=True)
    (campaign / "_SUCCESS").write_text("{}", encoding="utf-8")
    (campaign / "runs" / "campaign_contract.json").write_text(
        json.dumps(
            {
                "registry": {
                    "campaign_version": "SMI_DUAL_THEME_IGC_FULL_SCOPE_COMPARE_V1",
                    "theme_family_order": ["momentum_state", "residual_return"],
                    "consensus": {"enabled": False},
                    "scope_contract_counts": {
                        "global": 1,
                        "momentum_state_within_theme": 0,
                        "momentum_state_inter_theme": 0,
                        "residual_return_within_theme": 0,
                        "residual_return_inter_theme": 0,
                    },
                },
                "campaign_contract": {"igc_scope_contract_count": 1},
                "dates": dates,
            }
        ),
        encoding="utf-8",
    )
    for trade_date in dates:
        _write_gff_partition(
            campaign
            / "graphs"
            / "scope=global"
            / "batch=IGC_DUAL_THEME_COMPARE"
            / "p0"
            / f"date={trade_date}"
            / "layer=momentum_state_to_return"
            / "scale=30"
            / "variant=v",
            trade_date=trade_date,
        )
    return campaign


def _export_frames(output: Path) -> dict[str, pd.DataFrame]:
    frames = {}
    for path in sorted(output.rglob("data.parquet")):
        frame = pd.read_parquet(path)
        frames[str(path.relative_to(output))] = frame.sort_values(
            by=list(frame.columns)
        ).reset_index(drop=True)
    return frames


def test_parallel_export_matches_sequential_and_resumes(tmp_path: Path) -> None:
    dates = ["2026-07-06", "2026-07-07", "2026-07-08"]
    campaign = _make_campaign(tmp_path, dates)
    sequential = tmp_path / "signals_seq"
    parallel = tmp_path / "signals_par"
    budget = ResourceBudget(memory_limit_gb=4.0, threads=2, temp_directory=str(tmp_path / "tmp"))
    summary_seq = export_dual_theme_signals(
        campaign,
        sequential,
        scopes=("global",),
        resource_budget=budget,
    )
    summary_par = export_dual_theme_signals(
        campaign,
        parallel,
        scopes=("global",),
        resource_budget=budget,
        workers=2,
    )
    assert summary_seq.factor_count == summary_par.factor_count
    assert summary_seq.output_rows == summary_par.output_rows
    assert summary_seq.counts_by_scope_family == summary_par.counts_by_scope_family
    assert summary_seq.edge_pit_violations == summary_par.edge_pit_violations == 0
    seq_frames = _export_frames(sequential)
    par_frames = _export_frames(parallel)
    assert set(seq_frames) == set(par_frames)
    for name in seq_frames:
        pd.testing.assert_frame_equal(seq_frames[name], par_frames[name])

    mtimes = {path: path.stat().st_mtime_ns for path in parallel.rglob("data.parquet")}
    export_dual_theme_signals(
        campaign,
        parallel,
        scopes=("global",),
        resource_budget=budget,
        workers=2,
    )
    assert mtimes == {path: path.stat().st_mtime_ns for path in parallel.rglob("data.parquet")}


def _write_bars(bars_root: Path, trade_date: str, minutes: int, *, with_close_bar: bool = True) -> None:
    day = bars_root / f"date={trade_date}"
    day.mkdir(parents=True, exist_ok=True)
    rows = []
    for index in range(minutes):
        timestamp = pd.Timestamp(f"{trade_date}T13:30:00", tz="UTC") + pd.Timedelta(minutes=index)
        for symbol_id, base in ((1, 100.0), (2, 200.0)):
            rows.append(
                {
                    "trade_date": trade_date,
                    "symbol_id": symbol_id,
                    "timestamp": timestamp,
                    "available_time": timestamp + pd.Timedelta(minutes=1),
                    "open": base + index,
                    "close": base + index + 0.5,
                }
            )
    pd.DataFrame(rows).to_parquet(day / "part-00000.parquet", index=False)


def _write_label_inputs(tmp_path: Path, dates: list[str], bar_dates: list[str], *, intraday_minutes: int = 100) -> tuple[Path, Path, Path]:
    campaign = tmp_path / "campaign=DUAL"
    (campaign / "runs").mkdir(parents=True, exist_ok=True)
    (campaign / "_SUCCESS").write_text("{}", encoding="utf-8")
    (campaign / "runs" / "campaign_contract.json").write_text(
        json.dumps(
            {
                "registry": {
                    "campaign_version": "SMI_DUAL_THEME_IGC_FULL_SCOPE_COMPARE_V1",
                    "theme_family_order": ["momentum_state", "residual_return"],
                    "consensus": {"enabled": False},
                    "scope_contract_counts": {
                        "global": 0,
                        "momentum_state_within_theme": 0,
                        "momentum_state_inter_theme": 0,
                        "residual_return_within_theme": 0,
                        "residual_return_inter_theme": 0,
                    },
                },
                "campaign_contract": {"igc_scope_contract_count": 0},
                "dates": dates,
            }
        ),
        encoding="utf-8",
    )
    signals = tmp_path / "signals"
    signal_dir = signals / "batch_id=dual_theme_igc" / "scope=global" / "part"
    signal_dir.mkdir(parents=True, exist_ok=True)
    rows = []
    for trade_date in dates:
        decision = pd.Timestamp(f"{trade_date}T13:45:00", tz="UTC")
        rows.extend(
            [
                {"trade_date": trade_date, "decision_time": decision, "symbol_id": 1},
                {"trade_date": trade_date, "decision_time": decision, "symbol_id": 2},
            ]
        )
    pd.DataFrame(rows).to_parquet(signal_dir / "data.parquet", index=False)
    (signals / "export_manifest.json").write_text("{}", encoding="utf-8")
    (signals / "_SUCCESS").write_text("{}", encoding="utf-8")
    bars = tmp_path / "bars"
    for trade_date in bar_dates:
        _write_bars(bars, trade_date, intraday_minutes)
    return campaign, signals, bars


def _label_frames(output: Path) -> dict[str, pd.DataFrame]:
    frames = {}
    for path in sorted((output / "labels").rglob("data.parquet")):
        frame = pd.read_parquet(path)
        frames[path.parent.name] = frame.sort_values(
            by=["trade_date", "decision_time", "symbol_id", "label_id"]
        ).reset_index(drop=True)
    return frames


def test_parallel_intraday_labels_match_sequential_and_reuse(tmp_path: Path) -> None:
    dates = ["2026-07-06", "2026-07-07", "2026-07-08"]
    campaign, signals, bars = _write_label_inputs(tmp_path, dates, dates)
    specs = (IntradayLabelSpec("5m", 5), IntradayLabelSpec("15m", 15))
    seq = tmp_path / "intra_seq"
    par = tmp_path / "intra_par"
    build_intraday_labels_v2(
        gff_campaign_root=campaign,
        signals_root=signals,
        bars_root=bars,
        output_root=seq,
        start_date=dates[0],
        end_date=dates[-1],
        specs=specs,
        threads=2,
        memory_limit_gb=4.0,
    )
    build_intraday_labels_v2(
        gff_campaign_root=campaign,
        signals_root=signals,
        bars_root=bars,
        output_root=par,
        start_date=dates[0],
        end_date=dates[-1],
        specs=specs,
        threads=2,
        memory_limit_gb=4.0,
        workers=2,
    )
    seq_frames = _label_frames(seq)
    par_frames = _label_frames(par)
    assert set(seq_frames) == set(par_frames)
    for name in seq_frames:
        pd.testing.assert_frame_equal(seq_frames[name], par_frames[name])

    build_intraday_labels_v2(
        gff_campaign_root=campaign,
        signals_root=signals,
        bars_root=bars,
        output_root=par,
        start_date=dates[0],
        end_date=dates[-1],
        specs=specs,
        threads=2,
        memory_limit_gb=4.0,
        workers=2,
    )
    progress = json.loads((par / "diagnostics" / "progress.json").read_text(encoding="utf-8"))
    assert progress["reused_units"] == len(dates)


def test_parallel_daily_labels_match_sequential_and_reuse(tmp_path: Path) -> None:
    dates = ["2026-07-06", "2026-07-07"]
    bar_dates = ["2026-07-06", "2026-07-07", "2026-07-08", "2026-07-09"]
    campaign, signals, bars = _write_label_inputs(tmp_path, dates, bar_dates, intraday_minutes=5)
    seq = tmp_path / "daily_seq"
    par = tmp_path / "daily_par"
    kwargs = dict(
        gff_campaign_root=campaign,
        signals_root=signals,
        bars_root=bars,
        start_date=dates[0],
        end_date=dates[-1],
        profile="core",
        threads=2,
        memory_limit_gb=4.0,
    )
    build_daily_labels_v2(output_root=seq, **kwargs)
    build_daily_labels_v2(output_root=par, workers=2, **kwargs)
    seq_frames = _label_frames(seq)
    par_frames = _label_frames(par)
    assert set(seq_frames) == set(par_frames)
    for name in seq_frames:
        pd.testing.assert_frame_equal(seq_frames[name], par_frames[name])

    build_daily_labels_v2(output_root=par, workers=2, **kwargs)
    progress = json.loads((par / "diagnostics" / "progress.json").read_text(encoding="utf-8"))
    assert progress["reused_units"] == len(dates)
