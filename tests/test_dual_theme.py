from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from graphalphalab.dual_theme import (
    _matched_variant_comparison,
    discover_dual_theme_partitions,
    export_dual_theme_signals,
    factor_id,
    load_horizon_manifest,
    parse_factor_id,
)


def _write_partition(
    root: Path,
    *,
    trade_date: str,
    layer: str,
    scale: int,
    theme_nodes: bool,
) -> None:
    root.mkdir(parents=True, exist_ok=True)
    decision = pd.Timestamp("2026-01-02 15:00:00", tz="UTC")
    if theme_nodes:
        symbols = ["THEME::T1", "THEME::T2"]
        ids = [101, 102]
    else:
        symbols = ["AAA", "BBB"]
        ids = [1, 2]
    edges = pd.DataFrame(
        {
            "trade_date": [trade_date],
            "decision_time": [decision],
            "edge_available_time": [decision],
            "layer_id": [layer],
            "scale_minutes": [scale],
            "p0_snapshot_id": ["snap"],
            "src_symbol_id": [ids[0]],
            "dst_symbol_id": [ids[1]],
            "edge_weight": [0.5],
        }
    )
    nodes = pd.DataFrame(
        {
            "trade_date": [trade_date, trade_date],
            "decision_time": [decision, decision],
            "layer_id": [layer, layer],
            "scale_minutes": [scale, scale],
            "p0_snapshot_id": ["snap", "snap"],
            "symbol": symbols,
            "symbol_id": ids,
            "security_entity_id": [f"entity:{value}" for value in ids],
            "node_score": [1.0, 2.0],
        }
    )
    edges.to_parquet(root / "edges.parquet", index=False)
    nodes.to_parquet(root / "node_projection.parquet", index=False)


def test_factor_identity_roundtrip() -> None:
    value = factor_id(
        "within_theme",
        "momentum_state",
        "momentum_state_to_return",
        "graph_forward",
    )
    assert parse_factor_id(value) == {
        "scope": "within_theme",
        "theme_family": "momentum_state",
        "factor_layer": "momentum_state_to_return",
        "factor_variant": "graph_forward",
    }


def test_horizon_manifest_uses_label_contract(tmp_path: Path) -> None:
    labels = tmp_path / "labels.parquet"
    pd.DataFrame({"x": [1]}).to_parquet(labels, index=False)
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
    manifest = tmp_path / "horizons.json"
    manifest.write_text(
        json.dumps(
            {"horizons": [{"labels": str(labels), "label_contract": str(contract)}]}
        ),
        encoding="utf-8",
    )
    rows = load_horizon_manifest(manifest)
    assert len(rows) == 1
    assert rows[0].name == "30m"


def test_discovery_and_inter_theme_projection(tmp_path: Path) -> None:
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
                        "momentum_state_inter_theme": 1,
                        "residual_return_within_theme": 0,
                        "residual_return_inter_theme": 0,
                    },
                },
                "campaign_contract": {"igc_scope_contract_count": 2},
                "dates": ["2026-01-02"],
            }
        ),
        encoding="utf-8",
    )
    global_p0 = (
        campaign
        / "graphs"
        / "scope=global"
        / "batch=IGC_DUAL_THEME_COMPARE"
        / "p0"
        / "date=2026-01-02"
        / "layer=momentum_state_to_return"
        / "scale=30"
        / "variant=v"
    )
    inter_p0 = (
        campaign
        / "graphs"
        / "scope=inter_theme"
        / "theme_family=momentum_state"
        / "p0"
        / "date=2026-01-02"
        / "layer=momentum_state_to_return"
        / "scale=30"
        / "variant=v"
    )
    _write_partition(
        global_p0,
        trade_date="2026-01-02",
        layer="momentum_state_to_return",
        scale=30,
        theme_nodes=False,
    )
    _write_partition(
        inter_p0,
        trade_date="2026-01-02",
        layer="momentum_state_to_return",
        scale=30,
        theme_nodes=True,
    )
    memberships = (
        campaign
        / "graphs"
        / "scope_index"
        / "theme_family=momentum_state"
        / "date_scope_index"
        / "date=2026-01-02"
    )
    memberships.mkdir(parents=True, exist_ok=True)
    decision = pd.Timestamp("2026-01-02 15:00:00", tz="UTC")
    pd.DataFrame(
        {
            "trade_date": ["2026-01-02", "2026-01-02"],
            "decision_time": [decision, decision],
            "theme_id": ["T1", "T1"],
            "symbol": ["AAA", "AAB"],
            "symbol_id": [1, 3],
            "security_entity_id": ["entity:1", "entity:3"],
            "membership_weight": [1.0, 0.8],
        }
    ).to_parquet(memberships / "memberships.parquet", index=False)

    discovered = discover_dual_theme_partitions(
        campaign,
        theme_families=("momentum_state",),
        scopes=("global", "inter_theme"),
    )
    assert {(row.scope, row.theme_family) for row in discovered} == {
        ("global", "shared_global"),
        ("inter_theme", "momentum_state"),
    }

    output = tmp_path / "signals"
    summary = export_dual_theme_signals(
        campaign,
        output,
        theme_families=("momentum_state",),
        scopes=("global", "inter_theme"),
        variants=("node_baseline",),
    )
    assert summary.factor_count == 2
    inter_file = next(
        (output / "batch_id=dual_theme_igc" / "scope=inter_theme").rglob(
            "data.parquet"
        )
    )
    projected = pd.read_parquet(inter_file)
    assert set(projected["symbol_id"]) == {1, 3}
    assert set(projected["context_theme_id"]) == {"T1"}


def test_matched_comparison_is_scope_family_horizon_specific() -> None:
    rows = []
    for variant, ic, net in (
        ("node_baseline", 0.01, -0.001),
        ("graph_forward", 0.03, 0.002),
        ("graph_reverse_placebo", 0.005, -0.002),
    ):
        rows.append(
            {
                "scope": "within_theme",
                "theme_family": "momentum_state",
                "layer_id": "momentum_state_to_return",
                "scale_minutes": 30,
                "horizon": "30m",
                "horizon_minutes": 30,
                "variant_id": variant,
                "mean_spearman_ic": ic,
                "net_mean_5bps": net,
            }
        )
    result = _matched_variant_comparison(pd.DataFrame(rows))
    assert len(result) == 1
    assert result.loc[0, "abs_ic_increment_vs_node"] == pytest.approx(0.02)
    assert result.loc[0, "net_5bps_increment_vs_reverse_placebo"] == pytest.approx(
        0.004
    )
