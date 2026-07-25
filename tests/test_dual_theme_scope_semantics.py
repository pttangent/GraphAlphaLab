from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from graphalphalab.contracts import LabelContract
from graphalphalab.dual_theme_common import (
    INDUCED_WITHIN_GFF_VERSION,
    P0Partition,
    load_gff_campaign_contract,
)
from graphalphalab.dual_theme_reporting import (
    _apply_financial_semantics,
    _cross_scope_comparison,
)
from graphalphalab.dual_theme_scope_alpha import (
    prepare_inter_theme_factor,
    prepare_within_theme_factor,
)
from graphalphalab.dual_theme_sql import _inter_query, _stock_query


def _decision() -> pd.Timestamp:
    return pd.Timestamp("2026-01-05 15:00:00", tz="UTC")


def test_core4_v2_contract_is_accepted_only_with_induced_within_semantics(
    tmp_path: Path,
) -> None:
    campaign = tmp_path / "campaign=c4"
    runs = campaign / "runs"
    runs.mkdir(parents=True)
    payload = {
        "registry": {
            "campaign_version": INDUCED_WITHIN_GFF_VERSION,
            "theme_family_order": ["momentum_state", "residual_return"],
            "consensus": {"enabled": False},
            "scope_contract_counts": {
                "global": 26,
                "momentum_state_within_theme": 20,
                "momentum_state_inter_theme": 20,
                "residual_return_within_theme": 20,
                "residual_return_inter_theme": 20,
            },
            "igc_scope_policy": {
                "global": "compute_once_and_share_between_theme_families",
                "within_theme": "induced_subgraph_from_global_final_edges",
                "inter_theme": "aggregate_cross-theme edges from shared global p0",
            },
            "within_theme_semantics": {
                "mode": "induced_global_final_edges",
                "source": "governed_global_p0_final_edges",
                "local_residualization": False,
                "local_candidate_generation": False,
                "local_lag_selection": False,
                "local_top_k_or_degree_cap": False,
                "edge_weight_policy": "preserve_global_edge_weight",
                "edge_identity_policy": "preserve_global_edge_key",
                "p1_policy": "rebuild_p1_from_the_induced_edge_graph",
            },
        },
        "campaign_contract": {"campaign_version": INDUCED_WITHIN_GFF_VERSION},
        "dates": ["2026-01-05"],
    }
    (runs / "campaign_contract.json").write_text(
        json.dumps(payload),
        encoding="utf-8",
    )
    parsed = load_gff_campaign_contract(campaign)
    assert parsed["campaign_version"] == INDUCED_WITHIN_GFF_VERSION

    payload["registry"]["within_theme_semantics"]["local_residualization"] = True
    (runs / "campaign_contract.json").write_text(
        json.dumps(payload),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="Unsupported induced Within-Theme semantics"):
        load_gff_campaign_contract(campaign)


def test_within_theme_alpha_neutralizes_score_and_return_inside_each_theme() -> None:
    decision = _decision()
    frame = pd.DataFrame(
        {
            "decision_time": [decision] * 6,
            "context_theme_id": ["A", "A", "A", "B", "B", "B"],
            "symbol_id": [1, 2, 3, 4, 5, 6],
            "score": [1.0, 2.0, 3.0, 10.0, 20.0, 30.0],
            "own_score": [0.0] * 6,
            "target_return": [0.01, 0.02, 0.03, -0.03, -0.02, -0.01],
        }
    )
    prepared = prepare_within_theme_factor(
        frame,
        score_column="score",
        control_columns=("own_score",),
        min_theme_size=3,
    )
    assert len(prepared) == 6
    grouped = prepared.groupby("context_theme_id", observed=True)
    assert grouped["scope_target_return"].mean().abs().max() < 1e-12
    assert grouped["scope_score"].mean().abs().max() < 1e-12
    assert set(prepared["scope_alpha_unit"]) == {"stock_within_theme_neutral"}


def test_inter_theme_alpha_uses_one_weighted_portfolio_return_per_theme() -> None:
    decision = _decision()
    frame = pd.DataFrame(
        {
            "batch_id": ["dual_theme_igc"] * 6,
            "factor_id": ["inter_theme::momentum_state::x_to_return::graph_forward"] * 6,
            "layer_id": ["x_to_return"] * 6,
            "scale_minutes": [30] * 6,
            "variant_id": ["graph_forward"] * 6,
            "trade_date": ["2026-01-05"] * 6,
            "decision_time": [decision] * 6,
            "context_theme_id": ["T1", "T1", "T2", "T2", "T3", "T3"],
            "symbol_id": [1, 2, 3, 4, 5, 6],
            "score": [1.0, 1.0, 2.0, 2.0, 3.0, 3.0],
            "own_score": [0.0] * 6,
            "target_return": [0.00, 0.04, 0.01, 0.03, -0.02, 0.02],
            "membership_weight": [1.0, 3.0, 1.0, 1.0, 3.0, 1.0],
            "signal_available_time": [decision] * 6,
        }
    )
    prepared = prepare_inter_theme_factor(
        frame,
        score_column="score",
        control_columns=("own_score",),
        min_theme_cross_section=3,
        min_theme_size=2,
    ).set_index("context_theme_id")
    assert set(prepared.index) == {"T1", "T2", "T3"}
    assert prepared.loc["T1", "target_return"] == pytest.approx(0.03)
    assert prepared.loc["T2", "target_return"] == pytest.approx(0.02)
    assert prepared.loc["T3", "target_return"] == pytest.approx(-0.01)
    assert prepared["broadcast_score_spread"].abs().max() == pytest.approx(0.0)
    assert set(prepared["scope_alpha_unit"]) == {
        "theme_portfolio_weighted_member_return"
    }


def test_inter_theme_rejects_arbitrary_stock_tie_breaking() -> None:
    decision = _decision()
    frame = pd.DataFrame(
        {
            "trade_date": ["2026-01-05"] * 4,
            "decision_time": [decision] * 4,
            "context_theme_id": ["T1", "T1", "T2", "T2"],
            "symbol_id": [1, 2, 3, 4],
            "score": [1.0, 1.1, 2.0, 2.0],
            "target_return": [0.01, 0.02, 0.03, 0.04],
            "membership_weight": [1.0] * 4,
        }
    )
    with pytest.raises(ValueError, match="arbitrary stock tie-breaking"):
        prepare_inter_theme_factor(
            frame,
            score_column="score",
            control_columns=(),
            min_theme_cross_section=2,
            min_theme_size=2,
        )


def test_financial_semantics_blocks_risk_layers_from_direct_alpha_promotion() -> None:
    contract = LabelContract(
        label_id="forward_return_30m",
        horizon_minutes=30,
        entry_lag_minutes=1,
    )
    frame = pd.DataFrame(
        {
            "layer_id": ["momentum_state_to_return", "burst_to_volatility"],
        }
    )
    result = _apply_financial_semantics(frame, contract)
    assert result.loc[0, "financial_role"] == "direct_return_alpha"
    assert bool(result.loc[0, "semantic_promotion_eligible"])
    assert result.loc[1, "financial_role"] == "risk_or_liquidity_regime_candidate"
    assert not bool(result.loc[1, "semantic_promotion_eligible"])


def test_cross_scope_comparison_expands_shared_global_for_each_theme_family() -> None:
    rows = []
    for scope, family, value in (
        ("global", "shared_global", 0.01),
        ("within_theme", "momentum_state", 0.02),
        ("inter_theme", "momentum_state", 0.03),
    ):
        rows.append(
            {
                "scope": scope,
                "theme_family": family,
                "layer_id": "momentum_state_to_return",
                "scale_minutes": 30,
                "variant_id": "graph_forward",
                "horizon": "30m",
                "horizon_minutes": 30,
                "financial_role": "direct_return_alpha",
                "mean_spearman_ic": value,
                "net_mean_5bps": value / 10,
                "cost_survives_5bps": True,
            }
        )
    result = _cross_scope_comparison(pd.DataFrame(rows))
    selected = result[result["theme_family"] == "momentum_state"]
    assert len(selected) == 1
    assert selected.iloc[0]["mean_spearman_ic__global"] == pytest.approx(0.01)
    assert selected.iloc[0]["mean_spearman_ic__within_theme"] == pytest.approx(0.02)
    assert selected.iloc[0]["mean_spearman_ic__inter_theme"] == pytest.approx(0.03)
    assert selected.iloc[0]["mean_spearman_ic__inter_minus_global"] == pytest.approx(0.02)


def test_scoped_sql_availability_waits_for_theme_membership_decision() -> None:
    decision_partition = P0Partition(
        scope="within_theme",
        theme_family="momentum_state",
        edges=Path("edges.parquet"),
        nodes=Path("nodes.parquet"),
        scope_memberships=Path("memberships.parquet"),
    )
    meta = {
        "trade_date": "2026-01-05",
        "layer_id": "momentum_state_to_return",
        "scale_minutes": 30,
    }
    within_sql = _stock_query(
        batch_id="dual_theme_igc",
        partition=decision_partition,
        meta=meta,
        variant="graph_forward",
    )
    assert "e.decision_time AS signal_available_time" in within_sql

    inter_partition = P0Partition(
        scope="inter_theme",
        theme_family="momentum_state",
        edges=Path("edges.parquet"),
        nodes=Path("nodes.parquet"),
        scope_memberships=Path("memberships.parquet"),
    )
    inter_sql = _inter_query(
        batch_id="dual_theme_igc",
        partition=inter_partition,
        meta=meta,
        variant="graph_forward",
    )
    assert "e.decision_time AS signal_available_time" in inter_sql
