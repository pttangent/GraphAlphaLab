from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from graphalphalab.dual_theme import _guard_existing_export
from graphalphalab.dual_theme_common import load_gff_campaign_contract
from graphalphalab.dual_theme_cross_scope import cross_scope_comparison
from graphalphalab.dual_theme_scope_alpha import (
    _prepare_inter_factor,
    _prepare_within_factor,
)


def _base_rows() -> dict[str, list[object]]:
    decision = pd.Timestamp("2026-01-02 15:00:00", tz="UTC")
    return {
        "batch_id": ["dual_theme_igc"] * 6,
        "factor_id": ["within_theme::momentum_state::layer::graph_forward"] * 6,
        "layer_id": ["layer"] * 6,
        "scale_minutes": [30] * 6,
        "variant_id": ["graph_forward"] * 6,
        "trade_date": ["2026-01-02"] * 6,
        "decision_time": [decision] * 6,
        "symbol_id": [1, 2, 3, 4, 5, 6],
        "score": [1.0, 2.0, 3.0, 10.0, 20.0, 30.0],
        "own_score": [0.2, 0.1, 0.3, 1.0, 0.5, 1.5],
        "target_return": [0.01, 0.02, 0.04, -0.02, 0.01, 0.03],
        "context_theme_id": ["T1", "T1", "T1", "T2", "T2", "T2"],
        "membership_weight": [1.0] * 6,
    }


def test_within_theme_alpha_is_neutral_inside_each_theme() -> None:
    prepared = _prepare_within_factor(
        pd.DataFrame(_base_rows()),
        score_column="score",
        control_columns=("own_score",),
        min_theme_size=3,
    )
    assert set(prepared["context_theme_id"]) == {"T1", "T2"}
    grouped = prepared.groupby("context_theme_id", observed=True)
    assert grouped["scope_target_return"].mean().abs().max() < 1e-12
    assert grouped["scope_score"].mean().abs().max() < 1e-12
    assert set(prepared["scope_alpha_unit"]) == {"stock_within_theme_neutral"}


def test_inter_theme_alpha_uses_one_weighted_portfolio_return_per_theme() -> None:
    rows = _base_rows()
    rows["factor_id"] = [
        "inter_theme::momentum_state::layer::graph_forward"
    ] * 6
    rows["score"] = [0.5, 0.5, 0.5, -0.25, -0.25, -0.25]
    rows["membership_weight"] = [0.6, 0.3, 0.1, 0.2, 0.3, 0.5]
    prepared = _prepare_inter_factor(
        pd.DataFrame(rows),
        score_column="score",
        control_columns=("own_score",),
        min_theme_cross_section=2,
        min_theme_size=3,
    )
    assert len(prepared) == 2
    assert set(prepared["symbol_id"]) == {"T1", "T2"}
    assert prepared["broadcast_score_spread"].max() == pytest.approx(0.0)
    expected_t1 = 0.01 * 0.6 + 0.02 * 0.3 + 0.04 * 0.1
    expected_t2 = -0.02 * 0.2 + 0.01 * 0.3 + 0.03 * 0.5
    values = prepared.set_index("context_theme_id")["scope_target_return"]
    assert values["T1"] == pytest.approx(expected_t1)
    assert values["T2"] == pytest.approx(expected_t2)
    assert set(prepared["scope_alpha_unit"]) == {
        "theme_portfolio_weighted_member_return"
    }


def test_inter_theme_rejects_conflicting_broadcast_scores() -> None:
    rows = _base_rows()
    rows["factor_id"] = [
        "inter_theme::momentum_state::layer::graph_forward"
    ] * 6
    rows["score"] = [0.5, 0.5, 0.6, -0.25, -0.25, -0.25]
    with pytest.raises(ValueError, match="non-identical stock scores"):
        _prepare_inter_factor(
            pd.DataFrame(rows),
            score_column="score",
            control_columns=(),
            min_theme_cross_section=2,
            min_theme_size=3,
        )


def test_cross_scope_matches_shared_global_to_each_theme_family() -> None:
    metrics = pd.DataFrame(
        [
            {
                "scope": "global",
                "theme_family": "shared_global",
                "layer_id": "layer",
                "scale_minutes": 30,
                "variant_id": "graph_forward",
                "horizon": "30m",
                "horizon_minutes": 30,
                "mean_spearman_ic": 0.01,
                "net_mean_5bps": 0.001,
            },
            {
                "scope": "within_theme",
                "theme_family": "momentum_state",
                "scope_alpha_unit": "stock_within_theme_neutral",
                "factor_id": "within_theme::momentum_state::layer::graph_forward",
                "layer_id": "layer",
                "scale_minutes": 30,
                "variant_id": "graph_forward",
                "horizon": "30m",
                "horizon_minutes": 30,
                "mean_spearman_ic": 0.03,
                "net_mean_5bps": 0.004,
            },
            {
                "scope": "inter_theme",
                "theme_family": "residual_return",
                "scope_alpha_unit": "theme_portfolio_weighted_member_return",
                "factor_id": "inter_theme::residual_return::layer::graph_forward",
                "layer_id": "layer",
                "scale_minutes": 30,
                "variant_id": "graph_forward",
                "horizon": "30m",
                "horizon_minutes": 30,
                "mean_spearman_ic": -0.02,
                "net_mean_5bps": -0.001,
            },
        ]
    )
    result = cross_scope_comparison(metrics)
    assert len(result) == 2
    momentum = result[result["theme_family"] == "momentum_state"].iloc[0]
    residual = result[result["theme_family"] == "residual_return"].iloc[0]
    assert momentum["mean_spearman_ic_increment_vs_global"] == pytest.approx(0.02)
    assert momentum["net_5bps_increment_vs_global"] == pytest.approx(0.003)
    assert residual["abs_ic_increment_vs_global"] == pytest.approx(0.01)


def test_existing_export_guard_rejects_legacy_semantics(tmp_path: Path) -> None:
    output = tmp_path / "signals"
    output.mkdir()
    (output / "export_manifest.json").write_text(
        json.dumps(
            {
                "export_version": "GAL_DUAL_THEME_GFF_EXPORT_V1",
                "parameters": {},
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="Existing signal export uses"):
        _guard_existing_export(
            output,
            campaign_root=tmp_path / "campaign",
            batch_id="dual_theme_igc",
            theme_families=("momentum_state", "residual_return"),
            scopes=("global", "within_theme", "inter_theme"),
            variants=("node_baseline", "graph_forward", "graph_reverse_placebo"),
            force=False,
        )


def test_core4_v2_induced_within_contract_is_accepted(tmp_path: Path) -> None:
    campaign = tmp_path / "campaign"
    runs = campaign / "runs"
    runs.mkdir(parents=True)
    payload = {
        "registry": {
            "campaign_version": (
                "SMI_DUAL_THEME_IGC_FULL_SCOPE_COMPARE_V2_INDUCED_WITHIN"
            ),
            "theme_family_order": ["momentum_state", "residual_return"],
            "consensus": {"enabled": False},
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
            "igc_scope_policy": {
                "global": "compute_once_and_share_between_theme_families",
                "within_theme": "induced_subgraph_from_global_final_edges",
                "inter_theme": "aggregate_cross-theme edges from shared global p0",
            },
            "scope_contract_counts": {
                "global": 26,
                "momentum_state_within_theme": 20,
                "momentum_state_inter_theme": 20,
                "residual_return_within_theme": 20,
                "residual_return_inter_theme": 20,
            },
        },
        "campaign_contract": {
            "campaign_version": (
                "SMI_DUAL_THEME_IGC_FULL_SCOPE_COMPARE_V2_INDUCED_WITHIN"
            )
        },
        "dates": ["2026-01-05"],
    }
    (runs / "campaign_contract.json").write_text(
        json.dumps(payload),
        encoding="utf-8",
    )
    parsed = load_gff_campaign_contract(campaign)
    assert parsed["campaign_version"].endswith("V2_INDUCED_WITHIN")
