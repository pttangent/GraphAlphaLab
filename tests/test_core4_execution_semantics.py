from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from graphalphalab.core4_execution_semantics import (
    CORE4_INDUCED_GLOBAL_CAMPAIGN_VERSION,
    annotate_scope_frame,
    load_core4_campaign_contract,
    semantics_for_scope,
)


def _write_contract(root: Path, *, local_residualization: bool = False) -> None:
    (root / "runs").mkdir(parents=True)
    (root / "_SUCCESS").write_text("{}", encoding="utf-8")
    payload = {
        "dates": ["2026-01-05", "2026-01-06"],
        "campaign_contract": {
            "campaign_version": CORE4_INDUCED_GLOBAL_CAMPAIGN_VERSION,
        },
        "registry": {
            "campaign_version": CORE4_INDUCED_GLOBAL_CAMPAIGN_VERSION,
            "within_theme_semantics": {
                "mode": "induced_global_final_edges",
                "source": "governed_global_p0_final_edges",
                "local_residualization": local_residualization,
                "local_candidate_generation": False,
                "local_lag_selection": False,
                "local_top_k_or_degree_cap": False,
                "edge_weight_policy": "preserve_global_edge_weight",
                "edge_identity_policy": "preserve_global_edge_key",
                "p1_policy": "rebuild_p1_from_the_induced_edge_graph",
            },
            "scope_contract_counts": {
                "global": 26,
                "momentum_state_within_theme": 20,
                "momentum_state_inter_theme": 20,
                "residual_return_within_theme": 20,
                "residual_return_inter_theme": 20,
            },
        },
    }
    (root / "runs" / "campaign_contract.json").write_text(
        json.dumps(payload), encoding="utf-8"
    )


def test_within_theme_is_induced_global_not_local_graph(tmp_path: Path) -> None:
    _write_contract(tmp_path)
    audit = load_core4_campaign_contract(tmp_path)
    within = audit["scope_semantics"]["within_theme"]
    assert within["graph_estimation_scope"] == "global_market"
    assert within["ranking_scope"] == "within_context_theme_stock_cross_section"
    assert within["canonical_name"] == "induced_global_graph_local_rank"
    assert within["is_local_graph_estimate"] is False
    assert audit["factor_count_per_horizon"] == 318


def test_contract_rejects_local_reestimate(tmp_path: Path) -> None:
    _write_contract(tmp_path, local_residualization=True)
    with pytest.raises(ValueError, match="local_residualization=false"):
        load_core4_campaign_contract(tmp_path)


def test_scope_annotation_is_explicit() -> None:
    frame = pd.DataFrame({"scope": ["global", "within_theme", "inter_theme"]})
    annotated = annotate_scope_frame(frame)
    assert annotated.loc[1, "graph_estimation_scope"] == "global_market"
    assert annotated.loc[1, "canonical_name"] == "induced_global_graph_local_rank"
    assert annotated.loc[2, "execution_unit"] == "theme_portfolio"
    assert semantics_for_scope("within_theme").is_local_graph_estimate is False
