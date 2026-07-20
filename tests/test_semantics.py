import pandas as pd
import pytest

from graph_alpha_lab.semantics import (
    SemanticThresholds,
    add_sector_loo_signals,
    build_theme_semantics,
    normalize_sector_mapping,
)


def sample_memberships():
    return pd.DataFrame({
        "trade_date": ["2026-07-01"] * 5,
        "decision_time": [1] * 5,
        "layer": ["x"] * 5,
        "scale": [15] * 5,
        "theme_id": ["t"] * 5,
        "tree_depth": [1] * 5,
        "channel": ["d"] * 5,
        "symbol": list("ABCDE"),
        "node_signal": [1.0, 2.0, 3.0, 4.0, 5.0],
        "core_score": [1.0] * 5,
    })


def test_purity_uses_total_theme_members_not_only_mapped():
    mapping = normalize_sector_mapping(pd.DataFrame({"symbol": ["A", "B", "C"], "sector": ["Semi", "Semi", "Semi"]}))
    out = build_theme_semantics(sample_memberships(), mapping, SemanticThresholds(min_mapping_coverage=0.5, min_total_purity=0.5))
    row = out.iloc[0]
    assert row.mapping_coverage == pytest.approx(0.6)
    assert row.mapped_purity == pytest.approx(1.0)
    assert row.total_purity == pytest.approx(0.6)


def test_effective_dates_are_point_in_time():
    mapping = normalize_sector_mapping(pd.DataFrame({
        "symbol": ["A", "A"],
        "sector": ["Old", "New"],
        "effective_from": ["2020-01-01", "2026-07-02"],
    }))
    out = build_theme_semantics(sample_memberships(), mapping, SemanticThresholds(min_mapping_coverage=0, min_total_purity=0))
    assert out.iloc[0].top_sector == "Old"


def test_sector_loo_excludes_self_and_cross_sector():
    mapping = normalize_sector_mapping(pd.DataFrame({"symbol": list("ABCDE"), "sector": ["S", "S", "S", "X", "X"]}))
    out = add_sector_loo_signals(sample_memberships(), mapping)
    row = out[out.symbol == "A"].iloc[0]
    assert row.same_sector_loo == pytest.approx(2.5)
    assert row.cross_sector_loo == pytest.approx(4.5)
