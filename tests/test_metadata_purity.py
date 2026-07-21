from __future__ import annotations

import pandas as pd

from graphalphalab.metadata import normalize_metadata
from graphalphalab.purity import evaluate_theme_purity


def test_metadata_auto_dimensions_and_market_cap_bucket() -> None:
    metadata = pd.DataFrame({
        "symbol": ["A", "B", "C", "D"],
        "sector_code": ["Tech", "Tech", "Health", "Health"],
        "industry_code": ["Semi", "Software", "Biotech", "Biotech"],
        "country": ["US", "US", "US", "CA"],
        "market_cap": [3e11, 5e10, 5e9, 1e8],
    })
    normalized, profile = normalize_metadata(metadata)
    assert "sector_code" in profile.dimensions
    assert "industry_code" in profile.dimensions
    assert "market_cap_bucket" in profile.dimensions
    assert normalized["market_cap_bucket"].notna().all()


def test_theme_purity_reports_multiple_dimensions() -> None:
    memberships = pd.DataFrame({
        "trade_date": ["2026-07-01"] * 4,
        "decision_time": pd.to_datetime(["2026-07-01T14:30:00Z"] * 4),
        "layer_id": ["similarity"] * 4,
        "scale_minutes": [30] * 4,
        "theme_id": ["T1", "T1", "T2", "T2"],
        "symbol": ["A", "B", "C", "D"],
        "membership_weight": [1.0, 1.0, 1.0, 1.0],
    })
    metadata = pd.DataFrame({
        "symbol": ["A", "B", "C", "D"],
        "sector_code": ["Tech", "Tech", "Health", "Health"],
        "industry_code": ["Semi", "Semi", "Biotech", "Pharma"],
        "country": ["US", "US", "US", "CA"],
    })
    result = evaluate_theme_purity(memberships, metadata)
    summary = result.dimension_summary.set_index("dimension")
    assert summary.loc["sector_code", "weighted_purity"] == 1.0
    assert 0.0 < summary.loc["industry_code", "weighted_purity"] < 1.0
    assert set(result.snapshot_agreement["dimension"]) >= {"sector_code", "industry_code"}
