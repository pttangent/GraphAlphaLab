from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Iterable

import numpy as np
import pandas as pd
from sklearn.metrics import adjusted_rand_score, normalized_mutual_info_score

from .metadata import normalize_metadata, resolve_id_column


@dataclass
class PurityResult:
    theme_dimension: pd.DataFrame
    dimension_summary: pd.DataFrame
    snapshot_agreement: pd.DataFrame
    metadata_profile: dict[str, object]


def _entropy(weights: np.ndarray) -> tuple[float, float, float]:
    weights = weights[weights > 0]
    if len(weights) == 0:
        return math.nan, math.nan, math.nan
    probabilities = weights / weights.sum()
    entropy = float(-(probabilities * np.log(probabilities)).sum())
    normalized = float(entropy / np.log(len(probabilities))) if len(probabilities) > 1 else 0.0
    effective = float(np.exp(entropy))
    return entropy, normalized, effective


def _group_keys(frame: pd.DataFrame) -> list[str]:
    preferred = [
        "batch_id", "trade_date", "decision_time", "layer_id", "scale_minutes",
        "variant_id", "theme_id",
    ]
    keys = [column for column in preferred if column in frame.columns]
    if "theme_id" not in keys:
        raise ValueError("Theme memberships require theme_id")
    return keys


def evaluate_theme_purity(
    memberships: pd.DataFrame,
    metadata: pd.DataFrame,
    *,
    dimensions: Iterable[str] | None = None,
    membership_id: str | None = None,
    metadata_id: str | None = None,
    weight_column: str = "membership_weight",
) -> PurityResult:
    metadata_norm, profile = normalize_metadata(metadata, id_column=metadata_id, dimensions=dimensions)
    metadata_id = profile.id_column
    if membership_id is None:
        membership_id = metadata_id if metadata_id in memberships.columns else resolve_id_column(memberships)
    if membership_id not in memberships.columns:
        raise ValueError(f"Membership ID column {membership_id!r} is missing")
    frame = memberships.copy()
    if weight_column not in frame.columns:
        frame[weight_column] = 1.0
    frame[weight_column] = pd.to_numeric(frame[weight_column], errors="coerce").fillna(0.0).clip(lower=0.0)
    meta_columns = [metadata_id, *profile.dimensions]
    joined = frame.merge(
        metadata_norm[meta_columns],
        left_on=membership_id,
        right_on=metadata_id,
        how="left",
        validate="many_to_one",
    )
    keys = _group_keys(joined)
    rows: list[dict[str, object]] = []
    global_shares: dict[str, pd.Series] = {
        dimension: metadata_norm[dimension].value_counts(normalize=True, dropna=True)
        for dimension in profile.dimensions
    }
    for group_values, group in joined.groupby(keys, observed=True, dropna=False):
        base = dict(zip(keys, group_values if isinstance(group_values, tuple) else (group_values,)))
        total_count = int(len(group))
        total_weight = float(group[weight_column].sum())
        for dimension in profile.dimensions:
            covered = group[group[dimension].notna()].copy()
            counts = covered[dimension].value_counts(dropna=True)
            weighted = covered.groupby(dimension, observed=True)[weight_column].sum().sort_values(ascending=False)
            dominant = weighted.index[0] if len(weighted) else None
            dominant_weight = float(weighted.iloc[0]) if len(weighted) else 0.0
            dominant_count = int(counts.get(dominant, 0)) if dominant is not None else 0
            covered_weight = float(weighted.sum())
            entropy, normalized_entropy, effective = _entropy(weighted.to_numpy(dtype=float))
            global_share = float(global_shares[dimension].get(dominant, np.nan)) if dominant is not None else np.nan
            rows.append({
                **base,
                "dimension": dimension,
                "theme_member_count": total_count,
                "theme_weight": total_weight,
                "covered_member_count": int(len(covered)),
                "covered_weight": covered_weight,
                "coverage": float(len(covered) / total_count) if total_count else np.nan,
                "weighted_coverage": float(covered_weight / total_weight) if total_weight else np.nan,
                "category_count": int(len(weighted)),
                "dominant_value": dominant,
                "dominant_count": dominant_count,
                "dominant_weight": dominant_weight,
                "purity_count": float(dominant_count / len(covered)) if len(covered) else np.nan,
                "purity_weighted": float(dominant_weight / covered_weight) if covered_weight else np.nan,
                "hhi": float(((weighted / covered_weight) ** 2).sum()) if covered_weight else np.nan,
                "entropy": entropy,
                "normalized_entropy": normalized_entropy,
                "effective_categories": effective,
                "global_dominant_share": global_share,
                "dominant_lift": float((dominant_weight / covered_weight) / global_share)
                if covered_weight and global_share and np.isfinite(global_share) else np.nan,
            })
    detail = pd.DataFrame(rows)
    if detail.empty:
        summary = pd.DataFrame()
    else:
        summary_rows = []
        for dimension, group in detail.groupby("dimension", observed=True):
            weights = group["covered_member_count"].fillna(0).to_numpy(dtype=float)
            purity = group["purity_weighted"].to_numpy(dtype=float)
            valid = np.isfinite(purity) & (weights > 0)
            summary_rows.append({
                "dimension": dimension,
                "theme_count": int(group["theme_id"].nunique()) if "theme_id" in group else int(len(group)),
                "mean_coverage": float(group["coverage"].mean()),
                "weighted_purity": float(np.average(purity[valid], weights=weights[valid])) if valid.any() else np.nan,
                "median_purity": float(group["purity_weighted"].median()),
                "p10_purity": float(group["purity_weighted"].quantile(0.10)),
                "p90_purity": float(group["purity_weighted"].quantile(0.90)),
                "mean_normalized_entropy": float(group["normalized_entropy"].mean()),
                "mean_dominant_lift": float(group["dominant_lift"].replace([np.inf, -np.inf], np.nan).mean()),
            })
        summary = pd.DataFrame(summary_rows)

    snapshot_keys = [column for column in ("batch_id", "trade_date", "decision_time", "layer_id", "scale_minutes") if column in joined.columns]
    agreement_rows: list[dict[str, object]] = []
    for values, snapshot in joined.groupby(snapshot_keys, observed=True, dropna=False) if snapshot_keys else [((), joined)]:
        base = dict(zip(snapshot_keys, values if isinstance(values, tuple) else (values,)))
        for dimension in profile.dimensions:
            valid = snapshot[["theme_id", dimension]].dropna()
            if len(valid) < 3 or valid["theme_id"].nunique() < 2 or valid[dimension].nunique() < 2:
                nmi = ari = np.nan
            else:
                nmi = float(normalized_mutual_info_score(valid[dimension].astype(str), valid["theme_id"].astype(str)))
                ari = float(adjusted_rand_score(valid[dimension].astype(str), valid["theme_id"].astype(str)))
            agreement_rows.append({
                **base,
                "dimension": dimension,
                "covered_members": int(len(valid)),
                "theme_count": int(valid["theme_id"].nunique()),
                "category_count": int(valid[dimension].nunique()),
                "nmi": nmi,
                "ari": ari,
            })
    return PurityResult(detail, summary, pd.DataFrame(agreement_rows), profile.as_dict())
