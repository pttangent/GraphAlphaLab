from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable
import json

import numpy as np
import pandas as pd


MATCH_COLUMNS = (
    "scope",
    "theme_family",
    "layer_id",
    "scale_minutes",
    "horizon",
    "horizon_minutes",
)


@dataclass(frozen=True)
class ExecutionCandidate:
    scope: str
    theme_family: str
    factor_id: str
    layer_id: str
    scale_minutes: int
    variant_id: str
    horizon: str
    horizon_minutes: int
    candidate_rank: int
    candidate_score: float
    selection_tier: str
    graph_abs_ic: float
    abs_ic_increment_vs_node: float | None
    abs_ic_increment_vs_reverse_placebo: float | None
    graph_gross_mean: float | None
    graph_mean_turnover: float | None
    date_count: int
    observations: int

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def _as_float(value: object) -> float:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return float(numeric) if pd.notna(numeric) else np.nan


def _as_bool(value: object, default: bool = False) -> bool:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return default
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "pass"}
    return bool(value)


def _identity_group_columns(frame: pd.DataFrame) -> list[str]:
    missing = [column for column in MATCH_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"Alpha metrics are missing candidate identity columns: {missing}")
    return list(MATCH_COLUMNS)


def _candidate_rows(metrics: pd.DataFrame, *, min_date_count: int) -> pd.DataFrame:
    required = {
        "factor_id",
        "variant_id",
        "mean_spearman_ic",
        "date_count",
        "observations",
    }
    missing = sorted(required - set(metrics.columns))
    if missing:
        raise ValueError(f"Alpha metrics are missing required columns: {missing}")

    data = metrics.copy()
    if "financial_role" in data.columns:
        data = data[data["financial_role"].astype(str) == "direct_return_alpha"]
    if "semantic_promotion_eligible" in data.columns:
        data = data[data["semantic_promotion_eligible"].map(_as_bool)]
    data = data[
        data["variant_id"].isin(
            ["graph_forward", "node_baseline", "graph_reverse_placebo"]
        )
    ]
    if data.empty:
        return pd.DataFrame()

    rows: list[dict[str, object]] = []
    keys = _identity_group_columns(data)
    for key_values, group in data.groupby(
        keys,
        observed=True,
        dropna=False,
        sort=False,
    ):
        values = key_values if isinstance(key_values, tuple) else (key_values,)
        identity = dict(zip(keys, values))
        by_variant = {
            str(row["variant_id"]): row
            for _, row in group.drop_duplicates("variant_id", keep="last").iterrows()
        }
        graph = by_variant.get("graph_forward")
        if graph is None:
            continue
        date_value = _as_float(graph.get("date_count", 0))
        date_count = int(date_value) if np.isfinite(date_value) else 0
        if date_count < int(min_date_count):
            continue
        graph_ic = abs(_as_float(graph.get("mean_spearman_ic")))
        if not np.isfinite(graph_ic):
            continue
        node = by_variant.get("node_baseline")
        reverse = by_variant.get("graph_reverse_placebo")
        node_ic = (
            abs(_as_float(node.get("mean_spearman_ic")))
            if node is not None
            else np.nan
        )
        reverse_ic = (
            abs(_as_float(reverse.get("mean_spearman_ic")))
            if reverse is not None
            else np.nan
        )
        inc_node = graph_ic - node_ic if np.isfinite(node_ic) else np.nan
        inc_reverse = graph_ic - reverse_ic if np.isfinite(reverse_ic) else np.nan
        strict = bool(
            np.isfinite(inc_node)
            and np.isfinite(inc_reverse)
            and inc_node > 0
            and inc_reverse > 0
        )
        observations = _as_float(graph.get("observations", 0))
        rows.append(
            {
                **identity,
                "factor_id": str(graph["factor_id"]),
                "variant_id": "graph_forward",
                "date_count": date_count,
                "observations": int(observations) if np.isfinite(observations) else 0,
                "graph_abs_ic": graph_ic,
                "abs_ic_increment_vs_node": inc_node,
                "abs_ic_increment_vs_reverse_placebo": inc_reverse,
                "graph_gross_mean": _as_float(
                    graph.get("oriented_long_short_mean")
                ),
                "graph_mean_turnover": _as_float(graph.get("mean_turnover")),
                "daily_ic_sign_consistency": _as_float(
                    graph.get("daily_ic_sign_consistency")
                ),
                "spearman_icir": abs(_as_float(graph.get("spearman_icir"))),
                "selection_tier": (
                    "strict_incremental" if strict else "exploratory"
                ),
            }
        )
    return pd.DataFrame(rows)


def _rank_component(
    frame: pd.DataFrame,
    column: str,
    *,
    higher_is_better: bool = True,
) -> pd.Series:
    values = pd.to_numeric(frame[column], errors="coerce")
    if not higher_is_better:
        values = -values
    return values.rank(method="average", pct=True, na_option="bottom")


def select_execution_candidates(
    metrics: pd.DataFrame,
    *,
    min_date_count: int = 20,
    max_per_horizon: int = 12,
    min_per_horizon: int = 2,
    scopes: Iterable[str] = ("global", "within_theme", "inter_theme"),
) -> pd.DataFrame:
    if max_per_horizon <= 0:
        raise ValueError("max_per_horizon must be positive")
    if min_per_horizon < 0 or min_per_horizon > max_per_horizon:
        raise ValueError("min_per_horizon must be between 0 and max_per_horizon")
    candidates = _candidate_rows(metrics, min_date_count=min_date_count)
    if candidates.empty:
        return candidates
    selected_scopes = {str(scope) for scope in scopes}
    candidates = candidates[
        candidates["scope"].astype(str).isin(selected_scopes)
    ].copy()
    if candidates.empty:
        return candidates

    turnover = pd.to_numeric(candidates["graph_mean_turnover"], errors="coerce")
    gross = pd.to_numeric(candidates["graph_gross_mean"], errors="coerce").abs()
    candidates["gross_per_turnover"] = gross / turnover.clip(lower=1e-9)
    candidates["incremental_ic_sum"] = (
        pd.to_numeric(
            candidates["abs_ic_increment_vs_node"],
            errors="coerce",
        )
        .clip(lower=0)
        .fillna(0)
        + pd.to_numeric(
            candidates["abs_ic_increment_vs_reverse_placebo"],
            errors="coerce",
        )
        .clip(lower=0)
        .fillna(0)
    )

    ranked_groups: list[pd.DataFrame] = []
    for _, group in candidates.groupby("horizon", observed=True, sort=True):
        group = group.copy()
        group["candidate_score"] = (
            0.35 * _rank_component(group, "incremental_ic_sum")
            + 0.25 * _rank_component(group, "graph_abs_ic")
            + 0.15 * _rank_component(group, "daily_ic_sign_consistency")
            + 0.15 * _rank_component(group, "gross_per_turnover")
            + 0.10 * _rank_component(group, "spearman_icir")
        )
        strict = group[group["selection_tier"] == "strict_incremental"].sort_values(
            ["candidate_score", "graph_abs_ic"],
            ascending=[False, False],
        )
        exploratory = group[
            group["selection_tier"] != "strict_incremental"
        ].sort_values(
            ["candidate_score", "graph_abs_ic"],
            ascending=[False, False],
        )
        chosen = strict.head(max_per_horizon).copy()
        selected_factor_ids = set(chosen["factor_id"].astype(str))
        required = max(0, min_per_horizon - len(chosen))
        if required:
            filler = exploratory[
                ~exploratory["factor_id"].astype(str).isin(selected_factor_ids)
            ].head(required)
            chosen = pd.concat([chosen, filler], ignore_index=True)
            selected_factor_ids.update(filler["factor_id"].astype(str))
        if len(chosen) < max_per_horizon:
            remaining = exploratory[
                ~exploratory["factor_id"].astype(str).isin(selected_factor_ids)
            ]
            chosen = pd.concat(
                [chosen, remaining.head(max_per_horizon - len(chosen))],
                ignore_index=True,
            )
        chosen = chosen.head(max_per_horizon).copy()
        chosen["candidate_rank"] = np.arange(1, len(chosen) + 1)
        ranked_groups.append(chosen)

    result = (
        pd.concat(ranked_groups, ignore_index=True)
        if ranked_groups
        else pd.DataFrame()
    )
    keep = [
        "scope",
        "theme_family",
        "factor_id",
        "layer_id",
        "scale_minutes",
        "variant_id",
        "horizon",
        "horizon_minutes",
        "candidate_rank",
        "candidate_score",
        "selection_tier",
        "graph_abs_ic",
        "abs_ic_increment_vs_node",
        "abs_ic_increment_vs_reverse_placebo",
        "graph_gross_mean",
        "graph_mean_turnover",
        "date_count",
        "observations",
    ]
    return result[keep].sort_values(
        ["horizon_minutes", "candidate_rank", "scope", "factor_id"]
    ).reset_index(drop=True)


def candidate_manifest_payload(
    candidates: pd.DataFrame,
    *,
    source_metrics: str | Path,
    parameters: dict[str, object],
) -> dict[str, object]:
    rows = candidates.to_dict("records") if not candidates.empty else []
    return {
        "version": "GAL_EXECUTION_CANDIDATES_V1",
        "source_metrics": str(Path(source_metrics).expanduser().resolve()),
        "parameters": dict(parameters),
        "candidate_count": len(rows),
        "candidates": rows,
    }


def write_candidate_manifest(
    candidates: pd.DataFrame,
    *,
    source_metrics: str | Path,
    output: str | Path,
    parameters: dict[str, object],
) -> Path:
    destination = Path(output).expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    payload = candidate_manifest_payload(
        candidates,
        source_metrics=source_metrics,
        parameters=parameters,
    )
    destination.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return destination
