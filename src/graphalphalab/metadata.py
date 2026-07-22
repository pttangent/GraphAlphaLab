from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Iterable

import numpy as np
import pandas as pd


ALIASES: dict[str, tuple[str, ...]] = {
    "sector": ("sector", "sector_code", "gics_sector", "sector_name"),
    "industry": ("industry", "industry_code", "gics_industry", "industry_name"),
    "sub_industry": ("sub_industry", "subindustry", "gics_sub_industry", "sub_industry_code"),
    "semantic_theme": ("semantic_theme", "theme", "business_theme", "product_theme"),
    "country": ("country", "country_code", "domicile"),
    "exchange": ("exchange", "primary_exchange"),
    "security_type": ("security_type", "quote_type", "asset_type"),
}

_EXCLUDED_AUTO = {
    "symbol", "source_symbol", "symbol_id", "security_entity_id", "company_name",
    "fetch_status", "fetch_error", "last_price", "shares_outstanding", "enterprise_value",
    "rank", "currency",
}


@dataclass(frozen=True)
class MetadataProfile:
    id_column: str
    rows: int
    unique_ids: int
    duplicate_ids: int
    dimensions: tuple[str, ...]
    coverage: dict[str, float]
    cardinality: dict[str, int]
    aliases: dict[str, str]

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def resolve_id_column(frame: pd.DataFrame, preferred: str | None = None) -> str:
    if preferred:
        if preferred not in frame.columns:
            raise ValueError(f"Metadata ID column {preferred!r} is missing")
        return preferred
    for candidate in ("symbol_id", "symbol", "security_entity_id"):
        if candidate in frame.columns:
            return candidate
    raise ValueError("Metadata needs symbol_id, symbol or security_entity_id")


def _resolve_aliases(columns: Iterable[str]) -> dict[str, str]:
    available = set(columns)
    resolved: dict[str, str] = {}
    for canonical, candidates in ALIASES.items():
        match = next((value for value in candidates if value in available), None)
        if match:
            resolved[canonical] = match
    return resolved


def add_market_cap_bucket(frame: pd.DataFrame, column: str = "market_cap") -> pd.DataFrame:
    result = frame.copy()
    if column not in result.columns:
        return result
    values = pd.to_numeric(result[column], errors="coerce")
    result["market_cap_bucket"] = pd.cut(
        values,
        bins=[-np.inf, 3e8, 2e9, 1e10, 2e11, np.inf],
        labels=["micro_<300m", "small_300m_2b", "mid_2b_10b", "large_10b_200b", "mega_>=200b"],
    ).astype("string")
    return result


def auto_dimensions(frame: pd.DataFrame, id_column: str) -> tuple[str, ...]:
    preferred: list[str] = []
    aliases = _resolve_aliases(frame.columns)
    for canonical in ("sector", "industry", "sub_industry", "semantic_theme", "country", "exchange", "security_type"):
        value = aliases.get(canonical)
        if value and value not in preferred:
            series = frame[value]
            if series.nunique(dropna=True) >= 2 and float(series.notna().mean()) >= 0.05:
                preferred.append(value)
    for explicit in ("market_cap_bucket", "is_etf", "quote_type"):
        if explicit in frame.columns and explicit not in preferred:
            series = frame[explicit]
            if series.nunique(dropna=True) >= 2 and float(series.notna().mean()) >= 0.05:
                preferred.append(explicit)
    for column in frame.columns:
        if column == id_column or column in _EXCLUDED_AUTO or column in preferred:
            continue
        series = frame[column]
        cardinality = int(series.nunique(dropna=True))
        coverage = float(series.notna().mean()) if len(series) else 0.0
        if 2 <= cardinality <= 500 and coverage >= 0.05 and (
            pd.api.types.is_string_dtype(series.dtype)
            or pd.api.types.is_bool_dtype(series.dtype)
            or isinstance(series.dtype, pd.CategoricalDtype)
        ):
            preferred.append(column)
    return tuple(preferred)


def normalize_metadata(
    frame: pd.DataFrame,
    *,
    id_column: str | None = None,
    dimensions: Iterable[str] | None = None,
) -> tuple[pd.DataFrame, MetadataProfile]:
    result = add_market_cap_bucket(frame)
    id_column = resolve_id_column(result, id_column)
    result = result.copy()
    if pd.api.types.is_string_dtype(result[id_column].dtype):
        result[id_column] = result[id_column].astype("string").str.strip()
    duplicate_ids = int(result[id_column].duplicated(keep=False).sum())
    if duplicate_ids:
        order = [column for column in ("fetch_status", "market_cap") if column in result.columns]
        if "fetch_status" in order:
            result["_status_rank"] = result["fetch_status"].eq("ok").astype(int)
            order = ["_status_rank", *[c for c in order if c != "fetch_status"]]
        result = result.sort_values(order, ascending=False, na_position="last") if order else result
        result = result.drop_duplicates(id_column, keep="first").drop(columns=["_status_rank"], errors="ignore")
    chosen = tuple(dimensions) if dimensions else auto_dimensions(result, id_column)
    missing = [column for column in chosen if column not in result.columns]
    if missing:
        raise ValueError(f"Metadata purity dimensions are missing: {missing}")
    for column in chosen:
        if pd.api.types.is_string_dtype(result[column].dtype) or result[column].dtype == object:
            result[column] = result[column].astype("string").str.strip().replace("", pd.NA)
    coverage = {column: float(result[column].notna().mean()) for column in chosen}
    cardinality = {column: int(result[column].nunique(dropna=True)) for column in chosen}
    profile = MetadataProfile(
        id_column=id_column,
        rows=int(len(result)),
        unique_ids=int(result[id_column].nunique(dropna=True)),
        duplicate_ids=duplicate_ids,
        dimensions=chosen,
        coverage=coverage,
        cardinality=cardinality,
        aliases=_resolve_aliases(result.columns),
    )
    return result, profile


def profile_metadata(frame: pd.DataFrame, **kwargs) -> MetadataProfile:
    return normalize_metadata(frame, **kwargs)[1]
