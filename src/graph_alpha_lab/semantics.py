from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import math

import pandas as pd

_REQUIRED_MAPPING = {"symbol", "sector"}
_THEME_KEYS = ["trade_date", "decision_time", "layer", "scale", "theme_id", "tree_depth", "channel"]


@dataclass(frozen=True)
class SemanticThresholds:
    min_theme_size: int = 5
    min_mapping_coverage: float = 0.70
    min_total_purity: float = 0.50
    high_purity: float = 0.75


def normalize_sector_mapping(frame: pd.DataFrame, *, default_source: str = "user_supplied") -> pd.DataFrame:
    missing = _REQUIRED_MAPPING - set(frame.columns)
    if missing:
        raise ValueError(f"sector mapping missing columns: {sorted(missing)}")
    out = frame.copy()
    out["symbol"] = out["symbol"].astype(str).str.upper().str.strip()
    out["sector"] = out["sector"].astype(str).str.strip()
    out = out[(out.symbol != "") & (out.sector != "")].copy()
    if "industry" not in out: out["industry"] = pd.NA
    if "source" not in out: out["source"] = default_source
    if "confidence" not in out: out["confidence"] = 1.0
    if "effective_from" not in out: out["effective_from"] = pd.Timestamp("1900-01-01")
    if "effective_to" not in out: out["effective_to"] = pd.Timestamp("2262-04-11")
    if "as_of_date" not in out: out["as_of_date"] = pd.NaT
    out["confidence"] = pd.to_numeric(out["confidence"], errors="coerce").fillna(0).clip(0, 1)
    for c in ["effective_from", "effective_to", "as_of_date"]:
        out[c] = pd.to_datetime(out[c], errors="coerce")
    out = out.sort_values(["symbol", "effective_from", "confidence"], ascending=[True, False, False])
    dup = out.duplicated(["symbol", "effective_from"], keep=False)
    if dup.any():
        conflicts = out.loc[dup].groupby(["symbol", "effective_from"])["sector"].nunique()
        bad = conflicts[conflicts > 1]
        if len(bad):
            raise ValueError(f"conflicting sector assignments: {bad.index.tolist()[:10]}")
    return out.drop_duplicates(["symbol", "effective_from", "sector"], keep="first").reset_index(drop=True)


def active_mapping(mapping: pd.DataFrame, trade_date: str | pd.Timestamp) -> pd.DataFrame:
    day = pd.Timestamp(trade_date).normalize()
    m = mapping[(mapping.effective_from <= day) & (mapping.effective_to > day)].copy()
    return m.sort_values(["symbol", "effective_from", "confidence"], ascending=[True, False, False]).drop_duplicates("symbol")


def _entropy(counts: pd.Series) -> float:
    if counts.sum() <= 0:
        return math.nan
    p = counts / counts.sum()
    return float(-(p * p.map(math.log)).sum())


def build_theme_semantics(memberships: pd.DataFrame, mapping: pd.DataFrame, thresholds: SemanticThresholds = SemanticThresholds()) -> pd.DataFrame:
    m = memberships.copy()
    for old, new in {"date": "trade_date", "layer_id": "layer", "scale_minutes": "scale"}.items():
        if new not in m and old in m: m[new] = m[old]
    if "tree_depth" not in m: m["tree_depth"] = 0
    if "channel" not in m: m["channel"] = "default"
    required = {"trade_date", "decision_time", "layer", "scale", "theme_id", "symbol"}
    missing = required - set(m.columns)
    if missing: raise ValueError(f"memberships missing columns: {sorted(missing)}")
    m["symbol"] = m.symbol.astype(str).str.upper().str.strip()
    m["trade_date"] = pd.to_datetime(m.trade_date).dt.normalize()
    rows: list[dict] = []
    for day, day_members in m.groupby("trade_date", sort=False):
        amap = active_mapping(mapping, day)[["symbol", "sector", "industry", "source", "confidence"]]
        joined = day_members.merge(amap, on="symbol", how="left")
        for keys, g in joined.groupby(_THEME_KEYS, dropna=False, sort=False):
            total = int(g.symbol.nunique())
            mapped = g[g.sector.notna()].drop_duplicates("symbol")
            mapped_count = int(mapped.symbol.nunique())
            counts = mapped.groupby("sector").symbol.nunique().sort_values(ascending=False)
            top_sector = counts.index[0] if len(counts) else None
            top_count = int(counts.iloc[0]) if len(counts) else 0
            coverage = mapped_count / total if total else 0.0
            mapped_purity = top_count / mapped_count if mapped_count else 0.0
            total_purity = top_count / total if total else 0.0
            hhi = float(((counts / mapped_count) ** 2).sum()) if mapped_count else math.nan
            label = "UNMAPPED"
            if total < thresholds.min_theme_size: label = "TOO_SMALL"
            elif coverage < thresholds.min_mapping_coverage: label = "LOW_COVERAGE"
            elif total_purity >= thresholds.high_purity: label = "HIGH_PURITY"
            elif total_purity >= thresholds.min_total_purity: label = "SECTOR_ALIGNED"
            else: label = "CROSS_SECTOR"
            row = dict(zip(_THEME_KEYS, keys))
            row.update(total_members=total, mapped_members=mapped_count, mapping_coverage=coverage,
                       top_sector=top_sector, top_sector_members=top_count,
                       mapped_purity=mapped_purity, total_purity=total_purity,
                       sector_count=int(len(counts)), sector_entropy=_entropy(counts), sector_hhi=hhi,
                       mean_mapping_confidence=float(mapped.confidence.mean()) if mapped_count else math.nan,
                       semantic_label=label)
            rows.append(row)
    return pd.DataFrame(rows)


def add_sector_loo_signals(memberships: pd.DataFrame, mapping: pd.DataFrame, value_col: str = "node_signal") -> pd.DataFrame:
    m = memberships.copy()
    if value_col not in m: raise ValueError(f"missing {value_col}")
    for old, new in {"date":"trade_date", "layer_id":"layer", "scale_minutes":"scale"}.items():
        if new not in m and old in m: m[new] = m[old]
    if "tree_depth" not in m: m["tree_depth"] = 0
    if "channel" not in m: m["channel"] = "default"
    m["symbol"] = m.symbol.astype(str).str.upper().str.strip()
    m["trade_date"] = pd.to_datetime(m.trade_date).dt.normalize()
    outputs=[]
    for day, g in m.groupby("trade_date", sort=False):
        amap = active_mapping(mapping, day)[["symbol","sector"]]
        x = g.merge(amap, on="symbol", how="left")
        keys = _THEME_KEYS
        if "core_score" in x:
            x["_w"] = pd.to_numeric(x["core_score"], errors="coerce").fillna(1.0).clip(lower=0)
        else:
            x["_w"] = 1.0
        x["_wv"] = x["_w"] * pd.to_numeric(x[value_col], errors="coerce")
        all_agg = x.groupby(keys, dropna=False).agg(all_sw=("_w","sum"), all_ss=("_wv","sum")).reset_index()
        sec_agg = x[x.sector.notna()].groupby(keys+["sector"], dropna=False).agg(sec_sw=("_w","sum"), sec_ss=("_wv","sum")).reset_index()
        x = x.merge(all_agg,on=keys,how="left").merge(sec_agg,on=keys+["sector"],how="left")
        x["theme_loo"]=(x.all_ss-x._wv)/(x.all_sw-x._w).replace(0,pd.NA)
        x["same_sector_loo"]=(x.sec_ss-x._wv)/(x.sec_sw-x._w).replace(0,pd.NA)
        x["cross_sector_loo"]=(x.all_ss-x.sec_ss)/(x.all_sw-x.sec_sw).replace(0,pd.NA)
        x["semantic_disagreement"] = x["same_sector_loo"] - x["cross_sector_loo"]
        outputs.append(x[keys+["symbol","sector","theme_loo","same_sector_loo","cross_sector_loo","semantic_disagreement"]])
    return pd.concat(outputs, ignore_index=True) if outputs else pd.DataFrame()


def load_mapping(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path) if path.suffix.lower() == ".csv" else pd.read_parquet(path)
    return normalize_sector_mapping(frame)
