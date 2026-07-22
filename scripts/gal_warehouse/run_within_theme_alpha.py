from __future__ import annotations

import argparse
import json
import math
import os
import shutil
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from scipy import stats

from graphalphalab.checkpoint import (
    CheckpointSpec,
    checkpoint_valid,
    commit_frames,
    load_checkpoint_frames,
    safe_key,
    write_progress,
)
from graphalphalab.governance import atomic_write_json, atomic_write_text, sha256_json


def csv_list(value: str | None) -> list[str]:
    return [x.strip() for x in (value or "").split(",") if x.strip()]


def sql_literal(value: object) -> str:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return "NULL"
    if isinstance(value, (int, float, np.integer, np.floating)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def sql_path(path: Path) -> str:
    return path.expanduser().resolve().as_posix().replace("'", "''")


def parquet_expr(path: Path, filename: str) -> str:
    resolved = path.expanduser().resolve()
    target = resolved / "**" / filename if resolved.is_dir() else resolved
    return f"read_parquet('{sql_path(target)}', union_by_name=true, hive_partitioning=false)"


def columns(con: duckdb.DuckDBPyConnection, view: str) -> set[str]:
    return {str(row[0]) for row in con.execute(f"DESCRIBE {view}").fetchall()}


def safe_corr(group: pd.DataFrame, method: str) -> float:
    clean = group[["_score", "target_return"]].replace([np.inf, -np.inf], np.nan).dropna()
    if len(clean) < 3 or clean["_score"].nunique() < 2 or clean["target_return"].nunique() < 2:
        return np.nan
    return float(clean["_score"].corr(clean["target_return"], method=method))


def ttest(values: pd.Series) -> tuple[float, float]:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    if len(clean) < 2 or clean.std(ddof=1) <= 0:
        return np.nan, np.nan
    t = float(clean.mean() / (clean.std(ddof=1) / math.sqrt(len(clean))))
    return t, float(2 * stats.t.sf(abs(t), len(clean) - 1))


def bh_fdr(values: pd.Series) -> pd.Series:
    raw = pd.to_numeric(values, errors="coerce")
    valid = raw.dropna().sort_values()
    out = pd.Series(np.nan, index=raw.index, dtype=float)
    if valid.empty:
        return out
    adjusted = valid.to_numpy(float) * len(valid) / np.arange(1, len(valid) + 1)
    adjusted = np.minimum.accumulate(adjusted[::-1])[::-1]
    out.loc[valid.index] = np.minimum(adjusted, 1.0)
    return out


def residualize(group: pd.DataFrame, controls: list[str]) -> pd.Series:
    selected = [c for c in controls if c in group.columns]
    numeric = group[["score", *selected]].apply(pd.to_numeric, errors="coerce").replace([np.inf, -np.inf], np.nan)
    out = pd.Series(np.nan, index=group.index, dtype=float)
    valid = numeric.dropna()
    if not selected:
        out.loc[valid.index] = valid["score"]
        return out
    if len(valid) < max(10, len(selected) + 5):
        return out
    y = valid["score"].rank(method="average", pct=True).to_numpy(float)
    xcols = [np.ones(len(valid))]
    for control in selected:
        if valid[control].nunique() >= 2:
            xcols.append(valid[control].rank(method="average", pct=True).to_numpy(float))
    x = np.column_stack(xcols)
    out.loc[valid.index] = y - x @ np.linalg.lstsq(x, y, rcond=None)[0]
    return out


def factor_condition(key_columns: list[str], row: pd.Series) -> str:
    terms = []
    for column in key_columns:
        value = row[column]
        terms.append(f's."{column}" IS NULL' if pd.isna(value) else f's."{column}"={sql_literal(value)}')
    return " AND ".join(terms)


def evaluate_factor(
    frame: pd.DataFrame,
    identity: dict[str, object],
    *,
    quantiles: int,
    min_theme_size: int,
    min_themes: int,
    direction_mode: str,
    controls: list[str],
    costs: list[float],
) -> tuple[dict[str, object], pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    data = frame.copy()
    for column in ("decision_time", "theme_decision_time"):
        data[column] = pd.to_datetime(data[column], utc=True, errors="coerce")
    for column in ("score", "target_return", *controls):
        if column in data:
            data[column] = pd.to_numeric(data[column], errors="coerce")
    data = data.dropna(subset=["decision_time", "theme_decision_time", "theme_id", "symbol_id", "score", "target_return"])
    lag_violations = int((data["theme_decision_time"] >= data["decision_time"]).sum())

    group_keys = ["trade_date", "decision_time", "theme_id"]
    score_parts = [residualize(group, controls) for _, group in data.groupby(group_keys, observed=True, sort=False)]
    data["_score"] = pd.concat(score_parts).sort_index() if score_parts else np.nan
    data = data.dropna(subset=["_score"])
    sizes = data.groupby(group_keys, observed=True)["symbol_id"].transform("nunique")
    data = data[sizes >= min_theme_size].copy()
    if data.empty:
        return {**identity, "decision_count": 0, "sample_sufficient": False, "research_status": "insufficient_or_rejected"}, pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    data["_rank"] = data.groupby(group_keys, observed=True)["_score"].rank(method="first", pct=True)
    data["_quantile"] = np.minimum(np.ceil(data["_rank"] * quantiles).astype(int), quantiles)
    data["_theme_n"] = data.groupby(group_keys, observed=True)["symbol_id"].transform("nunique")

    theme_stats = (
        data.groupby(group_keys, observed=True)
        .apply(
            lambda g: pd.Series(
                {
                    "theme_member_count": len(g),
                    "spearman_ic": safe_corr(g, "spearman"),
                    "pearson_ic": safe_corr(g, "pearson"),
                    "raw_top_minus_bottom_return": g.loc[g["_quantile"] == quantiles, "target_return"].mean()
                    - g.loc[g["_quantile"] == 1, "target_return"].mean(),
                    "long_high_n": int((g["_quantile"] == quantiles).sum()),
                    "long_low_n": int((g["_quantile"] == 1).sum()),
                }
            ),
            include_groups=False,
        )
        .reset_index()
    )
    valid_decisions = theme_stats.groupby(["trade_date", "decision_time"], observed=True)["theme_id"].nunique()
    valid_index = valid_decisions[valid_decisions >= min_themes].index
    valid_frame = pd.DataFrame(valid_index.tolist(), columns=["trade_date", "decision_time"])
    theme_stats = theme_stats.merge(valid_frame, on=["trade_date", "decision_time"], how="inner")
    data = data.merge(valid_frame, on=["trade_date", "decision_time"], how="inner")
    if theme_stats.empty:
        return {**identity, "decision_count": 0, "sample_sufficient": False, "research_status": "insufficient_or_rejected"}, pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    weighted_ic = theme_stats.assign(
        _spearman_x_n=theme_stats["spearman_ic"] * theme_stats["theme_member_count"],
        _pearson_x_n=theme_stats["pearson_ic"] * theme_stats["theme_member_count"],
    )
    ic = weighted_ic.groupby(["trade_date", "decision_time"], observed=True).agg(
        theme_count=("theme_id", "nunique"),
        n=("theme_member_count", "sum"),
        spearman_num=("_spearman_x_n", "sum"),
        pearson_num=("_pearson_x_n", "sum"),
    ).reset_index()
    ic["spearman_ic"] = ic["spearman_num"] / ic["n"]
    ic["pearson_ic"] = ic["pearson_num"] / ic["n"]
    ic = ic.drop(columns=["spearman_num", "pearson_num"]).assign(**identity)
    daily_ic = ic.groupby("trade_date", observed=True)[["spearman_ic", "pearson_ic"]].mean()
    mean_ic = float(daily_ic["spearman_ic"].mean())
    direction_predeclared = direction_mode != "auto"
    direction = 1 if direction_mode == "positive" else -1 if direction_mode == "negative" else (1 if mean_ic >= 0 else -1)

    theme_stats["expected_direction"] = direction
    theme_stats["oriented_long_short_return"] = theme_stats["raw_top_minus_bottom_return"] * direction
    theme_stats = theme_stats.assign(**identity)

    data["_side"] = np.where(data["_quantile"] == quantiles, 1, np.where(data["_quantile"] == 1, -1, 0))
    if direction < 0:
        data["_side"] *= -1
    selected = data[data["_side"] != 0].copy()
    selected["_leg_n"] = selected.groupby(group_keys + ["_side"], observed=True)["symbol_id"].transform("count")
    selected["_theme_count"] = selected.groupby(["trade_date", "decision_time"], observed=True)["theme_id"].transform("nunique")
    selected["weight"] = selected["_side"] * 0.5 / selected["_leg_n"] / selected["_theme_count"]

    rows = []
    previous_by_date: dict[str, dict[object, float]] = {}
    for (trade_date, decision_time), group in selected.groupby(["trade_date", "decision_time"], observed=True, sort=True):
        weights = group.groupby("symbol_id", observed=True)["weight"].sum().to_dict()
        previous = previous_by_date.get(str(trade_date), {})
        turnover = float(sum(abs(weights.get(s, 0.0) - previous.get(s, 0.0)) for s in set(weights) | set(previous)))
        previous_by_date[str(trade_date)] = weights
        raw_theme = theme_stats[(theme_stats["trade_date"] == trade_date) & (theme_stats["decision_time"] == decision_time)]
        oriented = float(raw_theme["oriented_long_short_return"].mean())
        row = {
            **identity,
            "trade_date": str(trade_date),
            "decision_time": decision_time,
            "expected_direction": direction,
            "theme_count": int(group["theme_id"].nunique()),
            "raw_top_minus_bottom_return": float(raw_theme["raw_top_minus_bottom_return"].mean()),
            "oriented_long_short_return": oriented,
            "turnover": turnover,
            "long_n": int((group["weight"] > 0).sum()),
            "short_n": int((group["weight"] < 0).sum()),
        }
        for cost in costs:
            row[f"net_return_{cost:g}bps"] = oriented - turnover * cost / 10000.0
        rows.append(row)
    portfolio = pd.DataFrame(rows)
    daily_portfolio = portfolio.groupby("trade_date", observed=True)["oriented_long_short_return"].mean()
    ic_t, ic_p = ttest(daily_ic["spearman_ic"])
    pnl_t, _ = ttest(daily_portfolio)
    positive = portfolio.loc[portfolio["oriented_long_short_return"] > 0, "oriented_long_short_return"].sort_values(ascending=False)
    top_n = max(1, math.ceil(len(portfolio) * 0.05))
    top_share = float(positive.head(top_n).sum() / positive.sum()) if len(positive) and positive.sum() > 0 else np.nan
    metric = {
        **identity,
        "portfolio_mode": "lagged_similarity_leiden_within_theme_equal_theme",
        "observations": int(len(data)),
        "decision_count": int(len(portfolio)),
        "date_count": int(portfolio["trade_date"].nunique()),
        "symbol_count": int(data["symbol_id"].nunique()),
        "theme_count": int(data["theme_id"].nunique()),
        "mean_themes_per_decision": float(portfolio["theme_count"].mean()),
        "mean_spearman_ic": mean_ic,
        "median_spearman_ic": float(daily_ic["spearman_ic"].median()),
        "spearman_ic_tstat_daily": ic_t,
        "spearman_ic_pvalue_daily": ic_p,
        "daily_ic_direction_consistency": float((daily_ic["spearman_ic"] * direction > 0).mean()),
        "expected_direction": direction,
        "direction_source": "predeclared" if direction_predeclared else "observed_daily_ic",
        "direction_predeclared": direction_predeclared,
        "raw_top_minus_bottom_mean": float(portfolio["raw_top_minus_bottom_return"].mean()),
        "oriented_long_short_mean": float(portfolio["oriented_long_short_return"].mean()),
        "oriented_long_short_tstat_daily": pnl_t,
        "oriented_long_short_hit_rate": float((portfolio["oriented_long_short_return"] > 0).mean()),
        "mean_turnover": float(portfolio["turnover"].mean()),
        "top_5pct_positive_pnl_share": top_share,
        "drop_best_day_daily_mean": float(daily_portfolio.drop(daily_portfolio.idxmax()).mean()) if len(daily_portfolio) > 1 else np.nan,
        "drop_best_3_days_daily_mean": float(daily_portfolio.drop(daily_portfolio.nlargest(min(3, len(daily_portfolio))).index).mean()) if len(daily_portfolio) > 3 else np.nan,
        "theme_lag_violations": lag_violations,
        "theme_lag_safe": lag_violations == 0,
        "sample_sufficient": bool(portfolio["trade_date"].nunique() >= 20 and len(portfolio) >= 100 and len(data) >= 500),
    }
    for cost in costs:
        metric[f"net_mean_{cost:g}bps"] = float(portfolio[f"net_return_{cost:g}bps"].mean())
    metric["cost_survives_5bps"] = bool(metric.get("net_mean_5bps", np.nan) > 0)
    metric["research_status"] = "needs_falsification" if metric["sample_sufficient"] else "insufficient_or_rejected"
    return metric, ic, portfolio, theme_stats


def write_frame(frame: pd.DataFrame, root: Path, name: str) -> None:
    frame.to_parquet(root / f"{name}.parquet", index=False)
    frame.to_csv(root / f"{name}.csv", index=False)


def checkpoint_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty and len(frame.columns) == 0:
        return pd.DataFrame({"_checkpoint_empty": pd.Series(dtype="int8")})
    return frame


def restore_checkpoint_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if list(frame.columns) == ["_checkpoint_empty"]:
        return pd.DataFrame()
    return frame


def main() -> None:
    p = argparse.ArgumentParser(description="Evaluate IG27/RM14 graph scores inside lagged Similarity consensus Leiden themes")
    p.add_argument("--batch-id", required=True)
    p.add_argument("--signals", type=Path, required=True)
    p.add_argument("--labels", type=Path, required=True)
    p.add_argument("--label-contract", type=Path, required=True)
    p.add_argument("--memberships", type=Path, required=True)
    p.add_argument("--theme-layer-id", default="similarity_consensus")
    p.add_argument("--factor-ids", default="")
    p.add_argument("--variant-ids", default="graph_forward")
    p.add_argument("--default-direction", choices=("auto", "positive", "negative"), default="auto")
    p.add_argument("--quantiles", type=int, default=5)
    p.add_argument("--min-theme-size", type=int, default=20)
    p.add_argument("--min-themes-per-decision", type=int, default=2)
    p.add_argument("--control-columns", default="own_score")
    p.add_argument("--cost-bps", default="0,1,2,5,10")
    p.add_argument("--memory-limit-gb", type=float, default=24.0)
    p.add_argument("--threads", type=int, default=8)
    p.add_argument("--temp-directory", type=Path)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--force", action="store_true")
    p.add_argument("--reset-checkpoints", action="store_true")
    args = p.parse_args()

    contract = json.loads(args.label_contract.read_text(encoding="utf-8"))
    label_id = str(contract["label_id"])
    target = str(contract.get("target_column", "target_return"))
    factor_filter, variant_filter = set(csv_list(args.factor_ids)), set(csv_list(args.variant_ids))
    controls, costs = csv_list(args.control_columns), [float(x) for x in csv_list(args.cost_bps)]
    output = args.output.expanduser().resolve()
    if output.exists() and (output / "_SUCCESS").exists() and not args.force:
        raise FileExistsError(f"Output already exists and is complete: {output}")
    output.mkdir(parents=True, exist_ok=True)
    (output / "_SUCCESS").unlink(missing_ok=True)
    pending = output.with_name(f".{output.name}.pending")
    if pending.exists():
        shutil.rmtree(pending, ignore_errors=True)
    checkpoint_root = output / "_checkpoints" / "within_theme_factor"
    if args.reset_checkpoints and checkpoint_root.exists():
        shutil.rmtree(checkpoint_root, ignore_errors=True)
    checkpoint_root.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute(f"PRAGMA threads={max(1, args.threads)}")
    con.execute(f"SET memory_limit='{args.memory_limit_gb:g}GB'")
    if args.temp_directory:
        args.temp_directory.mkdir(parents=True, exist_ok=True)
        con.execute(f"SET temp_directory='{sql_path(args.temp_directory)}'")
    con.execute(f"CREATE VIEW signals AS SELECT * FROM {parquet_expr(args.signals, 'data.parquet')}")
    con.execute(f"CREATE VIEW labels AS SELECT * FROM {parquet_expr(args.labels, 'data.parquet')}")
    con.execute(f"CREATE VIEW memberships_raw AS SELECT * FROM {parquet_expr(args.memberships, 'memberships.parquet')}")
    signal_cols, label_cols, member_cols = columns(con, "signals"), columns(con, "labels"), columns(con, "memberships_raw")
    required = {
        "signals": {"batch_id", "factor_id", "trade_date", "decision_time", "symbol_id", "score"} - signal_cols,
        "labels": {"trade_date", "decision_time", "symbol_id", "label_id", target} - label_cols,
        "memberships": {"trade_date", "decision_time", "layer_id", "theme_id", "symbol_id", "theme_size", "canonical_eligible", "split_origin", "tree_depth"} - member_cols,
    }
    if any(required.values()):
        raise ValueError(f"Missing required columns: { {k: sorted(v) for k, v in required.items()} }")
    signal_pit = int(con.execute("SELECT count(*) FROM signals WHERE signal_available_time > decision_time").fetchone()[0]) if "signal_available_time" in signal_cols else 0
    if signal_pit:
        raise ValueError(f"Signal PIT audit failed: {signal_pit}")

    con.execute(f"""
        CREATE TEMP TABLE canonical_memberships AS
        SELECT CAST(trade_date AS VARCHAR) trade_date, decision_time theme_decision_time,
               CAST(symbol_id AS BIGINT) symbol_id, CAST(theme_id AS VARCHAR) theme_id,
               CAST(theme_size AS INTEGER) theme_size, CAST(tree_depth AS INTEGER) tree_depth
        FROM memberships_raw
        WHERE CAST(layer_id AS VARCHAR)={sql_literal(args.theme_layer_id)}
          AND canonical_eligible=true AND CAST(theme_size AS INTEGER)>={args.min_theme_size}
          AND lower(coalesce(CAST(split_origin AS VARCHAR), '')) NOT LIKE '%forced%'
        QUALIFY row_number() OVER (
          PARTITION BY trade_date, decision_time, symbol_id
          ORDER BY coalesce(tree_depth, 0) DESC, theme_size ASC, theme_id
        )=1
    """)
    audit_row = con.execute("""
        SELECT count(*), count(DISTINCT trade_date), count(DISTINCT theme_decision_time),
               count(DISTINCT symbol_id), count(DISTINCT theme_id), min(theme_size), median(theme_size), max(theme_size)
        FROM canonical_memberships
    """).fetchone()
    audit = {
        "theme_layer_id": args.theme_layer_id,
        "membership_rows": int(audit_row[0] or 0), "trade_dates": int(audit_row[1] or 0),
        "membership_decision_times": int(audit_row[2] or 0), "symbols": int(audit_row[3] or 0),
        "themes": int(audit_row[4] or 0), "min_theme_size": int(audit_row[5] or 0),
        "median_theme_size": float(audit_row[6] or 0), "max_theme_size": int(audit_row[7] or 0),
        "lag_policy": "latest canonical membership with theme_decision_time < signal decision_time",
        "signal_pit_violations": signal_pit,
    }
    if not audit["membership_rows"]:
        raise ValueError(f"No canonical memberships for {args.theme_layer_id}")

    key_columns = [c for c in ("batch_id", "factor_id", "layer_id", "scale_minutes", "horizon", "variant_id") if c in signal_cols]
    selected_keys = ", ".join(f'"{c}"' for c in key_columns)
    filters = [f"CAST(batch_id AS VARCHAR)={sql_literal(args.batch_id)}"]
    if factor_filter:
        filters.append("CAST(factor_id AS VARCHAR) IN (" + ",".join(sql_literal(x) for x in sorted(factor_filter)) + ")")
    if variant_filter and "variant_id" in signal_cols:
        filters.append("CAST(variant_id AS VARCHAR) IN (" + ",".join(sql_literal(x) for x in sorted(variant_filter)) + ")")
    factors = con.execute(f"SELECT DISTINCT {selected_keys} FROM signals WHERE {' AND '.join(filters)} ORDER BY {selected_keys}").fetch_df()
    if factors.empty:
        raise ValueError("No factors matched the requested filters")

    projection_cols = list(dict.fromkeys([*key_columns, "trade_date", "decision_time", "symbol_id", "score", *[c for c in controls if c in signal_cols]]))
    projection = ", ".join(f's."{c}"' for c in projection_cols)
    run_contract_hash = sha256_json(
        {
            "operation": "lagged_similarity_leiden_within_theme_alpha",
            "batch_id": args.batch_id,
            "signals": str(args.signals.resolve()),
            "labels": str(args.labels.resolve()),
            "label_contract": str(args.label_contract.resolve()),
            "label_contract_payload": contract,
            "memberships": str(args.memberships.resolve()),
            "label_id": label_id,
            "theme_layer_id": args.theme_layer_id,
            "factor_ids": sorted(factor_filter),
            "variant_ids": sorted(variant_filter),
            "default_direction": args.default_direction,
            "quantiles": args.quantiles,
            "min_theme_size": args.min_theme_size,
            "min_themes_per_decision": args.min_themes_per_decision,
            "control_columns": controls,
            "cost_bps": costs,
            "key_columns": key_columns,
        }
    )
    required_checkpoint_files = ("metrics.parquet", "ic_series.parquet", "portfolio_returns.parquet", "theme_returns.parquet")
    metric_rows, ic_frames, portfolio_frames, theme_frames = [], [], [], []
    units: list[dict[str, object]] = []
    completed = 0
    reused = 0
    write_progress(
        output,
        stage="within-theme-factor-checkpoints",
        total=len(factors),
        completed=0,
        current=None,
        units=units,
        extra={"checkpoint_contract_hash": run_contract_hash},
    )
    for _, factor_row in factors.iterrows():
        identity = {c: factor_row[c] for c in key_columns}
        unit_name = "|".join(f"{key}={identity[key]}" for key in key_columns)
        unit_dir = checkpoint_root / safe_key(identity, prefix="factor")
        checkpoint_spec = CheckpointSpec(
            "within-theme-factor",
            unit_name,
            run_contract_hash,
            sha256_json(identity),
        )
        write_progress(
            output,
            stage="within-theme-factor-checkpoints",
            total=len(factors),
            completed=completed,
            reused=reused,
            current=unit_name,
            units=units,
            extra={"checkpoint_contract_hash": run_contract_hash},
        )
        if checkpoint_valid(unit_dir, checkpoint_spec, required_files=required_checkpoint_files):
            loaded = load_checkpoint_frames(unit_dir, required_checkpoint_files)
            metric_frame = restore_checkpoint_frame(loaded["metrics.parquet"])
            ic = restore_checkpoint_frame(loaded["ic_series.parquet"])
            portfolio = restore_checkpoint_frame(loaded["portfolio_returns.parquet"])
            themes = restore_checkpoint_frame(loaded["theme_returns.parquet"])
            if not metric_frame.empty:
                metric_rows.extend(metric_frame.to_dict("records"))
            if not ic.empty:
                ic_frames.append(ic)
            if not portfolio.empty:
                portfolio_frames.append(portfolio)
            if not themes.empty:
                theme_frames.append(themes)
            completed += 1
            reused += 1
            units.append({"unit": unit_name, "status": "reused", "attempt": 0, "detail": str(unit_dir)})
            write_progress(
                output,
                stage="within-theme-factor-checkpoints",
                total=len(factors),
                completed=completed,
                reused=reused,
                current=None,
                units=units,
                extra={"checkpoint_contract_hash": run_contract_hash},
            )
            continue
        query = f"""
            WITH sl AS (
              SELECT {projection}, l."{target}"::DOUBLE target_return
              FROM signals s JOIN labels l
                ON s.trade_date=l.trade_date AND s.decision_time=l.decision_time AND s.symbol_id=l.symbol_id
              WHERE {factor_condition(key_columns, factor_row)}
                AND CAST(l.label_id AS VARCHAR)={sql_literal(label_id)}
            )
            SELECT sl.*, m.theme_id, m.theme_size, m.tree_depth, m.theme_decision_time
            FROM sl ASOF LEFT JOIN canonical_memberships m
              ON sl.symbol_id=m.symbol_id AND CAST(sl.trade_date AS VARCHAR)=m.trade_date
             AND sl.decision_time > m.theme_decision_time
            WHERE m.theme_id IS NOT NULL
            ORDER BY sl.trade_date, sl.decision_time, m.theme_id, sl.symbol_id
        """
        frame = con.execute(query).fetch_df()
        metric, ic, portfolio, themes = evaluate_factor(
            frame, identity, quantiles=args.quantiles, min_theme_size=args.min_theme_size,
            min_themes=args.min_themes_per_decision, direction_mode=args.default_direction,
            controls=controls, costs=costs,
        )
        metric_rows.append(metric)
        if not ic.empty:
            ic_frames.append(ic)
        if not portfolio.empty:
            portfolio_frames.append(portfolio)
        if not themes.empty:
            theme_frames.append(themes)
        commit_frames(
            unit_dir,
            checkpoint_spec,
            {
                "metrics.parquet": checkpoint_frame(pd.DataFrame([metric])),
                "ic_series.parquet": checkpoint_frame(ic),
                "portfolio_returns.parquet": checkpoint_frame(portfolio),
                "theme_returns.parquet": checkpoint_frame(themes),
            },
            metadata={"identity": identity, "label_id": label_id},
        )
        completed += 1
        units.append({"unit": unit_name, "status": "complete", "attempt": 1, "detail": str(unit_dir)})
        write_progress(
            output,
            stage="within-theme-factor-checkpoints",
            total=len(factors),
            completed=completed,
            reused=reused,
            current=None,
            units=units,
            extra={"checkpoint_contract_hash": run_contract_hash},
        )

    metrics = pd.DataFrame(metric_rows)
    if "spearman_ic_pvalue_daily" in metrics:
        metrics["fdr_qvalue"] = bh_fdr(metrics["spearman_ic_pvalue_daily"])
        metrics["fdr_pass"] = metrics["fdr_qvalue"] <= 0.05
        metrics["governance_ready"] = metrics["direction_predeclared"].fillna(False) & metrics["theme_lag_safe"].fillna(False) & (not bool(contract.get("overlapping", True)))
        metrics["research_status"] = np.select(
            [metrics["sample_sufficient"] & metrics["fdr_pass"] & metrics["cost_survives_5bps"] & metrics["governance_ready"], metrics["sample_sufficient"]],
            ["candidate", "needs_falsification"], default="insufficient_or_rejected",
        )
    ic = pd.concat(ic_frames, ignore_index=True) if ic_frames else pd.DataFrame()
    portfolios = pd.concat(portfolio_frames, ignore_index=True) if portfolio_frames else pd.DataFrame()
    theme_returns = pd.concat(theme_frames, ignore_index=True) if theme_frames else pd.DataFrame()
    for name, frame in (("metrics", metrics), ("ic_series", ic), ("portfolio_returns", portfolios), ("theme_returns", theme_returns)):
        write_frame(frame, output, name)
    manifest = {
        "operation": "lagged_similarity_leiden_within_theme_alpha", "batch_id": args.batch_id,
        "signals": str(args.signals.resolve()), "labels": str(args.labels.resolve()),
        "label_contract": str(args.label_contract.resolve()), "memberships": str(args.memberships.resolve()),
        "label_id": label_id, "theme_layer_id": args.theme_layer_id,
        "factor_ids": sorted(factor_filter), "variant_ids": sorted(variant_filter),
        "default_direction": args.default_direction, "quantiles": args.quantiles,
        "min_theme_size": args.min_theme_size, "min_themes_per_decision": args.min_themes_per_decision,
        "control_columns": controls, "cost_bps": costs, "membership_audit": audit,
        "factor_count": int(len(metrics)), "decision_rows": int(len(portfolios)),
        "theme_decision_rows": int(len(theme_returns)),
        "checkpoint_granularity": "factor",
        "checkpoint_count": int(len(factors)),
        "checkpoint_reused": int(reused),
        "checkpoint_contract_hash": run_contract_hash,
    }
    atomic_write_json(output / "run_manifest.json", manifest)
    atomic_write_text(
        output / "SUMMARY.md",
        "# Lagged Similarity-Leiden Within-Theme Alpha\n\n```json\n" + json.dumps(audit, indent=2) + "\n```\n\n```csv\n" + metrics.to_csv(index=False) + "```\n",
    )
    atomic_write_json(output / "_SUCCESS", {"status": "success", "factor_count": len(metrics), "checkpoint_contract_hash": run_contract_hash})
    write_progress(
        output,
        stage="within-theme-factor-checkpoints",
        total=len(factors),
        completed=completed,
        reused=reused,
        current=None,
        status="complete",
        units=units,
        extra={"checkpoint_contract_hash": run_contract_hash, "report_success": True},
    )
    con.close()
    print(json.dumps(manifest, indent=2, default=str))


if __name__ == "__main__":
    main()
