from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json, math, os, shutil
from typing import Iterable

import duckdb
import numpy as np
import pandas as pd
from scipy import stats

from .checkpoint import CheckpointSpec, checkpoint_valid, commit_frames, write_progress
from .contracts import LabelContract
from .dual_theme_common import DUAL_THEME_BATCH_ID, load_gff_campaign_contract
from .governance import atomic_write_frame, atomic_write_json, atomic_write_text, file_record, sha256_file, sha256_json


@dataclass(frozen=True)
class DailyLabelSpec:
    label_id: str
    entry_offset: int
    entry_point: str
    exit_offset: int
    exit_point: str

    @property
    def nominal_minutes(self) -> int:
        if self.entry_offset == self.exit_offset:
            return 390
        return max(1, self.exit_offset - self.entry_offset) * 390


CORE_DAILY_LABELS = (
    DailyLabelSpec("daily_n_close_to_n1_open", 0, "close", 1, "open"),
    DailyLabelSpec("daily_n_close_to_n1_close", 0, "close", 1, "close"),
    DailyLabelSpec("daily_n1_open_to_n1_close", 1, "open", 1, "close"),
)
EXTENDED_DAILY_LABELS = CORE_DAILY_LABELS + (
    DailyLabelSpec("daily_n_close_to_n3_close", 0, "close", 3, "close"),
    DailyLabelSpec("daily_n_close_to_n5_close", 0, "close", 5, "close"),
    DailyLabelSpec("daily_n_close_to_n7_close", 0, "close", 7, "close"),
)


def _sql(path: Path) -> str:
    return path.expanduser().resolve().as_posix().replace("'", "''")


def _lit(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _specs(profile: str) -> tuple[DailyLabelSpec, ...]:
    if profile == "core":
        return CORE_DAILY_LABELS
    if profile == "extended":
        return EXTENDED_DAILY_LABELS
    raise ValueError("profile must be core or extended")


def _bar_files(root: Path) -> dict[str, list[Path]]:
    rows: dict[str, list[Path]] = {}
    for date_root in sorted(root.glob("date=*")):
        files = sorted(date_root.rglob("part-*.parquet"))
        if files:
            rows[date_root.name.split("=", 1)[1]] = files
    if not rows:
        raise FileNotFoundError(f"No bars below {root}")
    return rows


def _point(alias: str, name: str) -> tuple[str, str, str]:
    prefix = "rth_open" if name == "open" else "rth_close"
    return f"{alias}.{prefix}_time", f"{alias}.{prefix}_price", f"{alias}.{prefix}_available_time"


def _contract(spec: DailyLabelSpec) -> LabelContract:
    return LabelContract(
        label_id=spec.label_id,
        horizon_minutes=spec.nominal_minutes,
        entry_lag_minutes=0,
        rebalance_minutes=390,
        horizon_tolerance_seconds=21 * 24 * 3600,
        horizon_unit="trading_sessions",
        entry_session_offset=spec.entry_offset,
        exit_session_offset=spec.exit_offset,
        entry_point=spec.entry_point,
        exit_point=spec.exit_point,
        tail_policy="allow_truncated_tail",
    )


def build_daily_labels(
    *,
    gff_campaign_root: str | Path,
    signals_root: str | Path,
    bars_root: str | Path,
    output_root: str | Path,
    date_count: int = 70,
    profile: str = "core",
    end_date: str | None = None,
    threads: int = 8,
    memory_limit_gb: float = 32,
    temp_directory: str | Path | None = None,
) -> Path:
    campaign = load_gff_campaign_contract(gff_campaign_root)
    dates = sorted(str(x) for x in campaign["dates"])
    if end_date:
        dates = [x for x in dates if x <= end_date]
    if len(dates) < date_count:
        raise ValueError(f"Need {date_count} campaign dates, found {len(dates)}")
    dates = dates[-date_count:]
    specs = _specs(profile)
    bars = Path(bars_root).resolve()
    signals = Path(signals_root).resolve()
    output = Path(output_root).resolve()
    parts = output / "_checkpoints" / "daily_labels"
    labels_root, contracts_root, diagnostics = output / "labels", output / "contracts", output / "diagnostics"
    for path in (parts, labels_root, contracts_root, diagnostics):
        path.mkdir(parents=True, exist_ok=True)
    files = _bar_files(bars)
    missing = [x for x in dates if x not in files]
    if missing:
        raise FileNotFoundError(f"Missing base-date bars: {missing}")
    all_dates = sorted(files)
    first, last = all_dates.index(dates[0]), all_dates.index(dates[-1])
    max_offset = max(x.exit_offset for x in specs)
    support = all_dates[first : min(len(all_dates), last + max_offset + 1)]
    global_signals = signals / f"batch_id={DUAL_THEME_BATCH_ID}" / "scope=global"
    export_manifest = signals / "export_manifest.json"
    if not global_signals.exists() or not export_manifest.exists() or not (signals / "_SUCCESS").exists():
        raise FileNotFoundError(f"Incomplete governed signal export: {signals}")
    contract_hash = sha256_json({
        "operation": "dual_theme_daily_labels_v3",
        "code": sha256_file(Path(__file__)),
        "campaign": file_record(campaign["path"]),
        "signals": file_record(export_manifest),
        "dates": dates,
        "specs": [x.__dict__ for x in specs],
        "rth": "America/New_York 09:30-16:00",
        "tail": "trailing_missing_future_sessions_allowed",
    })
    con = duckdb.connect()
    con.execute(f"PRAGMA threads={max(1, threads)}")
    con.execute(f"SET memory_limit='{memory_limit_gb:g}GB'")
    if temp_directory:
        Path(temp_directory).mkdir(parents=True, exist_ok=True)
        con.execute(f"SET temp_directory='{_sql(Path(temp_directory))}'")
    date_sql = ",".join(_lit(x) for x in support)
    selected_sql = ",".join(_lit(x) for x in dates)
    con.execute(f"""
      CREATE VIEW bars AS SELECT CAST(trade_date AS VARCHAR) trade_date, CAST(symbol_id AS BIGINT) symbol_id,
        CAST(timestamp AS TIMESTAMPTZ) timestamp, CAST(available_time AS TIMESTAMPTZ) available_time,
        CAST(open AS DOUBLE) open, CAST(close AS DOUBLE) close
      FROM read_parquet('{_sql(bars / 'date=*' / 'part-*.parquet')}', union_by_name=true, hive_partitioning=false)
      WHERE CAST(trade_date AS VARCHAR) IN ({date_sql}) AND symbol_id IS NOT NULL AND open IS NOT NULL AND close IS NOT NULL
    """)
    con.execute("""
      CREATE VIEW rth AS SELECT * FROM bars
      WHERE CAST(timezone('America/New_York', timestamp) AS TIME) BETWEEN TIME '09:30:00' AND TIME '16:00:00'
    """)
    con.execute("""
      CREATE VIEW ohlc AS SELECT trade_date, symbol_id,
        min(timestamp) rth_open_time, max(timestamp) rth_close_time,
        arg_min(open,timestamp) rth_open_price, arg_max(close,timestamp) rth_close_price,
        arg_min(available_time,timestamp) rth_open_available_time,
        arg_max(available_time,timestamp) rth_close_available_time
      FROM rth GROUP BY trade_date,symbol_id
    """)
    con.execute("CREATE VIEW calendar AS SELECT trade_date,row_number() OVER(ORDER BY trade_date)-1 session_index FROM (SELECT DISTINCT trade_date FROM ohlc)")
    pattern = _sql(global_signals / "**" / "data.parquet")
    con.execute(f"""
      CREATE VIEW decisions AS SELECT DISTINCT CAST(trade_date AS VARCHAR) trade_date,
        CAST(decision_time AS TIMESTAMPTZ) decision_time, CAST(symbol_id AS BIGINT) symbol_id
      FROM read_parquet('{pattern}', union_by_name=true, hive_partitioning=false)
      WHERE CAST(trade_date AS VARCHAR) IN ({selected_sql})
    """)
    states, completed, reused = [], 0, 0
    write_progress(diagnostics, stage="daily-label-date", total=len(dates), completed=0, units=[])
    try:
        for base_date in dates:
            base_index = support.index(base_date)
            support_dates = support[base_index : base_index + max_offset + 1]
            source_records = [file_record(p) for d in support_dates for p in files[d]]
            spec_cp = CheckpointSpec("daily-label-date", base_date, contract_hash, sha256_json(source_records))
            target = parts / f"date={base_date}"
            state = {"unit": base_date, "status": "pending", "attempt": 0, "detail": str(target)}
            states.append(state)
            if checkpoint_valid(target, spec_cp, required_files=("data.parquet",)):
                state["status"] = "reused"; completed += 1; reused += 1
                write_progress(diagnostics, stage="daily-label-date", total=len(dates), completed=completed, reused=reused, current=base_date, units=states)
                continue
            queries = []
            for label in specs:
                et, ep, ea = _point("entry", label.entry_point); xt, xp, xa = _point("exit", label.exit_point)
                queries.append(f"""SELECT d.trade_date,d.decision_time,d.symbol_id,{_lit(label.label_id)} label_id,
                  {et} entry_time,{xt} exit_time,greatest({ea},{xa}) label_available_time,
                  {xp}/nullif({ep},0)-1 target_return,{label.entry_offset} entry_session_offset,
                  {label.exit_offset} exit_session_offset,{_lit(label.entry_point)} entry_point,{_lit(label.exit_point)} exit_point
                  FROM decisions d JOIN calendar b ON b.trade_date=d.trade_date
                  JOIN calendar ec ON ec.session_index=b.session_index+{label.entry_offset}
                  JOIN calendar xc ON xc.session_index=b.session_index+{label.exit_offset}
                  JOIN ohlc entry ON entry.trade_date=ec.trade_date AND entry.symbol_id=d.symbol_id
                  JOIN ohlc exit ON exit.trade_date=xc.trade_date AND exit.symbol_id=d.symbol_id
                  WHERE d.trade_date={_lit(base_date)} AND {et}>d.decision_time AND {xt}>{et}""")
            tmp = target.parent / ("." + target.name + ".part.parquet")
            tmp.unlink(missing_ok=True)
            con.execute("COPY (" + " UNION ALL ".join(queries) + f") TO '{_sql(tmp)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            frame = con.execute(f"SELECT * FROM read_parquet('{_sql(tmp)}')").fetch_df()
            tmp.unlink(missing_ok=True)
            commit_frames(target, spec_cp, {"data.parquet": frame}, metadata={"base_date": base_date, "sources": source_records})
            state["status"] = "complete"; state["detail"] = f"{len(frame)} rows"; completed += 1
            write_progress(diagnostics, stage="daily-label-date", total=len(dates), completed=completed, reused=reused, current=base_date, units=states)
        parts_pattern = _sql(parts / "date=*" / "data.parquet")
        coverage = []
        horizon_rows = []
        for label in specs:
            target = labels_root / f"label_id={label.label_id}"
            pending = labels_root / f".{target.name}.{os.getpid()}.pending"
            shutil.rmtree(pending, ignore_errors=True); pending.mkdir(parents=True)
            dest = pending / "data.parquet"
            con.execute(f"COPY (SELECT * FROM read_parquet('{parts_pattern}', union_by_name=true, hive_partitioning=false) WHERE label_id={_lit(label.label_id)}) TO '{_sql(dest)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            stats_row = con.execute(f"SELECT count(*),count(DISTINCT trade_date),min(trade_date),max(trade_date) FROM read_parquet('{_sql(dest)}')").fetchone()
            if target.exists(): shutil.rmtree(target)
            os.replace(pending, target)
            contract = _contract(label)
            contract_path = contracts_root / f"{label.label_id}.json"
            atomic_write_json(contract_path, contract.as_dict())
            available = con.execute(f"SELECT DISTINCT trade_date FROM read_parquet('{_sql(dest)}') ORDER BY trade_date").fetch_df()["trade_date"].astype(str).tolist()
            missing_dates = [x for x in dates if x not in set(available)]
            interior = [x for x in missing_dates if available and x < max(available)]
            if interior: raise RuntimeError(f"Interior daily-label gaps for {label.label_id}: {interior}")
            coverage.append({"label_id": label.label_id, "rows": int(stats_row[0]), "available_date_count": int(stats_row[1]), "first_available_date": stats_row[2], "last_available_date": stats_row[3], "tail_missing_date_count": len(missing_dates), "tail_missing_dates": ",".join(missing_dates)})
            horizon_rows.append({"name": label.label_id, "labels": str(target), "label_contract": str(contract_path)})
        atomic_write_frame(pd.DataFrame(coverage), diagnostics / "daily_label_coverage.csv")
        manifest = {"version": "GAL_DAILY_LABELS_V3", "analysis_dates": dates, "date_count": len(dates), "profile": profile, "tail_policy": "allow_truncated_tail", "labels": coverage, "horizon_manifest": str(output / "daily_horizon_manifest.json")}
        atomic_write_json(diagnostics / "daily_label_manifest.json", manifest)
        atomic_write_json(output / "daily_horizon_manifest.json", {"horizons": horizon_rows})
        atomic_write_json(output / "_SUCCESS", {"status": "complete", "contract_hash": contract_hash})
        write_progress(diagnostics, stage="daily-label-date", total=len(dates), completed=len(dates), reused=reused, status="complete", units=states)
        return output / "daily_horizon_manifest.json"
    finally:
        con.close()


def _ttest(values: pd.Series) -> tuple[float, float]:
    x = pd.to_numeric(values, errors="coerce").dropna()
    if len(x) < 2 or x.std(ddof=1) <= 0: return np.nan, np.nan
    t = float(x.mean() / (x.std(ddof=1) / math.sqrt(len(x))))
    return t, float(2 * stats.t.sf(abs(t), len(x)-1))


def _max_dd(values: pd.Series) -> float:
    x = pd.to_numeric(values, errors="coerce").dropna().to_numpy(float)
    if len(x) == 0: return np.nan
    wealth = np.cumprod(1+x); peak = np.maximum.accumulate(wealth)
    return float(np.min(wealth/peak-1))


def run_rolling_alpha(
    report_root: str | Path,
    output_root: str | Path,
    *,
    label_manifest: str | Path | None = None,
    windows: Iterable[int] = (20, 40, 60),
    step: int = 5,
    min_coverage_ratio: float = 0.8,
    min_direction_train_dates: int = 20,
    direction_rolling_dates: int = 60,
) -> Path:
    report, output = Path(report_root).resolve(), Path(output_root).resolve()
    output.mkdir(parents=True, exist_ok=True)
    labels = json.loads(Path(label_manifest).read_text()) if label_manifest else {}
    profiles = {str(x["label_id"]): x for x in labels.get("labels", [])}
    analysis_dates = [str(x) for x in labels.get("analysis_dates", [])]
    all_rows = []
    for horizon_dir in sorted(report.glob("horizon=*")):
        if not (horizon_dir / "_SUCCESS").exists(): continue
        horizon = horizon_dir.name.split("=",1)[1]
        daily_ic = pd.read_csv(horizon_dir / "daily_ic.csv")
        portfolio = pd.read_csv(horizon_dir / "portfolio_returns.csv")
        metrics = pd.read_csv(horizon_dir / "alpha_metrics.csv")
        keys = [x for x in ("batch_id","factor_id","layer_id","scale_minutes","variant_id","scope","theme_family") if x in daily_ic.columns or x in portfolio.columns]
        ic = daily_ic.groupby([*keys,"trade_date"], observed=True, dropna=False)["spearman_ic"].mean().reset_index()
        port = portfolio.groupby([*keys,"trade_date"], observed=True, dropna=False).agg(raw_return=("raw_top_minus_bottom_return","mean"),turnover=("turnover","mean")).reset_index()
        daily = ic.merge(port, on=[*keys,"trade_date"], how="outer")
        dates = analysis_dates or sorted(daily["trade_date"].astype(str).unique())
        profile = profiles.get(horizon, {})
        for factor_values, factor in daily.groupby(keys, observed=True, dropna=False):
            identity = dict(zip(keys, factor_values if isinstance(factor_values, tuple) else (factor_values,)))
            metric = metrics.copy()
            for key,value in identity.items():
                if key in metric: metric = metric[metric[key].isna()] if pd.isna(value) else metric[metric[key] == value]
            pre = None
            if not metric.empty and bool(metric.iloc[0].get("direction_predeclared", False)):
                value = pd.to_numeric(pd.Series([metric.iloc[0].get("expected_direction")]), errors="coerce").iloc[0]
                pre = int(np.sign(value)) if pd.notna(value) and value != 0 else None
            factor = factor.copy(); factor["trade_date"] = factor["trade_date"].astype(str); by_date = factor.set_index("trade_date")
            for window in sorted(set(int(x) for x in windows if int(x)>0)):
                for end_pos in range(window-1, len(dates), max(1,step)):
                    wd = dates[end_pos-window+1:end_pos+1]; observed = by_date.loc[[x for x in wd if x in by_date.index]].reset_index()
                    coverage = len(observed)/window
                    train_dates = [x for x in dates[:end_pos-window+1] if x in by_date.index][-direction_rolling_dates:]
                    train_ic = pd.to_numeric(by_date.loc[train_dates]["spearman_ic"], errors="coerce").dropna() if train_dates else pd.Series(dtype=float)
                    direction, source = pre, "predeclared" if pre else "unavailable"
                    if direction is None and len(train_ic) >= min_direction_train_dates:
                        direction = 1 if train_ic.mean() >= 0 else -1; source = "prior_daily_ic"
                    ic_values = pd.to_numeric(observed["spearman_ic"], errors="coerce").dropna()
                    returns = observed[["raw_return","turnover"]].apply(pd.to_numeric, errors="coerce").dropna(subset=["raw_return"])
                    oriented = returns["raw_return"] * direction if direction in (-1,1) else pd.Series(dtype=float)
                    t,p = _ttest(ic_values)
                    row = {**identity,"horizon":horizon,"window_sessions":window,"window_start":wd[0],"window_end":wd[-1],"expected_sessions":window,"observed_sessions":len(observed),"coverage_ratio":coverage,"coverage_pass":coverage>=min_coverage_ratio,"direction":direction if direction else np.nan,"direction_source":source,"direction_train_dates":len(train_ic),"mean_spearman_ic":ic_values.mean() if len(ic_values) else np.nan,"spearman_ic_tstat":t,"spearman_ic_pvalue":p,"pit_oriented_gross_mean":oriented.mean() if len(oriented) else np.nan,"mean_turnover":returns["turnover"].mean() if len(returns) else np.nan,"pit_oriented_hit_rate":(oriented>0).mean() if len(oriented) else np.nan,"pit_oriented_max_drawdown":_max_dd(oriented),"label_last_available_date":profile.get("last_available_date"),"label_tail_missing_dates":profile.get("tail_missing_dates","")}
                    for bps in (0,1,2,5,10): row[f"pit_net_mean_{bps}bps"] = (oriented - returns["turnover"].fillna(0)*bps/10000).mean() if len(oriented) else np.nan
                    row["rolling_status"] = "complete" if row["coverage_pass"] and source != "unavailable" else "insufficient_direction_history" if source == "unavailable" else "insufficient_coverage"
                    all_rows.append(row)
    result = pd.DataFrame(all_rows)
    if result.empty: raise ValueError("No completed horizon reports available for rolling Alpha")
    atomic_write_frame(result, output / "rolling_alpha_metrics.csv")
    graph = result[result["variant_id"] == "graph_forward"] if "variant_id" in result else result
    summary_keys = [x for x in ("scope","theme_family","horizon","window_sessions","rolling_status") if x in graph.columns]
    summary = graph.groupby(summary_keys, observed=True, dropna=False).agg(factor_rows=("factor_id","size"),mean_abs_ic=("mean_spearman_ic",lambda x: pd.to_numeric(x,errors="coerce").abs().mean()),mean_net_5bps=("pit_net_mean_5bps","mean"),mean_turnover=("mean_turnover","mean"),mean_coverage=("coverage_ratio","mean")).reset_index()
    atomic_write_frame(summary, output / "rolling_scope_summary.csv")
    stability_keys = [x for x in ("factor_id","scope","theme_family","horizon","window_sessions") if x in result.columns]
    stability = result.groupby(stability_keys, observed=True, dropna=False).agg(window_count=("window_end","size"),ic_mean=("mean_spearman_ic","mean"),ic_std=("mean_spearman_ic","std"),ic_positive_rate=("mean_spearman_ic",lambda x:(pd.to_numeric(x,errors="coerce")>0).mean()),net_5bps_mean=("pit_net_mean_5bps","mean"),complete_rate=("rolling_status",lambda x:(x.astype(str)=="complete").mean())).reset_index()
    atomic_write_frame(stability, output / "rolling_stability_summary.csv")
    payload = {"version":"GAL_ROLLING_ALPHA_V1","windows":sorted(set(int(x) for x in windows)),"step":step,"row_count":len(result),"complete_rows":int((result["rolling_status"]=="complete").sum()),"tail_policy":"use only labels actually available; never synthesize future returns"}
    atomic_write_json(output / "summary.json", payload)
    atomic_write_text(output / "REPORT.md", "# Rolling Alpha report\n\n" + "\n".join(f"- {k}: {v}" for k,v in payload.items()) + "\n")
    atomic_write_json(output / "_SUCCESS", {"status":"complete","summary":payload})
    return output
