from __future__ import annotations

from collections import deque
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import shutil
from typing import Any, Iterable

import duckdb
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from .checkpoint import CheckpointSpec, checkpoint_valid, commit_frames, write_progress
from .contracts import LabelContract
from .dual_theme_global_dag import _healable_pool_run, run_dual_theme_alpha_campaign
from .dual_theme_resumable import stable_export_manifest_record
from .governance import (
    ResourceBudget,
    atomic_write_frame,
    atomic_write_json,
    atomic_write_text,
    configure_duckdb,
    enforce_git_lineage,
    file_record,
    implementation_manifest,
    sha256_file,
    sha256_json,
)
from .streaming import _table_expression


FORWARD_VOLATILITY_VERSION = "GAL_FORWARD_VOLATILITY_V1"
DEFAULT_HORIZONS = (30, 60, 120, 180)
DEFAULT_TARGETS = (
    "forward_log_rv",
    "forward_vol_expansion",
    "forward_downside_semivar",
    "forward_downside_share",
    "forward_jump_var",
    "forward_jump_share",
    "forward_max_abs_return",
    "forward_max_drawdown",
    "forward_tail_event",
)
LABEL_DATA_FILE = "data.parquet"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sql_path(path: str | Path) -> str:
    return Path(path).expanduser().resolve().as_posix().replace("'", "''")


def _json_read(path: str | Path) -> dict[str, Any]:
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return value


def _cache_read(path: Path, key: str) -> Any | None:
    try:
        payload = _json_read(path)
    except (OSError, json.JSONDecodeError, ValueError):
        return None
    return payload.get("value") if payload.get("key") == key else None


def _cache_write(path: Path, key: str, value: Any) -> None:
    atomic_write_json(path, {"key": key, "value": value, "written_at": _utc_now()})


@dataclass(frozen=True)
class ForwardVolatilityConfig:
    horizons: tuple[int, ...] = DEFAULT_HORIZONS
    targets: tuple[str, ...] = DEFAULT_TARGETS
    entry_lag_minutes: int = 1
    min_coverage: float = 0.90
    epsilon: float = 1e-12
    tail_quantile: float = 0.90

    def validate(self) -> None:
        if not self.horizons or any(int(value) <= 0 for value in self.horizons):
            raise ValueError("horizons must contain positive minutes")
        unknown = sorted(set(self.targets) - set(DEFAULT_TARGETS))
        if unknown:
            raise ValueError(f"Unknown forward-volatility targets: {unknown}")
        if self.entry_lag_minutes <= 0:
            raise ValueError("entry_lag_minutes must be positive")
        if not 0 < self.min_coverage <= 1:
            raise ValueError("min_coverage must be in (0, 1]")
        if not 0 < self.tail_quantile < 1:
            raise ValueError("tail_quantile must be in (0, 1)")

    def as_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["horizons"] = list(self.horizons)
        payload["targets"] = list(self.targets)
        return payload


@dataclass(frozen=True)
class LabelDateTask:
    trade_date: str
    gff_core: str
    decisions: str
    labels_root: str
    contract_hash: str
    config: dict[str, object]

    @property
    def checkpoint_dir(self) -> Path:
        return Path(self.labels_root) / f"trade_date={self.trade_date}"


def _render_template(template: str, trade_date: str) -> Path:
    date = pd.Timestamp(trade_date).date()
    return Path(
        template.format(
            date=date.isoformat(),
            yyyymmdd=date.strftime("%Y%m%d"),
            year=date.strftime("%Y"),
            month=date.strftime("%m"),
        )
    ).expanduser().resolve()


def _global_signal_root(signals_root: Path, batch_id: str) -> Path:
    exact = signals_root / f"batch_id={batch_id}" / "scope=global"
    if exact.exists():
        return exact
    matches = sorted(signals_root.rglob(f"batch_id={batch_id}/scope=global"))
    if len(matches) != 1:
        raise FileNotFoundError(
            f"Expected one Global signal path for batch={batch_id}; found={matches}"
        )
    return matches[0]


def _decision_inventory(
    signals_root: Path,
    batch_id: str,
    output_root: Path,
    *,
    start_date: str | None,
    end_date: str | None,
    export_anchor: dict[str, object],
    resource_budget: ResourceBudget,
) -> list[dict[str, str]]:
    preflight = output_root / "_checkpoints" / "forward_volatility" / "preflight"
    cache = preflight / "decision_inventory.json"
    key = sha256_json(
        {
            "kind": "forward-volatility-decision-inventory-v1",
            "signals": export_anchor,
            "start_date": start_date,
            "end_date": end_date,
        }
    )
    cached = _cache_read(cache, key)
    if isinstance(cached, list) and all(
        Path(str(row.get("decisions", ""))).exists() for row in cached
    ):
        return [dict(row) for row in cached]

    source_root = _global_signal_root(signals_root, batch_id)
    connection = duckdb.connect()
    try:
        configure_duckdb(connection, resource_budget)
        source = _table_expression(source_root)
        clauses: list[str] = []
        if start_date:
            clauses.append(f"CAST(trade_date AS DATE) >= DATE '{start_date}'")
        if end_date:
            clauses.append(f"CAST(trade_date AS DATE) <= DATE '{end_date}'")
        where = "WHERE " + " AND ".join(clauses) if clauses else ""
        dates = [
            str(row[0])
            for row in connection.execute(
                f"SELECT DISTINCT CAST(trade_date AS VARCHAR) FROM {source} {where} ORDER BY 1"
            ).fetchall()
        ]
        result: list[dict[str, str]] = []
        for trade_date in dates:
            destination = preflight / "decisions" / f"trade_date={trade_date}" / LABEL_DATA_FILE
            marker = destination.parent / "cache.json"
            day_key = sha256_json(
                {
                    "kind": "forward-volatility-decisions-date-v1",
                    "signals": export_anchor,
                    "trade_date": trade_date,
                }
            )
            day_cached = _cache_read(marker, day_key)
            if day_cached is None or not destination.exists():
                destination.parent.mkdir(parents=True, exist_ok=True)
                temporary = destination.with_name(f".{destination.name}.{os.getpid()}.part")
                temporary.unlink(missing_ok=True)
                connection.execute(
                    f"""
                    COPY (
                      SELECT DISTINCT
                        CAST(trade_date AS VARCHAR) AS trade_date,
                        decision_time,
                        CAST(symbol_id AS BIGINT) AS symbol_id
                      FROM {source}
                      WHERE CAST(trade_date AS VARCHAR)='{trade_date}'
                      ORDER BY decision_time, symbol_id
                    ) TO '{_sql_path(temporary)}'
                    (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 250000)
                    """
                )
                os.replace(temporary, destination)
                _cache_write(
                    marker,
                    day_key,
                    {
                        "rows": int(pq.ParquetFile(destination).metadata.num_rows),
                        "sha256": sha256_file(destination),
                    },
                )
            result.append({"trade_date": trade_date, "decisions": str(destination)})
    finally:
        connection.close()
    _cache_write(cache, key, result)
    return result


def _interval_sum(cumulative: np.ndarray, left: int, right: int) -> float:
    return float(cumulative[right] - cumulative[left]) if right > left else 0.0


def _build_symbol_rows(
    frame: pd.DataFrame,
    decisions: np.ndarray,
    config: ForwardVolatilityConfig,
    *,
    trade_date: str,
    symbol_id: int,
) -> list[dict[str, object]]:
    frame = frame.sort_values("timestamp")
    timestamps = pd.to_datetime(frame["timestamp"], utc=True).astype("int64").to_numpy()
    raw = pd.to_numeric(frame["ret_1m"], errors="coerce").to_numpy(float)
    valid = np.isfinite(raw) & (raw > -1.0)
    returns = np.zeros(len(raw), dtype=float)
    returns[valid] = np.log1p(raw[valid])
    squared = returns * returns
    downside = np.where(returns < 0, squared, 0.0)
    bipower = np.zeros(len(returns), dtype=float)
    if len(returns) > 1:
        pair_valid = valid[1:] & valid[:-1]
        bipower[1:] = np.where(
            pair_valid,
            (math.pi / 2.0) * np.abs(returns[1:]) * np.abs(returns[:-1]),
            0.0,
        )
    cumulative_rv = np.r_[0.0, np.cumsum(squared)]
    cumulative_down = np.r_[0.0, np.cumsum(downside)]
    cumulative_bv = np.r_[0.0, np.cumsum(bipower)]
    cumulative_count = np.r_[0, np.cumsum(valid.astype(np.int64))]
    minute_ns = int(pd.Timedelta(minutes=1).value)
    rows: list[dict[str, object]] = []
    for raw_decision in decisions:
        decision_ns = int(raw_decision)
        decision_time = pd.Timestamp(decision_ns, tz="UTC")
        for horizon in config.horizons:
            horizon = int(horizon)
            entry_ns = decision_ns + config.entry_lag_minutes * minute_ns
            exit_ns = entry_ns + horizon * minute_ns
            future_left = int(np.searchsorted(timestamps, entry_ns, side="right"))
            future_right = int(np.searchsorted(timestamps, exit_ns, side="right"))
            past_left = int(
                np.searchsorted(timestamps, decision_ns - horizon * minute_ns, side="right")
            )
            past_right = int(np.searchsorted(timestamps, decision_ns, side="right"))
            future_count = int(cumulative_count[future_right] - cumulative_count[future_left])
            past_count = int(cumulative_count[past_right] - cumulative_count[past_left])
            required = int(math.ceil(horizon * config.min_coverage))
            if future_count < required or past_count < required:
                continue
            if future_right <= future_left or timestamps[future_right - 1] < exit_ns:
                continue
            future_rv = _interval_sum(cumulative_rv, future_left, future_right)
            past_rv = _interval_sum(cumulative_rv, past_left, past_right)
            future_down = _interval_sum(cumulative_down, future_left, future_right)
            future_bv = _interval_sum(cumulative_bv, future_left, future_right)
            future_jump = max(future_rv - future_bv, 0.0)
            path = np.cumsum(returns[future_left:future_right])
            wealth = np.exp(path)
            peak = np.maximum.accumulate(np.r_[1.0, wealth])
            drawdown = np.r_[1.0, wealth] / peak - 1.0
            rows.append(
                {
                    "trade_date": trade_date,
                    "decision_time": decision_time,
                    "symbol_id": int(symbol_id),
                    "label_id": f"forward_volatility_{horizon}m_v1",
                    "entry_time": pd.Timestamp(entry_ns, tz="UTC"),
                    "exit_time": pd.Timestamp(exit_ns, tz="UTC"),
                    "label_available_time": pd.Timestamp(exit_ns, tz="UTC"),
                    "horizon_minutes": horizon,
                    "entry_lag_minutes": config.entry_lag_minutes,
                    "future_observation_count": future_count,
                    "past_observation_count": past_count,
                    "past_rv": past_rv,
                    "future_rv": future_rv,
                    "forward_log_rv": math.log(future_rv + config.epsilon),
                    "forward_vol_expansion": math.log(
                        (future_rv + config.epsilon) / (past_rv + config.epsilon)
                    ),
                    "forward_downside_semivar": future_down,
                    "forward_downside_share": future_down / (future_rv + config.epsilon),
                    "forward_jump_var": future_jump,
                    "forward_jump_share": future_jump / (future_rv + config.epsilon),
                    "forward_max_abs_return": float(np.max(np.abs(path))),
                    "forward_max_drawdown": float(max(0.0, -np.min(drawdown))),
                    "label_status": "complete",
                }
            )
    return rows


def _label_spec(task: LabelDateTask) -> CheckpointSpec:
    return CheckpointSpec(
        stage="forward-volatility-label-date",
        unit=task.trade_date,
        contract_hash=task.contract_hash,
        source_hash=sha256_json(
            {
                "gff_core": file_record(task.gff_core),
                "decisions": file_record(task.decisions),
                "config": task.config,
            }
        ),
    )


def _run_label_task(task: LabelDateTask) -> dict[str, object]:
    spec = _label_spec(task)
    if checkpoint_valid(task.checkpoint_dir, spec, required_files=(LABEL_DATA_FILE,)):
        return {"unit": task.trade_date, "status": "reused"}
    config = ForwardVolatilityConfig(**task.config)
    config.validate()
    decisions = pd.read_parquet(task.decisions)
    available = set(pq.ParquetFile(task.gff_core).schema_arrow.names)
    missing = sorted({"timestamp", "symbol_id", "ret_1m"} - available)
    if missing:
        raise ValueError(f"NFF gff_core missing required columns: {missing}")
    market = pq.read_table(
        task.gff_core, columns=["timestamp", "symbol_id", "ret_1m"]
    ).to_pandas()
    market["timestamp"] = pd.to_datetime(market["timestamp"], utc=True)
    decisions["decision_time"] = pd.to_datetime(decisions["decision_time"], utc=True)
    decision_map = {
        int(symbol_id): group["decision_time"].astype("int64").sort_values().unique()
        for symbol_id, group in decisions.groupby("symbol_id", observed=True)
    }
    market = market[market["symbol_id"].isin(decision_map)].copy()
    rows: list[dict[str, object]] = []
    for symbol_id, group in market.groupby("symbol_id", observed=True, sort=False):
        rows.extend(
            _build_symbol_rows(
                group,
                decision_map[int(symbol_id)],
                config,
                trade_date=task.trade_date,
                symbol_id=int(symbol_id),
            )
        )
    labels = pd.DataFrame(rows)
    if labels.empty:
        raise ValueError(f"No forward-volatility labels produced for {task.trade_date}")
    labels["forward_tail_event"] = 0.0
    for _, indices in labels.groupby(
        ["decision_time", "horizon_minutes"], observed=True, sort=False
    ).groups.items():
        rank = labels.loc[indices, "forward_log_rv"].rank(method="average", pct=True)
        labels.loc[indices, "forward_tail_event"] = (rank >= config.tail_quantile).astype(float)
    duplicate = int(
        labels.duplicated(
            ["label_id", "trade_date", "decision_time", "symbol_id"], keep=False
        ).sum()
    )
    entry = pd.to_datetime(labels["entry_time"], utc=True)
    decision = pd.to_datetime(labels["decision_time"], utc=True)
    exit_time = pd.to_datetime(labels["exit_time"], utc=True)
    available_time = pd.to_datetime(labels["label_available_time"], utc=True)
    invalid_time = int(
        ((entry <= decision) | (exit_time <= entry) | (available_time < exit_time)).sum()
    )
    if duplicate or invalid_time:
        raise ValueError(
            f"Label QA failed for {task.trade_date}: duplicate={duplicate}, "
            f"invalid_time={invalid_time}"
        )
    labels = labels.sort_values(
        ["decision_time", "symbol_id", "horizon_minutes"]
    ).reset_index(drop=True)
    commit_frames(
        task.checkpoint_dir,
        spec,
        {LABEL_DATA_FILE: labels},
        metadata={
            "version": FORWARD_VOLATILITY_VERSION,
            "trade_date": task.trade_date,
            "rows": int(len(labels)),
            "symbols": int(labels["symbol_id"].nunique()),
            "decisions": int(labels["decision_time"].nunique()),
        },
    )
    return {"unit": task.trade_date, "status": "built", "rows": int(len(labels))}


def _write_contracts(
    output_root: Path,
    labels_root: Path,
    config: ForwardVolatilityConfig,
    *,
    labels_anchor: list[dict[str, object]],
) -> Path:
    root = output_root / "contracts"
    contract_hash = sha256_json(
        {
            "version": FORWARD_VOLATILITY_VERSION,
            "config": config.as_dict(),
            "labels": labels_anchor,
        }
    )
    marker = root / "checkpoint.json"
    cached = _cache_read(marker, contract_hash)
    manifest = root / "horizon_manifest.json"
    if cached is not None and manifest.exists():
        return manifest
    pending = root.with_name(f".{root.name}.{os.getpid()}.pending")
    shutil.rmtree(pending, ignore_errors=True)
    pending.mkdir(parents=True, exist_ok=False)
    horizons: list[dict[str, str]] = []
    rows: list[dict[str, object]] = []
    for horizon in config.horizons:
        for target in config.targets:
            name = f"{target}_{int(horizon)}m"
            filename = f"{name}.json"
            contract = LabelContract(
                label_id=f"forward_volatility_{int(horizon)}m_v1",
                horizon_minutes=int(horizon),
                entry_lag_minutes=config.entry_lag_minutes,
                target_column=target,
                overlapping=True,
                rebalance_minutes=5,
                require_entry_after_decision=True,
            )
            contract.validate()
            (pending / filename).write_text(
                json.dumps(contract.as_dict(), indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            horizons.append(
                {
                    "name": name,
                    "labels": str(labels_root.resolve()),
                    "label_contract": str((root / filename).resolve()),
                }
            )
            rows.append(
                {
                    "name": name,
                    "horizon_minutes": int(horizon),
                    "target_kind": target,
                    "label_id": contract.label_id,
                    "contract_file": filename,
                }
            )
    (pending / "horizon_manifest.json").write_text(
        json.dumps(
            {"version": FORWARD_VOLATILITY_VERSION, "horizons": horizons},
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    pd.DataFrame(rows).to_parquet(pending / "contract_index.parquet", index=False)
    _cache_write(
        pending / "checkpoint.json",
        contract_hash,
        {"status": "complete", "contracts": len(rows)},
    )
    if root.exists():
        shutil.rmtree(root, ignore_errors=True)
    os.replace(pending, root)
    return root / "horizon_manifest.json"


def _finalize_volatility_report(
    output_root: Path,
    raw_root: Path,
    config: ForwardVolatilityConfig,
) -> Path:
    report_root = output_root / "prediction_report"
    metric_frames: list[pd.DataFrame] = []
    quantile_frames: list[pd.DataFrame] = []
    daily_frames: list[pd.DataFrame] = []
    for horizon in config.horizons:
        for target in config.targets:
            name = f"{target}_{int(horizon)}m"
            horizon_root = raw_root / f"horizon={name}"
            required = [
                horizon_root / "_SUCCESS",
                horizon_root / "alpha_metrics.csv",
                horizon_root / "quantile_returns.csv",
                horizon_root / "daily_ic.csv",
            ]
            if not all(path.exists() for path in required):
                raise FileNotFoundError(
                    f"Incomplete raw volatility diagnostic for {name}: {required}"
                )
            metrics = pd.read_csv(horizon_root / "alpha_metrics.csv")
            metrics["target_kind"] = target
            metrics["horizon_minutes"] = int(horizon)
            metric_frames.append(metrics)
            quantiles = pd.read_csv(horizon_root / "quantile_returns.csv")
            quantiles["target_kind"] = target
            quantiles["horizon_minutes"] = int(horizon)
            quantile_frames.append(quantiles)
            daily = pd.read_csv(horizon_root / "daily_ic.csv")
            daily["target_kind"] = target
            daily["horizon_minutes"] = int(horizon)
            daily_frames.append(daily)
    raw_metrics = pd.concat(metric_frames, ignore_index=True)
    raw_quantiles = pd.concat(quantile_frames, ignore_index=True)
    daily_ic = pd.concat(daily_frames, ignore_index=True)

    identity = [
        column
        for column in (
            "batch_id",
            "factor_id",
            "scope",
            "theme_family",
            "layer_id",
            "scale_minutes",
            "variant_id",
            "target_kind",
            "horizon_minutes",
        )
        if column in raw_metrics.columns
    ]
    diagnostic_columns = [
        column
        for column in (
            "observations",
            "decision_count",
            "date_count",
            "symbol_count",
            "mean_spearman_ic",
            "median_spearman_ic",
            "spearman_ic_std",
            "spearman_icir",
            "daily_ic_positive_rate",
            "daily_ic_negative_rate",
            "daily_ic_sign_consistency",
            "spearman_ic_tstat_daily",
            "spearman_ic_pvalue_daily",
            "fdr_qvalue",
            "fdr_pass",
            "mean_pearson_ic_daily",
            "raw_top_minus_bottom_mean",
            "quantile_monotonicity",
            "sample_sufficient",
        )
        if column in raw_metrics.columns
    ]
    metrics = raw_metrics[[*identity, *diagnostic_columns]].copy()
    metrics = metrics.rename(
        columns={"raw_top_minus_bottom_mean": "top_bottom_target_spread"}
    )
    metrics["evaluation_family"] = "volatility_diagnostic"
    sample = metrics["sample_sufficient"].fillna(False).astype(bool)
    fdr = metrics["fdr_pass"].fillna(False).astype(bool)
    ic = pd.to_numeric(metrics["mean_spearman_ic"], errors="coerce").abs()
    consistency = pd.to_numeric(
        metrics["daily_ic_sign_consistency"], errors="coerce"
    )
    metrics["prediction_status"] = np.select(
        [sample & fdr & (ic >= 0.01) & (consistency >= 0.55), sample & (ic >= 0.01)],
        ["predictive_candidate", "needs_falsification"],
        default="insufficient_or_rejected",
    )

    group_keys = [
        column
        for column in (
            "scope",
            "theme_family",
            "layer_id",
            "scale_minutes",
            "target_kind",
            "horizon_minutes",
        )
        if column in metrics.columns
    ]
    comparison_rows: list[dict[str, object]] = []
    for values, group in metrics.groupby(group_keys, observed=True, dropna=False):
        key_values = values if isinstance(values, tuple) else (values,)
        row = dict(zip(group_keys, key_values))
        by_variant = group.drop_duplicates("variant_id").set_index("variant_id")
        if "graph_forward" not in by_variant.index:
            continue
        graph = by_variant.loc["graph_forward"]
        graph_ic = float(graph.get("mean_spearman_ic", np.nan))
        node_ic = (
            float(by_variant.loc["node_baseline"].get("mean_spearman_ic", np.nan))
            if "node_baseline" in by_variant.index
            else np.nan
        )
        reverse_ic = (
            float(
                by_variant.loc["graph_reverse_placebo"].get(
                    "mean_spearman_ic", np.nan
                )
            )
            if "graph_reverse_placebo" in by_variant.index
            else np.nan
        )
        row.update(
            {
                "graph_mean_spearman_ic": graph_ic,
                "node_mean_spearman_ic": node_ic,
                "reverse_mean_spearman_ic": reverse_ic,
                "graph_minus_node_abs_ic": (
                    abs(graph_ic) - abs(node_ic) if np.isfinite(node_ic) else np.nan
                ),
                "graph_minus_reverse_abs_ic": (
                    abs(graph_ic) - abs(reverse_ic)
                    if np.isfinite(reverse_ic)
                    else np.nan
                ),
                "graph_fdr_qvalue": float(graph.get("fdr_qvalue", np.nan)),
                "graph_sign_consistency": float(
                    graph.get("daily_ic_sign_consistency", np.nan)
                ),
                "graph_sample_sufficient": bool(
                    graph.get("sample_sufficient", False)
                ),
                "graph_target_spread": float(
                    graph.get("top_bottom_target_spread", np.nan)
                ),
            }
        )
        comparison_rows.append(row)
    comparison = pd.DataFrame(comparison_rows)
    if not comparison.empty:
        comparison["graph_incremental_confirmed"] = (
            comparison["graph_sample_sufficient"]
            & (comparison["graph_fdr_qvalue"] <= 0.05)
            & (comparison["graph_sign_consistency"] >= 0.55)
            & (comparison["graph_minus_reverse_abs_ic"] > 0.002)
            & (comparison["graph_minus_node_abs_ic"] > 0)
        )

    quantiles = raw_quantiles.rename(columns={"mean_return": "mean_target"})
    summary_keys = [
        column
        for column in (
            "layer_id",
            "scale_minutes",
            "variant_id",
            "target_kind",
            "horizon_minutes",
        )
        if column in metrics.columns
    ]
    summary = (
        metrics.groupby(summary_keys, observed=True, dropna=False)[
            [
                column
                for column in (
                    "mean_spearman_ic",
                    "spearman_icir",
                    "top_bottom_target_spread",
                    "fdr_qvalue",
                )
                if column in metrics.columns
            ]
        ]
        .mean()
        .reset_index()
    )

    top = (
        comparison.sort_values(
            [
                "graph_incremental_confirmed",
                "graph_minus_reverse_abs_ic",
                "graph_minus_node_abs_ic",
            ],
            ascending=[False, False, False],
        ).head(40)
        if not comparison.empty
        else pd.DataFrame()
    )
    confirmed = (
        int(comparison["graph_incremental_confirmed"].sum())
        if not comparison.empty
        else 0
    )
    lines = [
        "# Forward Volatility Layer Prediction",
        "",
        f"- Version: `{FORWARD_VOLATILITY_VERSION}`",
        f"- Metric rows: {len(metrics)}",
        f"- Matched graph comparisons: {len(comparison)}",
        f"- Confirmed graph-incremental candidates: {confirmed}",
        "",
        "This report treats every target as a volatility diagnostic. It intentionally "
        "does not carry forward the return-P&L, transaction-cost, Sharpe, drawdown or "
        "VaR fields produced by the generic evaluator.",
        "",
        "`graph_forward` scores were evaluated with the existing GAL scope semantics "
        "and `own_score` controls. The matched comparison requires graph-forward to "
        "beat both node baseline and reverse-edge placebo in absolute IC.",
        "",
        "## Leading layer × target × horizon candidates",
        "",
    ]
    if top.empty:
        lines.append("No matched graph-forward comparisons were available.")
    else:
        columns = [
            column
            for column in (
                "scope",
                "theme_family",
                "layer_id",
                "scale_minutes",
                "target_kind",
                "horizon_minutes",
                "graph_mean_spearman_ic",
                "graph_minus_node_abs_ic",
                "graph_minus_reverse_abs_ic",
                "graph_fdr_qvalue",
                "graph_incremental_confirmed",
            )
            if column in top.columns
        ]
        lines.append("| " + " | ".join(columns) + " |")
        lines.append("|" + "|".join("---" for _ in columns) + "|")
        for values in top[columns].itertuples(index=False, name=None):
            lines.append("| " + " | ".join(str(value) for value in values) + " |")

    pending = report_root.with_name(f".{report_root.name}.{os.getpid()}.pending")
    shutil.rmtree(pending, ignore_errors=True)
    pending.mkdir(parents=True, exist_ok=False)
    atomic_write_frame(metrics, pending / "prediction_metrics.parquet")
    atomic_write_frame(comparison, pending / "variant_comparison.parquet")
    atomic_write_frame(summary, pending / "layer_target_summary.parquet")
    atomic_write_frame(quantiles, pending / "quantile_targets.parquet")
    atomic_write_frame(daily_ic, pending / "daily_ic.parquet")
    atomic_write_text(pending / "REPORT.md", "\n".join(lines) + "\n")
    atomic_write_json(
        pending / "summary.json",
        {
            "version": FORWARD_VOLATILITY_VERSION,
            "metric_rows": int(len(metrics)),
            "matched_comparisons": int(len(comparison)),
            "confirmed_graph_incremental": confirmed,
        },
    )
    atomic_write_text(pending / "_SUCCESS", _utc_now() + "\n")
    if report_root.exists():
        shutil.rmtree(report_root, ignore_errors=True)
    os.replace(pending, report_root)
    return report_root


def run_forward_volatility_patch(
    *,
    signals_root: str | Path,
    gff_core_template: str,
    output_root: str | Path,
    start_date: str | None = None,
    end_date: str | None = None,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    targets: Iterable[str] = DEFAULT_TARGETS,
    label_workers: int = 4,
    prediction_workers: int = 6,
    min_workers: int = 2,
    resource_budget: ResourceBudget = ResourceBudget(96.0, 18, None),
    expected_git_commit: str | None = None,
    require_clean: bool = False,
) -> Path:
    signals = Path(signals_root).expanduser().resolve()
    output = Path(output_root).expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    if not (signals / "_SUCCESS").exists():
        raise FileNotFoundError(f"Incomplete GAL signals: {signals / '_SUCCESS'}")
    export_manifest_path = signals / "export_manifest.json"
    export_manifest = _json_read(export_manifest_path)
    batch_id = str(
        export_manifest.get("parameters", {}).get("batch_id") or "dual_theme_igc"
    )
    config = ForwardVolatilityConfig(
        horizons=tuple(sorted({int(value) for value in horizons})),
        targets=tuple(dict.fromkeys(str(value) for value in targets)),
    )
    config.validate()
    export_anchor = stable_export_manifest_record(export_manifest_path)
    run_manifest = implementation_manifest(
        operation="forward_volatility_patch",
        parameters={
            "version": FORWARD_VOLATILITY_VERSION,
            "signals_root": str(signals),
            "gff_core_template": gff_core_template,
            "output_root": str(output),
            "start_date": start_date,
            "end_date": end_date,
            "config": config.as_dict(),
            "label_workers": int(label_workers),
            "prediction_workers": int(prediction_workers),
            "min_workers": int(min_workers),
        },
        inputs=[export_anchor],
        resource_budget=resource_budget,
    )
    enforce_git_lineage(
        run_manifest,
        expected_commit=expected_git_commit,
        require_clean=require_clean,
    )
    checkpoint_root = output / "_checkpoints" / "forward_volatility"
    preflight_budget = ResourceBudget(
        memory_limit_gb=max(
            2.0, resource_budget.memory_limit_gb / max(1, label_workers)
        ),
        threads=max(1, resource_budget.threads // max(1, label_workers)),
        temp_directory=resource_budget.temp_directory,
    )
    inventory = _decision_inventory(
        signals,
        batch_id,
        output,
        start_date=start_date,
        end_date=end_date,
        export_anchor=export_anchor,
        resource_budget=preflight_budget,
    )
    labels_root = output / "labels"
    label_contract_hash = sha256_json(
        {
            "version": FORWARD_VOLATILITY_VERSION,
            "config": config.as_dict(),
            "signals": export_anchor,
        }
    )
    tasks: list[LabelDateTask] = []
    for row in inventory:
        gff_core = _render_template(gff_core_template, row["trade_date"])
        if not gff_core.exists():
            raise FileNotFoundError(
                f"Missing NFF gff_core for {row['trade_date']}: {gff_core}"
            )
        tasks.append(
            LabelDateTask(
                trade_date=row["trade_date"],
                gff_core=str(gff_core),
                decisions=row["decisions"],
                labels_root=str(labels_root),
                contract_hash=label_contract_hash,
                config=config.as_dict(),
            )
        )
    pending = deque(
        task
        for task in tasks
        if not checkpoint_valid(
            task.checkpoint_dir,
            _label_spec(task),
            required_files=(LABEL_DATA_FILE,),
        )
    )
    state: dict[str, Any] = {
        "completed": len(tasks) - len(pending),
        "reused": len(tasks) - len(pending),
        "failed": 0,
        "workers": int(label_workers),
        "failures": [],
    }

    def update_progress(status: str = "running") -> None:
        write_progress(
            checkpoint_root / "label_build",
            stage="forward-volatility-label-build",
            total=len(tasks),
            completed=int(state["completed"]),
            reused=int(state["reused"]),
            failed=int(state["failed"]),
            status=status,
            extra={
                "factor_workers": int(state["workers"]),
                "failures": list(state["failures"]),
            },
        )

    def on_success(task: LabelDateTask, result: dict[str, object]) -> None:
        state["completed"] += 1
        if result.get("status") == "reused":
            state["reused"] += 1
        update_progress()

    def on_failure(task: LabelDateTask, exc: Exception) -> None:
        state["failed"] += 1
        state["failures"].append(f"{task.trade_date}: {exc!r}")
        update_progress()

    def on_rebuild(old: int, new: int, rebuilds: int, requeued: int) -> None:
        state["workers"] = new
        state["failures"].append(
            f"pool_rebuild#{rebuilds}: workers {old}->{new}; requeued={requeued}"
        )
        update_progress()

    update_progress()
    if pending:
        _healable_pool_run(
            pending,
            initial_workers=max(1, int(label_workers)),
            min_workers=max(1, min(int(min_workers), int(label_workers))),
            run_task=_run_label_task,
            on_success=on_success,
            on_task_failure=on_failure,
            on_pool_rebuild=on_rebuild,
            executor_factory=ProcessPoolExecutor,
        )
    if state["failed"]:
        update_progress("failed")
        raise RuntimeError("Label DAG failed: " + "; ".join(state["failures"]))
    state["completed"] = len(tasks)
    update_progress("complete")

    label_records = [file_record(task.checkpoint_dir / LABEL_DATA_FILE) for task in tasks]
    horizon_manifest = _write_contracts(
        output,
        labels_root,
        config,
        labels_anchor=label_records,
    )
    atomic_write_json(
        output / "label_manifest.json",
        {
            "version": FORWARD_VOLATILITY_VERSION,
            "dates": [task.trade_date for task in tasks],
            "labels": label_records,
            "horizon_manifest": str(horizon_manifest),
            "run_manifest": run_manifest,
        },
    )

    raw_root = output / "raw_volatility_diagnostic"
    run_dual_theme_alpha_campaign(
        signals,
        horizon_manifest,
        raw_root,
        join_keys=("trade_date", "decision_time", "symbol_id"),
        score_column="score",
        symbol_column="symbol_id",
        quantiles=10,
        min_cross_section=100,
        min_theme_size=5,
        min_theme_cross_section=5,
        direction_column="expected_direction",
        default_direction="auto",
        control_columns=("own_score",),
        annualization_factor=None,
        resource_budget=resource_budget,
        expected_git_commit=expected_git_commit,
        require_clean=require_clean,
        allow_legacy_signals=False,
        correlation_sample_modulus=0,
        allow_partial=False,
        factor_workers=prediction_workers,
    )
    report = _finalize_volatility_report(output, raw_root, config)
    atomic_write_json(output / "run_manifest.json", run_manifest)
    atomic_write_text(output / "_SUCCESS", _utc_now() + "\n")
    return report


__all__ = [
    "DEFAULT_HORIZONS",
    "DEFAULT_TARGETS",
    "FORWARD_VOLATILITY_VERSION",
    "ForwardVolatilityConfig",
    "run_forward_volatility_patch",
]
