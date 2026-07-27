from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb
import pandas as pd

from graphalphalab.alpha import _residualize_score
from graphalphalab.contracts import LabelContract
from graphalphalab.core4_execution_semantics import semantics_for_scope
from graphalphalab.dual_theme_scope_alpha import (
    _prepare_inter_factor,
    _prepare_within_factor,
)
from graphalphalab.execution_frequency import (
    add_frequency_deltas,
    default_execution_policy_grid,
    evaluate_policy_grid,
    pareto_frontier,
    walk_forward_direction_by_date,
)


def _sql_path(path: Path) -> str:
    return path.expanduser().resolve().as_posix().replace("'", "''")


def _signal_glob(root: Path, scope: str) -> str:
    path = root / "batch_id=dual_theme_igc" / f"scope={scope}" / "**" / "*.parquet"
    return _sql_path(path)


def _prepare_global(frame: pd.DataFrame, controls: list[str]) -> pd.DataFrame:
    data = frame.copy()
    if controls:
        parts = []
        for _, cross in data.groupby("decision_time", observed=True, sort=True):
            residual = _residualize_score(cross, "score", controls)
            parts.append(pd.Series(residual, index=cross.index))
        if parts:
            data["execution_score"] = pd.concat(parts).sort_index()
    else:
        data["execution_score"] = pd.to_numeric(data["score"], errors="coerce")
    data["execution_target_return"] = pd.to_numeric(data["target_return"], errors="coerce")
    return data.dropna(subset=["execution_score", "execution_target_return"])


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Compare fixed and gated rebalance frequencies for one governed dual-theme "
            "factor without recomputing GFF."
        )
    )
    parser.add_argument("--signals-root", type=Path, required=True)
    parser.add_argument("--labels", type=Path, required=True)
    parser.add_argument("--label-contract", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--scope", choices=("global", "within_theme", "inter_theme"), required=True)
    parser.add_argument("--factor-id", required=True)
    parser.add_argument("--layer-id", required=True)
    parser.add_argument("--scale-minutes", type=int, required=True)
    parser.add_argument("--variant-id", default="graph_forward")
    parser.add_argument("--min-theme-size", type=int, default=5)
    parser.add_argument("--min-theme-cross-section", type=int, default=5)
    parser.add_argument("--min-direction-train-dates", type=int, default=20)
    parser.add_argument("--direction-rolling-dates", type=int, default=60)
    parser.add_argument("--control-columns", default="own_score")
    parser.add_argument("--quantiles", type=int, default=5)
    args = parser.parse_args()

    contract = LabelContract.from_json(args.label_contract)
    controls = [value.strip() for value in args.control_columns.split(",") if value.strip()]
    connection = duckdb.connect()
    try:
        signals = _signal_glob(args.signals_root, args.scope)
        labels = _sql_path(args.labels)
        selected = [
            "s.batch_id",
            "s.factor_id",
            "s.layer_id",
            "s.scale_minutes",
            "s.variant_id",
            "s.trade_date",
            "s.decision_time",
            "s.symbol_id",
            "s.score",
            "s.own_score",
            "s.context_theme_id",
            "s.membership_weight",
            f'l."{contract.target_column}" AS target_return',
        ]
        query = f"""
        SELECT {', '.join(selected)}
        FROM read_parquet('{signals}', union_by_name=true) s
        JOIN read_parquet('{labels}', union_by_name=true) l
          ON s.trade_date=l.trade_date
         AND s.decision_time=l.decision_time
         AND s.symbol_id=l.symbol_id
        WHERE CAST(l.label_id AS VARCHAR)=?
          AND CAST(s.factor_id AS VARCHAR)=?
          AND CAST(s.layer_id AS VARCHAR)=?
          AND CAST(s.scale_minutes AS INTEGER)=?
          AND CAST(s.variant_id AS VARCHAR)=?
        ORDER BY s.trade_date, s.decision_time, s.symbol_id
        """
        frame = connection.execute(
            query,
            [
                contract.label_id,
                args.factor_id,
                args.layer_id,
                args.scale_minutes,
                args.variant_id,
            ],
        ).fetch_df()
    finally:
        connection.close()

    if frame.empty:
        raise SystemExit("No joined signal/label rows matched the requested factor identity")

    if args.scope == "within_theme":
        prepared = _prepare_within_factor(
            frame,
            score_column="score",
            control_columns=controls,
            min_theme_size=args.min_theme_size,
        ).rename(
            columns={
                "scope_score": "execution_score",
                "scope_target_return": "execution_target_return",
            }
        )
    elif args.scope == "inter_theme":
        prepared = _prepare_inter_factor(
            frame,
            score_column="score",
            control_columns=controls,
            min_theme_cross_section=args.min_theme_cross_section,
            min_theme_size=args.min_theme_size,
        ).rename(
            columns={
                "scope_score": "execution_score",
                "scope_target_return": "execution_target_return",
            }
        )
    else:
        prepared = _prepare_global(frame, controls)

    direction_by_date = walk_forward_direction_by_date(
        prepared,
        score_column="execution_score",
        return_column="execution_target_return",
        min_train_dates=args.min_direction_train_dates,
        rolling_dates=args.direction_rolling_dates,
    )
    if not direction_by_date:
        raise SystemExit(
            "No PIT-safe walk-forward direction became available. Increase the date range "
            "or reduce --min-direction-train-dates only for an explicit research diagnostic."
        )

    metrics, returns = evaluate_policy_grid(
        prepared,
        default_execution_policy_grid(),
        score_column="execution_score",
        return_column="execution_target_return",
        symbol_column="symbol_id",
        theme_column="context_theme_id" if "context_theme_id" in prepared.columns else None,
        quantiles=args.quantiles,
        direction_by_date=direction_by_date,
    )
    identity = {
        "scope": args.scope,
        "factor_id": args.factor_id,
        "layer_id": args.layer_id,
        "scale_minutes": args.scale_minutes,
        "variant_id": args.variant_id,
        "horizon_minutes": contract.horizon_minutes,
        **semantics_for_scope(args.scope).as_dict(),
    }
    for key, value in identity.items():
        metrics[key] = value
        if not returns.empty:
            returns[key] = value
    metrics = add_frequency_deltas(metrics)
    frontier = pareto_frontier(metrics)

    args.output.mkdir(parents=True, exist_ok=True)
    metrics.to_csv(args.output / "frequency_policy_metrics.csv", index=False)
    returns.to_csv(args.output / "frequency_policy_returns.csv", index=False)
    frontier.to_csv(args.output / "frequency_pareto_frontier.csv", index=False)
    summary = {
        "identity": identity,
        "label_contract": contract.as_dict(),
        "prepared_rows": int(len(prepared)),
        "direction_dates": len(direction_by_date),
        "policy_count": int(len(metrics)),
        "pareto_policy_ids": frontier.get("policy_id", pd.Series(dtype=str)).tolist(),
        "principle": (
            "Frequency is compared at fixed factor and fixed return horizon. Direction "
            "is walk-forward and uses prior dates only. GFF is not recomputed."
        ),
    }
    (args.output / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(args.output)


if __name__ == "__main__":
    main()
