from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .alpha import AlphaResult
from .batch import get_batch
from .governance import atomic_write_frame, atomic_write_json, atomic_write_text
from .purity import PurityResult


def _fmt(value: Any, digits: int = 6) -> str:
    if value is None or (isinstance(value, float) and not np.isfinite(value)):
        return "N/A"
    if isinstance(value, (float, np.floating)):
        return f"{float(value):.{digits}g}"
    return str(value)


def _write_csv(frame: pd.DataFrame, path: Path) -> None:
    atomic_write_frame(frame, path)


def write_report_bundle(
    output_root: str | Path,
    *,
    batch_id: str,
    batch_status: dict[str, object],
    alpha: AlphaResult | None = None,
    purity: PurityResult | None = None,
    lineage: dict[str, object] | None = None,
    run_manifest: dict[str, object] | None = None,
    top_n: int = 20,
) -> Path:
    output_root = Path(output_root).expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    success = output_root / "_SUCCESS"
    success.unlink(missing_ok=True)
    spec = get_batch(batch_id)
    summary: dict[str, object] = {
        "batch_id": batch_id,
        "batch_spec": spec.as_dict(),
        "batch_status": batch_status,
        "lineage": lineage or {},
    }
    if run_manifest:
        summary["run_contract_hash"] = run_manifest.get("contract_hash")
        atomic_write_json(output_root / "run_manifest.json", run_manifest)
    lines = [
        f"# GraphAlphaLab report: {batch_id}",
        "",
        "## Scope and completion",
        "",
        f"- Expected contracts: {batch_status.get('expected_contracts')}",
        f"- Observed contracts: {batch_status.get('observed_contracts')}",
        f"- Partial: {batch_status.get('partial')}",
        f"- Report scope: {', '.join(spec.report_scope)}",
        "",
    ]

    if purity is not None:
        _write_csv(purity.theme_dimension, output_root / "theme_purity.csv")
        _write_csv(purity.dimension_summary, output_root / "purity_dimension_summary.csv")
        _write_csv(purity.snapshot_agreement, output_root / "purity_snapshot_agreement.csv")
        atomic_write_json(output_root / "metadata_profile.json", purity.metadata_profile)
        summary["metadata_profile"] = purity.metadata_profile
        summary["purity_dimensions"] = purity.dimension_summary.to_dict("records")
        lines.extend(["## Theme Discovery purity", ""])
        for row in purity.dimension_summary.to_dict("records"):
            lines.extend(
                [
                    f"### {row['dimension']}",
                    f"- Coverage: {_fmt(row.get('mean_coverage'))}",
                    f"- Weighted purity: {_fmt(row.get('weighted_purity'))}",
                    f"- Median purity: {_fmt(row.get('median_purity'))}",
                    f"- P10 / P90 purity: {_fmt(row.get('p10_purity'))} / {_fmt(row.get('p90_purity'))}",
                    f"- Mean normalized entropy: {_fmt(row.get('mean_normalized_entropy'))}",
                    f"- Mean dominant-label lift: {_fmt(row.get('mean_dominant_lift'))}",
                    "",
                ]
            )
        if not purity.theme_dimension.empty:
            _write_csv(
                purity.theme_dimension.sort_values("purity_weighted", na_position="last").head(top_n),
                output_root / "lowest_purity_themes.csv",
            )
            _write_csv(
                purity.theme_dimension.sort_values("purity_weighted", ascending=False, na_position="last").head(top_n),
                output_root / "highest_purity_themes.csv",
            )
        lines.extend(
            [
                "Purity is diagnostic, not a clustering target. Stable cross-sector themes must be reviewed rather than rejected solely for low sector purity.",
                "",
            ]
        )

    if alpha is not None:
        _write_csv(alpha.metrics, output_root / "alpha_metrics.csv")
        _write_csv(alpha.ic_series, output_root / "ic_series.csv")
        _write_csv(alpha.daily_ic, output_root / "daily_ic.csv")
        _write_csv(alpha.quantile_returns, output_root / "quantile_returns.csv")
        _write_csv(alpha.portfolio_returns, output_root / "portfolio_returns.csv")
        _write_csv(alpha.stability, output_root / "stability_slices.csv")
        _write_csv(alpha.score_correlation, output_root / "score_correlation.csv")
        summary["factor_count"] = int(len(alpha.metrics))
        summary["research_status_counts"] = (
            alpha.metrics.get("research_status", pd.Series(dtype=str)).value_counts().to_dict()
        )
        summary["governance"] = alpha.governance or {}
        ranking = alpha.metrics.copy()
        if "research_status" in ranking:
            ranking["_status_rank"] = (
                ranking["research_status"]
                .map({"candidate": 0, "needs_falsification": 1, "insufficient_or_rejected": 2})
                .fillna(3)
            )
        ranking_columns = [
            column
            for column in ("_status_rank", "fdr_pass", "net_daily_diagnostic_sharpe_5bps", "mean_spearman_ic")
            if column in ranking
        ]
        if ranking_columns:
            ranking = ranking.sort_values(
                ranking_columns,
                ascending=[True if column == "_status_rank" else False for column in ranking_columns],
                na_position="last",
            )
        ranking = ranking.drop(columns=["_status_rank"], errors="ignore")
        _write_csv(ranking, output_root / "ranking.csv")
        lines.extend(["## Governance and PIT", ""])
        governance = alpha.governance or {}
        pit = governance.get("pit_audit", {}) if isinstance(governance, dict) else {}
        contract = governance.get("label_contract", {}) if isinstance(governance, dict) else {}
        lines.extend(
            [
                f"- PIT audit passed: {_fmt(pit.get('passed'))}",
                f"- Signal-after-decision violations: {_fmt(pit.get('signal_after_decision'))}",
                f"- Duplicate label keys: {_fmt(pit.get('duplicate_label_keys'))}",
                f"- Entry not after decision: {_fmt(pit.get('entry_not_after_decision'))}",
                f"- Label ID / horizon: {_fmt(contract.get('label_id'))} / {_fmt(contract.get('horizon_minutes'))}m",
                f"- Label overlapping: {_fmt(contract.get('overlapping'))}",
                "",
            ]
        )
        lines.extend(["## Complete factor-by-factor Alpha interpretation", ""])
        for index, row in alpha.metrics.iterrows():
            identity = " | ".join(
                f"{column}={row[column]}"
                for column in ("factor_id", "layer_id", "scale_minutes", "horizon", "variant_id")
                if column in row and pd.notna(row[column])
            ) or f"factor_row={index}"
            lines.extend(
                [
                    f"### {identity}",
                    "**Data sufficiency and inference unit**",
                    f"- Observations / decisions / dates / symbols: {_fmt(row.get('observations'))} / {_fmt(row.get('decision_count'))} / {_fmt(row.get('date_count'))} / {_fmt(row.get('symbol_count'))}",
                    f"- Sample sufficient (>=20 dates): {_fmt(row.get('sample_sufficient'))}",
                    "**Predictive strength**",
                    f"- Daily Spearman IC mean / median / ICIR: {_fmt(row.get('mean_spearman_ic'))} / {_fmt(row.get('median_spearman_ic'))} / {_fmt(row.get('spearman_icir'))}",
                    f"- Daily IC sign consistency: {_fmt(row.get('daily_ic_sign_consistency'))}",
                    f"- Daily IC t-stat / p-value / FDR q: {_fmt(row.get('spearman_ic_tstat_daily'))} / {_fmt(row.get('spearman_ic_pvalue_daily'))} / {_fmt(row.get('fdr_qvalue'))}",
                    "**Direction contract**",
                    f"- Expected direction / source: {_fmt(row.get('expected_direction'))} / {_fmt(row.get('direction_source'))}",
                    f"- Direction predeclared: {_fmt(row.get('direction_predeclared'))}",
                    "**Portfolio read-through**",
                    f"- Raw top-minus-bottom / oriented long-short: {_fmt(row.get('raw_top_minus_bottom_mean'))} / {_fmt(row.get('oriented_long_short_mean'))}",
                    f"- Hit rate / daily diagnostic t-stat: {_fmt(row.get('oriented_long_short_hit_rate'))} / {_fmt(row.get('oriented_long_short_tstat_daily'))}",
                    f"- Annualization valid / annualized Sharpe: {_fmt(row.get('annualization_valid'))} / {_fmt(row.get('annualized_sharpe'))}",
                    f"- Daily diagnostic Sharpe: {_fmt(row.get('daily_diagnostic_sharpe'))}",
                    "**Risk and tails**",
                    f"- Max drawdown / VaR5 / CVaR5: {_fmt(row.get('max_drawdown'))} / {_fmt(row.get('var_5pct'))} / {_fmt(row.get('cvar_5pct'))}",
                    "**Tradability**",
                    f"- Mean traded notional: {_fmt(row.get('mean_turnover'))}",
                    f"- Net mean at 5 bps: {_fmt(row.get('net_mean_5bps'))}",
                    f"- Survives 5 bps: {_fmt(row.get('cost_survives_5bps'))}",
                    "**Decision gates**",
                    f"- Governance ready / FDR pass: {_fmt(row.get('governance_ready'))} / {_fmt(row.get('fdr_pass'))}",
                    f"- Research status: {_fmt(row.get('research_status'))}",
                    "",
                ]
            )
        lines.extend(
            [
                "## Information-distillation output",
                "",
                "`daily_ic.csv` is the inferential evidence. `ic_series.csv` retains overlapping snapshot diagnostics. `score_correlation.csv` is a deterministic sampled redundancy audit rather than an empty placeholder.",
                "",
            ]
        )

    atomic_write_json(output_root / "summary.json", summary)
    atomic_write_text(output_root / "REPORT.md", "\n".join(lines) + "\n")
    atomic_write_json(success, {"batch_id": batch_id, "complete": not bool(batch_status.get("partial"))})
    return output_root
