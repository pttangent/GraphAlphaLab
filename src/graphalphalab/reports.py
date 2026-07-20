from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .alpha import AlphaResult
from .batch import get_batch
from .io import atomic_write_json, atomic_write_text
from .purity import PurityResult


def _fmt(value: Any, digits: int = 6) -> str:
    if value is None or (isinstance(value, float) and not np.isfinite(value)):
        return "N/A"
    if isinstance(value, (float, np.floating)):
        return f"{float(value):.{digits}g}"
    return str(value)


def write_report_bundle(
    output_root: str | Path,
    *,
    batch_id: str,
    batch_status: dict[str, object],
    alpha: AlphaResult | None = None,
    purity: PurityResult | None = None,
    lineage: dict[str, object] | None = None,
    top_n: int = 20,
) -> Path:
    output_root = Path(output_root).expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    spec = get_batch(batch_id)
    summary: dict[str, object] = {
        "batch_id": batch_id,
        "batch_spec": spec.as_dict(),
        "batch_status": batch_status,
        "lineage": lineage or {},
    }
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
        purity.theme_dimension.to_csv(output_root / "theme_purity.csv", index=False)
        purity.dimension_summary.to_csv(output_root / "purity_dimension_summary.csv", index=False)
        purity.snapshot_agreement.to_csv(output_root / "purity_snapshot_agreement.csv", index=False)
        atomic_write_json(output_root / "metadata_profile.json", purity.metadata_profile)
        summary["metadata_profile"] = purity.metadata_profile
        summary["purity_dimensions"] = purity.dimension_summary.to_dict("records")
        lines.extend(["## Theme Discovery purity", ""])
        for row in purity.dimension_summary.to_dict("records"):
            lines.extend([
                f"### {row['dimension']}",
                f"- Coverage: {_fmt(row.get('mean_coverage'))}",
                f"- Weighted purity: {_fmt(row.get('weighted_purity'))}",
                f"- Median purity: {_fmt(row.get('median_purity'))}",
                f"- P10 / P90 purity: {_fmt(row.get('p10_purity'))} / {_fmt(row.get('p90_purity'))}",
                f"- Mean normalized entropy: {_fmt(row.get('mean_normalized_entropy'))}",
                f"- Mean dominant-label lift: {_fmt(row.get('mean_dominant_lift'))}",
                "",
            ])
        if not purity.theme_dimension.empty:
            weakest = purity.theme_dimension.sort_values("purity_weighted", na_position="last").head(top_n)
            strongest = purity.theme_dimension.sort_values("purity_weighted", ascending=False, na_position="last").head(top_n)
            weakest.to_csv(output_root / "lowest_purity_themes.csv", index=False)
            strongest.to_csv(output_root / "highest_purity_themes.csv", index=False)
        lines.extend([
            "Purity is diagnostic, not a clustering target. Stable cross-sector themes must be reviewed rather than rejected solely for low sector purity.",
            "",
        ])

    if alpha is not None:
        alpha.metrics.to_csv(output_root / "alpha_metrics.csv", index=False)
        alpha.ic_series.to_csv(output_root / "ic_series.csv", index=False)
        alpha.quantile_returns.to_csv(output_root / "quantile_returns.csv", index=False)
        alpha.portfolio_returns.to_csv(output_root / "portfolio_returns.csv", index=False)
        alpha.stability.to_csv(output_root / "stability_slices.csv", index=False)
        alpha.score_correlation.to_csv(output_root / "score_correlation.csv", index=False)
        summary["factor_count"] = int(len(alpha.metrics))
        summary["research_status_counts"] = alpha.metrics.get("research_status", pd.Series(dtype=str)).value_counts().to_dict()
        ranking = alpha.metrics.copy()
        if "research_status" in ranking:
            ranking["_status_rank"] = ranking["research_status"].map({
                "candidate": 0, "needs_falsification": 1, "insufficient_or_rejected": 2
            }).fillna(3)
        ranking_columns = [column for column in ("_status_rank", "fdr_pass", "net_sharpe_5bps", "mean_spearman_ic") if column in ranking]
        if ranking_columns:
            ranking = ranking.sort_values(
                ranking_columns,
                ascending=[True if column == "_status_rank" else False for column in ranking_columns],
                na_position="last",
            )
        ranking = ranking.drop(columns=["_status_rank"], errors="ignore")
        ranking.to_csv(output_root / "ranking.csv", index=False)
        lines.extend(["## Complete factor-by-factor Alpha interpretation", ""])
        for index, row in alpha.metrics.iterrows():
            identity = " | ".join(
                f"{column}={row[column]}" for column in ("factor_id", "layer_id", "scale_minutes", "horizon", "variant_id")
                if column in row and pd.notna(row[column])
            ) or f"factor_row={index}"
            lines.extend([
                f"### {identity}",
                "**Data sufficiency**",
                f"- Observations / decisions / symbols: {_fmt(row.get('observations'))} / {_fmt(row.get('decision_count'))} / {_fmt(row.get('symbol_count'))}",
                f"- Sample sufficient: {_fmt(row.get('sample_sufficient'))}",
                "**Predictive strength**",
                f"- Spearman IC mean / median / ICIR: {_fmt(row.get('mean_spearman_ic'))} / {_fmt(row.get('median_spearman_ic'))} / {_fmt(row.get('spearman_icir'))}",
                f"- Pearson IC mean: {_fmt(row.get('mean_pearson_ic'))}",
                f"- IC positive rate / t-stat / p-value / FDR q: {_fmt(row.get('spearman_ic_positive_rate'))} / {_fmt(row.get('spearman_ic_tstat'))} / {_fmt(row.get('spearman_ic_pvalue'))} / {_fmt(row.get('fdr_qvalue'))}",
                "**Portfolio read-through**",
                f"- Top / bottom / long-short mean: {_fmt(row.get('top_leg_mean'))} / {_fmt(row.get('bottom_leg_mean'))} / {_fmt(row.get('long_short_mean'))}",
                f"- Long-short Sharpe / hit rate / t-stat: {_fmt(row.get('long_short_sharpe'))} / {_fmt(row.get('long_short_hit_rate'))} / {_fmt(row.get('long_short_tstat'))}",
                f"- Quantile monotonicity: {_fmt(row.get('quantile_monotonicity'))}",
                "**Risk and tails**",
                f"- Max drawdown / VaR5 / CVaR5: {_fmt(row.get('max_drawdown'))} / {_fmt(row.get('var_5pct'))} / {_fmt(row.get('cvar_5pct'))}",
                f"- Skew / kurtosis: {_fmt(row.get('skew'))} / {_fmt(row.get('kurtosis'))}",
                "**Tradability**",
                f"- Turnover: {_fmt(row.get('turnover'))}",
                f"- Net mean / Sharpe at 5 bps: {_fmt(row.get('net_mean_5bps'))} / {_fmt(row.get('net_sharpe_5bps'))}",
                f"- Survives 5 bps: {_fmt(row.get('cost_survives_5bps'))}",
                "**Decision gates**",
                f"- Direction consistent / FDR pass: {_fmt(row.get('direction_consistent'))} / {_fmt(row.get('fdr_pass'))}",
                f"- Research status: {_fmt(row.get('research_status'))}",
                "",
            ])
        lines.extend([
            "## Information-distillation output",
            "",
            "Use `ranking.csv` for the complete ordered list, `stability_slices.csv` for date/time breakdowns, and `score_correlation.csv` for redundancy. The Markdown report intentionally lists every factor but avoids embedding large raw partitions.",
            "",
        ])

    atomic_write_json(output_root / "summary.json", summary)
    atomic_write_text(output_root / "REPORT.md", "\n".join(lines) + "\n")
    return output_root
