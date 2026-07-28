from pathlib import Path
import json

import pandas as pd

from graphalphalab.daily_labels_v2 import CORE_DAILY_LABELS
from graphalphalab.discussion_pack import write_discussion_pack
from graphalphalab.rolling_alpha_v2 import DEFAULT_ROLLING_SCHEDULE, run_rolling_alpha_v2


def test_daily_v2_always_enters_next_session_open() -> None:
    assert len(CORE_DAILY_LABELS) == 3
    assert all(spec.entry_offset == 1 for spec in CORE_DAILY_LABELS)
    assert all(spec.entry_point == "open" for spec in CORE_DAILY_LABELS)


def test_rolling_v2_uses_short_medium_schedule_and_shards(tmp_path: Path) -> None:
    report = tmp_path / "report"
    horizon = report / "horizon=5m"
    horizon.mkdir(parents=True)
    (horizon / "_SUCCESS").write_text("ok", encoding="utf-8")
    dates = pd.bdate_range("2026-04-01", periods=70).strftime("%Y-%m-%d").tolist()
    factor = "global::shared_global::momentum_state_to_return::graph_forward"
    common = {"batch_id": "dual_theme_igc", "factor_id": factor, "layer_id": "momentum_state_to_return", "scale_minutes": 15, "variant_id": "graph_forward", "scope": "global", "theme_family": "shared_global"}
    pd.DataFrame([{**common, "trade_date": date, "spearman_ic": 0.01} for date in dates]).to_csv(horizon / "daily_ic.csv", index=False)
    pd.DataFrame([{**common, "trade_date": date, "raw_top_minus_bottom_return": 0.001, "turnover": 0.2} for date in dates]).to_csv(horizon / "portfolio_returns.csv", index=False)
    pd.DataFrame([{**common, "direction_predeclared": True, "expected_direction": 1}]).to_csv(horizon / "alpha_metrics.csv", index=False)
    manifest = tmp_path / "labels.json"
    manifest.write_text(json.dumps({"analysis_dates": dates, "labels": [{"label_id": "5m", "horizon_type": "intraday", "last_available_date": dates[-1], "tail_missing_dates": ""}]}), encoding="utf-8")

    output = run_rolling_alpha_v2(report, tmp_path / "rolling", label_manifest=manifest, windows=tuple(DEFAULT_ROLLING_SCHEDULE), window_steps=DEFAULT_ROLLING_SCHEDULE, promotion_windows=(15, 20, 30), min_direction_train_dates=15, direction_rolling_dates=30)
    assert (output / "_SUCCESS").exists()
    assert not (output / "rolling_alpha_metrics.csv").exists()
    index = pd.read_csv(output / "rolling_shard_index.csv")
    assert set(index["window_sessions"]) == {5, 10, 15, 20, 30}
    summary = json.loads((output / "summary.json").read_text(encoding="utf-8"))
    assert summary["window_steps"] == {"5": 1, "10": 1, "15": 2, "20": 2, "30": 5}
    assert summary["promotion_windows"] == [15, 20, 30]


def test_discussion_pack_retains_every_factor_in_small_shards(tmp_path: Path) -> None:
    report = tmp_path / "report"
    rolling = tmp_path / "rolling"
    report.mkdir(); rolling.mkdir()
    rows = []
    for index, (scope, horizon, variant) in enumerate((("global", "5m", "graph_forward"), ("within_theme", "5m", "graph_forward"), ("inter_theme", "15m", "node_baseline"))):
        rows.append({"factor_id": f"factor-{index}", "layer_id": "momentum_state_to_return", "scale_minutes": 15, "variant_id": variant, "scope": scope, "theme_family": "shared_global", "horizon": horizon, "horizon_minutes": int(horizon[:-1]), "financial_role": "direct_return_alpha", "mean_spearman_ic": 0.01 * (index + 1), "spearman_icir": 0.2, "daily_ic_sign_consistency": 0.7, "oriented_long_short_mean": 0.001, "mean_turnover": 0.5, "net_mean_5bps": 0.0005, "date_count": 70, "research_status": "needs_falsification"})
    pd.DataFrame(rows).to_csv(report / "all_horizons_alpha_metrics.csv", index=False)
    pd.DataFrame(columns=["factor_id", "promotion_window"]).to_csv(rolling / "rolling_factor_stability.csv", index=False)
    output = write_discussion_pack(report, rolling, tmp_path / "pack", top_n=1)
    assert (output / "_SUCCESS").exists()
    assert (output / "overview" / "factor_master_compact.csv").exists()
    shards = pd.read_csv(output / "factor_shard_index.csv")
    assert shards["rows"].sum() == 3
    assert all((output / path).exists() for path in shards["relative_path"])
