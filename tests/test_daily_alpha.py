from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from graphalphalab.contracts import LabelContract, validate_label_frame
from graphalphalab.daily_alpha import CORE_DAILY_LABELS, run_rolling_alpha


def test_legacy_minute_contract_payload_is_hash_compatible() -> None:
    contract = LabelContract(label_id="ret_5m", horizon_minutes=5, entry_lag_minutes=1)
    assert contract.as_dict() == {
        "label_id": "ret_5m",
        "horizon_minutes": 5,
        "entry_lag_minutes": 1,
        "target_column": "target_return",
        "decision_time_column": "decision_time",
        "entry_time_column": "entry_time",
        "exit_time_column": "exit_time",
        "available_time_column": "label_available_time",
        "overlapping": True,
        "rebalance_minutes": None,
        "horizon_tolerance_seconds": 60,
        "require_entry_after_decision": True,
    }


def test_trading_session_contract_accepts_weekend_wall_clock_gap() -> None:
    contract = LabelContract(
        label_id="daily_n_close_to_n1_close",
        horizon_minutes=390,
        entry_lag_minutes=0,
        horizon_unit="trading_sessions",
        entry_session_offset=0,
        exit_session_offset=1,
        entry_point="close",
        exit_point="close",
        tail_policy="allow_truncated_tail",
    )
    labels = pd.DataFrame(
        {
            "trade_date": ["2026-07-17"],
            "decision_time": ["2026-07-17T19:00:00Z"],
            "symbol_id": [1],
            "label_id": [contract.label_id],
            "target_return": [0.01],
            "entry_time": ["2026-07-17T20:00:00Z"],
            "exit_time": ["2026-07-20T20:00:00Z"],
            "label_available_time": ["2026-07-20T20:00:01Z"],
            "entry_session_offset": [0],
            "exit_session_offset": [1],
            "entry_point": ["close"],
            "exit_point": ["close"],
        }
    )
    audit = validate_label_frame(
        labels,
        contract,
        join_keys=("trade_date", "decision_time", "symbol_id"),
    )
    assert audit.passed
    assert len(CORE_DAILY_LABELS) == 3


def test_rolling_report_marks_tail_missing_without_fabrication(tmp_path: Path) -> None:
    report = tmp_path / "report"
    horizon = report / "horizon=daily_n_close_to_n1_close"
    horizon.mkdir(parents=True)
    (horizon / "_SUCCESS").write_text("ok", encoding="utf-8")
    dates = pd.bdate_range("2026-04-01", periods=70).strftime("%Y-%m-%d").tolist()
    available = dates[:-1]
    factor = "global::shared_global::momentum_state_to_return::graph_forward"
    common = {
        "batch_id": "dual_theme_igc",
        "factor_id": factor,
        "layer_id": "momentum_state_to_return",
        "scale_minutes": 15,
        "variant_id": "graph_forward",
        "scope": "global",
        "theme_family": "shared_global",
    }
    pd.DataFrame(
        [{**common, "trade_date": date, "spearman_ic": 0.01} for date in available]
    ).to_csv(horizon / "daily_ic.csv", index=False)
    pd.DataFrame(
        [
            {
                **common,
                "trade_date": date,
                "raw_top_minus_bottom_return": 0.001,
                "turnover": 0.2,
            }
            for date in available
        ]
    ).to_csv(horizon / "portfolio_returns.csv", index=False)
    pd.DataFrame(
        [{**common, "direction_predeclared": True, "expected_direction": 1}]
    ).to_csv(horizon / "alpha_metrics.csv", index=False)
    manifest = tmp_path / "labels.json"
    manifest.write_text(
        json.dumps(
            {
                "analysis_dates": dates,
                "labels": [
                    {
                        "label_id": "daily_n_close_to_n1_close",
                        "available_date_count": 69,
                        "last_available_date": available[-1],
                        "tail_missing_date_count": 1,
                        "tail_missing_dates": dates[-1],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    output = run_rolling_alpha(
        report,
        tmp_path / "rolling",
        label_manifest=manifest,
        windows=(20, 40),
        step=5,
    )
    metrics = pd.read_csv(output / "rolling_alpha_metrics.csv")
    assert (output / "_SUCCESS").exists()
    assert metrics["label_tail_missing_dates"].astype(str).str.contains(dates[-1]).all()
    assert metrics["observed_sessions"].max() <= metrics["expected_sessions"].max()
    assert not metrics["window_end"].isna().any()
