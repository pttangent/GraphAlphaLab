from __future__ import annotations

from graphalphalab.dual_theme_global_dag import (
    FactorTask,
    round_robin_tasks,
    split_worker_budget,
)
from graphalphalab.governance import ResourceBudget


def _task(horizon: str, scope: str, ordinal: int) -> FactorTask:
    return FactorTask(
        horizon=horizon,
        horizon_minutes=5,
        scope=scope,
        ordinal=ordinal,
        signal_path="signals",
        labels_path="labels",
        label_contract_path="contract.json",
        checkpoint_root="checkpoints",
        checkpoint_contract_hash="contract",
        factor_keys=("factor_id",),
        factor_identity={"factor_id": f"{horizon}-{scope}-{ordinal}"},
        pit_audit={"passed": True},
        signal_columns=(
            "factor_id",
            "trade_date",
            "decision_time",
            "symbol_id",
            "score",
            "signal_available_time",
            "context_theme_id",
            "membership_weight",
        ),
        join_keys=("trade_date", "decision_time", "symbol_id"),
        metadata_path=None,
        metadata_id="symbol_id",
        metadata_signal_id="symbol_id",
        slice_columns=(),
        score_column="score",
        symbol_column="symbol_id",
        quantiles=5,
        min_cross_section=100,
        min_theme_size=5,
        min_theme_cross_section=5,
        direction_column="expected_direction",
        default_direction="auto",
        control_columns=("own_score",),
        annualization_factor=None,
        allow_legacy_signals=False,
        worker_memory_limit_gb=8.0,
        worker_threads=2,
        temp_directory=None,
    )


def test_default_six_worker_budget_partitions_total_resources() -> None:
    budget = split_worker_budget(ResourceBudget(60.0, 12, "tmp"), 6)
    assert budget.memory_limit_gb == 10.0
    assert budget.threads == 2
    assert budget.temp_directory == "tmp"


def test_global_ready_queue_round_robins_horizon_and_scope_without_barriers() -> None:
    tasks = [
        _task("5m", "global", 1),
        _task("5m", "global", 2),
        _task("5m", "within_theme", 1),
        _task("15m", "global", 1),
        _task("15m", "inter_theme", 1),
        _task("15m", "inter_theme", 2),
    ]
    ordered = round_robin_tasks(tasks)
    first_wave = {(task.horizon, task.scope) for task in ordered[:4]}
    assert first_wave == {
        ("5m", "global"),
        ("5m", "within_theme"),
        ("15m", "global"),
        ("15m", "inter_theme"),
    }
    assert ordered[-2].scope in {"global", "inter_theme"}
    assert len(ordered) == len(tasks)


def test_task_identity_includes_horizon_scope_and_factor() -> None:
    task = _task("30m", "within_theme", 7)
    assert "horizon=30m" in task.unit_name
    assert "scope=within_theme" in task.unit_name
    assert "factor_id=30m-within_theme-7" in task.unit_name
import json

from graphalphalab.dual_theme_resumable import stable_export_manifest_record


def _export_manifest(path, *, memory_gb, threads, factor_count=318):
    payload = {
        "export_version": "GAL_DUAL_THEME_GFF_EXPORT_V2_SCOPE_SEMANTICS",
        "gff_campaign_version": "SMI_DUAL_THEME_IGC_FULL_SCOPE_COMPARE_V2_INDUCED_WITHIN",
        "factor_count": factor_count,
        "parameters": {"batch_id": "dual_theme_igc", "factor_count": factor_count},
        "resource_budget": {"memory_limit_gb": memory_gb, "threads": threads, "temp_directory": "tmp"},
        "runtime_versions": {"python": "3.11"},
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_export_manifest_record_ignores_execution_budget(tmp_path) -> None:
    a = stable_export_manifest_record(_export_manifest(tmp_path / "a.json", memory_gb=64.0, threads=12))
    b = stable_export_manifest_record(_export_manifest(tmp_path / "b.json", memory_gb=72.0, threads=8))
    assert a["sha256"] == b["sha256"]
    c = stable_export_manifest_record(_export_manifest(tmp_path / "c.json", memory_gb=72.0, threads=8, factor_count=42))
    assert a["sha256"] != c["sha256"]