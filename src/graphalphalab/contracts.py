from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path
from typing import Iterable

import pandas as pd

from .governance import sha256_json


@dataclass(frozen=True)
class LabelContract:
    label_id: str
    horizon_minutes: int
    entry_lag_minutes: int
    target_column: str = "target_return"
    decision_time_column: str = "decision_time"
    entry_time_column: str = "entry_time"
    exit_time_column: str = "exit_time"
    available_time_column: str = "label_available_time"
    overlapping: bool = True
    rebalance_minutes: int | None = None
    horizon_tolerance_seconds: int = 60
    require_entry_after_decision: bool = True

    def validate(self) -> None:
        if not self.label_id.strip():
            raise ValueError("label_id must not be empty")
        if self.horizon_minutes <= 0:
            raise ValueError("horizon_minutes must be positive")
        if self.entry_lag_minutes < 0:
            raise ValueError("entry_lag_minutes must be non-negative")
        if self.rebalance_minutes is not None and self.rebalance_minutes <= 0:
            raise ValueError("rebalance_minutes must be positive when supplied")
        if self.horizon_tolerance_seconds < 0:
            raise ValueError("horizon_tolerance_seconds must be non-negative")

    def as_dict(self) -> dict[str, object]:
        return asdict(self)

    @property
    def contract_hash(self) -> str:
        return sha256_json(self.as_dict())

    @classmethod
    def from_json(cls, path: str | Path) -> "LabelContract":
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        contract = cls(**payload)
        contract.validate()
        return contract


@dataclass(frozen=True)
class PitAudit:
    signal_rows: int
    signal_available_time_missing: int
    signal_after_decision: int
    label_rows: int
    duplicate_label_keys: int
    entry_not_after_decision: int
    exit_not_after_entry: int
    label_available_before_exit: int
    horizon_mismatch: int

    @property
    def passed(self) -> bool:
        return not any(
            (
                self.signal_available_time_missing,
                self.signal_after_decision,
                self.duplicate_label_keys,
                self.entry_not_after_decision,
                self.exit_not_after_entry,
                self.label_available_before_exit,
                self.horizon_mismatch,
            )
        )

    def as_dict(self) -> dict[str, object]:
        return {**asdict(self), "passed": self.passed}


def required_label_columns(contract: LabelContract, join_keys: Iterable[str]) -> set[str]:
    return {
        *join_keys,
        "label_id",
        contract.target_column,
        contract.decision_time_column,
        contract.entry_time_column,
        contract.exit_time_column,
        contract.available_time_column,
    }


def validate_label_frame(
    labels: pd.DataFrame,
    contract: LabelContract,
    *,
    join_keys: Iterable[str],
) -> PitAudit:
    contract.validate()
    keys = list(join_keys)
    missing = sorted(required_label_columns(contract, keys) - set(labels.columns))
    if missing:
        raise ValueError(f"Label frame is missing governed columns: {missing}")
    data = labels.copy()
    for column in (
        contract.decision_time_column,
        contract.entry_time_column,
        contract.exit_time_column,
        contract.available_time_column,
    ):
        data[column] = pd.to_datetime(data[column], utc=True, errors="coerce")
    selected = data[data["label_id"].astype(str) == contract.label_id].copy()
    if selected.empty:
        raise ValueError(f"No rows found for label_id={contract.label_id!r}")
    duplicate = int(selected.duplicated(keys, keep=False).sum())
    decision = selected[contract.decision_time_column]
    entry = selected[contract.entry_time_column]
    exit_time = selected[contract.exit_time_column]
    available = selected[contract.available_time_column]
    if contract.require_entry_after_decision:
        entry_bad = int((entry <= decision).fillna(True).sum())
    else:
        entry_bad = int((entry < decision).fillna(True).sum())
    exit_bad = int((exit_time <= entry).fillna(True).sum())
    available_bad = int((available < exit_time).fillna(True).sum())
    actual_seconds = (exit_time - entry).dt.total_seconds()
    expected_seconds = float(contract.horizon_minutes * 60)
    horizon_bad = int(
        ((actual_seconds - expected_seconds).abs() > contract.horizon_tolerance_seconds).fillna(True).sum()
    )
    return PitAudit(
        signal_rows=0,
        signal_available_time_missing=0,
        signal_after_decision=0,
        label_rows=int(len(selected)),
        duplicate_label_keys=duplicate,
        entry_not_after_decision=entry_bad,
        exit_not_after_entry=exit_bad,
        label_available_before_exit=available_bad,
        horizon_mismatch=horizon_bad,
    )


def validate_signal_frame(
    signals: pd.DataFrame,
    *,
    decision_time_column: str = "decision_time",
    available_time_column: str = "signal_available_time",
    strict: bool = True,
) -> PitAudit:
    if decision_time_column not in signals.columns:
        raise ValueError(f"Signal frame is missing {decision_time_column!r}")
    if available_time_column not in signals.columns:
        if strict:
            raise ValueError(
                f"Signal frame is missing {available_time_column!r}; use an explicit legacy override only for non-governed research"
            )
        return PitAudit(
            signal_rows=int(len(signals)),
            signal_available_time_missing=int(len(signals)),
            signal_after_decision=0,
            label_rows=0,
            duplicate_label_keys=0,
            entry_not_after_decision=0,
            exit_not_after_entry=0,
            label_available_before_exit=0,
            horizon_mismatch=0,
        )
    decision = pd.to_datetime(signals[decision_time_column], utc=True, errors="coerce")
    available = pd.to_datetime(signals[available_time_column], utc=True, errors="coerce")
    return PitAudit(
        signal_rows=int(len(signals)),
        signal_available_time_missing=int(available.isna().sum()),
        signal_after_decision=int((available > decision).fillna(False).sum()),
        label_rows=0,
        duplicate_label_keys=0,
        entry_not_after_decision=0,
        exit_not_after_entry=0,
        label_available_before_exit=0,
        horizon_mismatch=0,
    )
