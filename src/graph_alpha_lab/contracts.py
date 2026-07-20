from __future__ import annotations

from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Iterable
import hashlib
import json


@dataclass(frozen=True)
class ResearchConfig:
    warehouse_root: Path
    output_root: Path
    variant: str
    dates: tuple[str, ...]
    workers: int = 16
    duckdb_threads: int = 3
    memory_limit_gb: int = 72
    min_free_disk_gb: int = 200
    min_free_ram_gb: int = 32
    min_price: float = 5.0
    min_dollar_volume_15m: float = 5_000_000.0
    min_regular_trades_15m: int = 100
    max_abs_ret1: float = 0.10
    horizons: tuple[int, ...] = (5, 15, 30)
    resume: bool = True

    def fingerprint(self) -> str:
        payload = asdict(self)
        payload["warehouse_root"] = str(self.warehouse_root.resolve())
        payload["output_root"] = str(self.output_root.resolve())
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(raw).hexdigest()


def stable_hash(parts: Iterable[str]) -> str:
    return hashlib.sha256("\n".join(parts).encode()).hexdigest()
