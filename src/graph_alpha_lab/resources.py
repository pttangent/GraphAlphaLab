from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import shutil
import psutil


@dataclass(frozen=True)
class ResourceSnapshot:
    available_ram_gb: float
    free_disk_gb: float
    cpu_percent: float


def snapshot(path: Path) -> ResourceSnapshot:
    vm = psutil.virtual_memory()
    disk = shutil.disk_usage(path if path.exists() else path.parent)
    return ResourceSnapshot(vm.available / 1024**3, disk.free / 1024**3, psutil.cpu_percent(interval=0.1))


def enforce(path: Path, min_free_ram_gb: int, min_free_disk_gb: int) -> ResourceSnapshot:
    state = snapshot(path)
    if state.available_ram_gb < min_free_ram_gb:
        raise RuntimeError(f"RAM gate failed: {state.available_ram_gb:.1f}GB available < {min_free_ram_gb}GB")
    if state.free_disk_gb < min_free_disk_gb:
        raise RuntimeError(f"Disk gate failed: {state.free_disk_gb:.1f}GB free < {min_free_disk_gb}GB")
    return state
