from __future__ import annotations

from pathlib import Path

from graphalphalab.dual_theme_global_dag import (
    _preflight_cache_file,
    _preflight_cache_read,
    _preflight_cache_write,
)


def test_preflight_cache_hit_miss_and_corruption(tmp_path: Path) -> None:
    path = _preflight_cache_file(tmp_path, "scope_inventory_global.json")
    assert _preflight_cache_read(path, "key-a") is None
    _preflight_cache_write(path, "key-a", {"factor_keys": ["factor_id"], "identities": [], "signal_columns": ["score"]})
    assert _preflight_cache_read(path, "key-a") == {
        "factor_keys": ["factor_id"],
        "identities": [],
        "signal_columns": ["score"],
    }
    assert _preflight_cache_read(path, "key-b") is None
    path.write_text("{not json", encoding="utf-8")
    assert _preflight_cache_read(path, "key-a") is None
