from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

from .dual_theme_common import (
    DEFAULT_SCOPES,
    DEFAULT_THEME_FAMILIES,
    DUAL_THEME_ALPHA_VERSION,
    DUAL_THEME_BATCH_ID,
    DUAL_THEME_EXPORT_VERSION,
    SUPPORTED_VARIANTS,
    DualThemeExportSummary,
    HorizonSpec,
    P0Partition,
    discover_dual_theme_partitions,
    factor_id,
    load_gff_campaign_contract,
    load_horizon_manifest,
    parse_factor_id,
    validate_partition_inventory,
)
from .dual_theme_export import export_dual_theme_signals as _export_dual_theme_signals
from .dual_theme_reporting import _matched_variant_comparison
from .dual_theme_resumable import run_dual_theme_alpha_campaign


def _normalized_strings(values: Iterable[str]) -> list[str]:
    return list(
        dict.fromkeys(
            str(value).strip() for value in values if str(value).strip()
        )
    )


def _guard_existing_export(
    output_root: str | Path,
    *,
    campaign_root: str | Path,
    batch_id: str,
    theme_families: Iterable[str],
    scopes: Iterable[str],
    variants: Iterable[str],
    force: bool,
) -> None:
    manifest_path = Path(output_root).expanduser().resolve() / "export_manifest.json"
    if force or not manifest_path.exists():
        return
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    version = str(
        payload.get("export_version")
        or payload.get("parameters", {}).get("version")
        or ""
    )
    if version != DUAL_THEME_EXPORT_VERSION:
        raise ValueError(
            f"Existing signal export uses {version!r}, expected "
            f"{DUAL_THEME_EXPORT_VERSION!r}. Use --force only after confirming "
            "the old export can be replaced."
        )
    parameters = payload.get("parameters", {})
    expected = {
        "batch_id": str(batch_id),
        "campaign_root": str(Path(campaign_root).expanduser().resolve()),
        "theme_families": _normalized_strings(theme_families),
        "scopes": _normalized_strings(scopes),
        "variants": _normalized_strings(variants),
    }
    observed = {
        "batch_id": str(parameters.get("batch_id") or ""),
        "campaign_root": str(parameters.get("campaign_root") or ""),
        "theme_families": list(parameters.get("theme_families") or []),
        "scopes": list(parameters.get("scopes") or []),
        "variants": list(parameters.get("variants") or []),
    }
    if observed != expected:
        raise ValueError(
            "Existing signal export parameters do not match this run; refusing "
            f"stale checkpoint reuse. expected={expected}, observed={observed}"
        )


def export_dual_theme_signals(
    campaign_root: str | Path,
    output_root: str | Path,
    *,
    batch_id: str = DUAL_THEME_BATCH_ID,
    theme_families: Iterable[str] = DEFAULT_THEME_FAMILIES,
    scopes: Iterable[str] = DEFAULT_SCOPES,
    variants: Iterable[str] = SUPPORTED_VARIANTS,
    force: bool = False,
    **kwargs: object,
) -> DualThemeExportSummary:
    families = tuple(theme_families)
    selected_scopes = tuple(scopes)
    selected_variants = tuple(variants)
    _guard_existing_export(
        output_root,
        campaign_root=campaign_root,
        batch_id=batch_id,
        theme_families=families,
        scopes=selected_scopes,
        variants=selected_variants,
        force=force,
    )
    return _export_dual_theme_signals(
        campaign_root,
        output_root,
        batch_id=batch_id,
        theme_families=families,
        scopes=selected_scopes,
        variants=selected_variants,
        force=force,
        **kwargs,
    )


__all__ = [
    "DEFAULT_SCOPES",
    "DEFAULT_THEME_FAMILIES",
    "DUAL_THEME_ALPHA_VERSION",
    "DUAL_THEME_BATCH_ID",
    "DUAL_THEME_EXPORT_VERSION",
    "SUPPORTED_VARIANTS",
    "DualThemeExportSummary",
    "HorizonSpec",
    "P0Partition",
    "discover_dual_theme_partitions",
    "export_dual_theme_signals",
    "factor_id",
    "load_gff_campaign_contract",
    "load_horizon_manifest",
    "parse_factor_id",
    "run_dual_theme_alpha_campaign",
    "validate_partition_inventory",
]
