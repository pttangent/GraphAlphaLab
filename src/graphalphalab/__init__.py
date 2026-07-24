"""GraphAlphaLab: governed graph signals, dual-theme scopes, labels, purity and Alpha evaluation."""

from .alpha import AlphaResult, evaluate_alpha
from .batch import BATCH_REGISTRY, BatchSpec
from .campaign import CampaignSource, build_campaign_report
from .contracts import LabelContract, PitAudit
from .dual_theme import (
    DUAL_THEME_BATCH_ID,
    DualThemeExportSummary,
    HorizonSpec,
    export_dual_theme_signals,
    run_dual_theme_alpha_campaign,
)
from .gff_export import ExportSummary, export_gff_signals
from .labels import LabelSpec, build_forward_return_labels
from .metadata import MetadataProfile, normalize_metadata, profile_metadata
from .p1_reporting import P1ReportResult, evaluate_p1_streaming, write_p1_report_bundle
from .purity import PurityResult, evaluate_theme_purity

__all__ = [
    "AlphaResult",
    "BATCH_REGISTRY",
    "BatchSpec",
    "CampaignSource",
    "DUAL_THEME_BATCH_ID",
    "DualThemeExportSummary",
    "ExportSummary",
    "HorizonSpec",
    "LabelContract",
    "LabelSpec",
    "MetadataProfile",
    "P1ReportResult",
    "PitAudit",
    "PurityResult",
    "build_campaign_report",
    "build_forward_return_labels",
    "evaluate_alpha",
    "evaluate_p1_streaming",
    "evaluate_theme_purity",
    "export_dual_theme_signals",
    "export_gff_signals",
    "normalize_metadata",
    "profile_metadata",
    "run_dual_theme_alpha_campaign",
    "write_p1_report_bundle",
]
