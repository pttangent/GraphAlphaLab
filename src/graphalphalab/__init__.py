"""GraphAlphaLab: governed graph signals, P1 structure, labels, purity and Alpha evaluation."""

from .alpha import AlphaResult, evaluate_alpha
from .batch import BATCH_REGISTRY, BatchSpec
from .campaign import CampaignSource, build_campaign_report
from .contracts import LabelContract, PitAudit
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
    "ExportSummary",
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
    "export_gff_signals",
    "normalize_metadata",
    "profile_metadata",
    "write_p1_report_bundle",
]
