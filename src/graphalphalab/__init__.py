"""GraphAlphaLab: governed graph signals, labels, purity and Alpha evaluation."""

from .alpha import AlphaResult, evaluate_alpha
from .batch import BATCH_REGISTRY, BatchSpec
from .contracts import LabelContract, PitAudit
from .gff_export import ExportSummary, export_gff_signals
from .labels import LabelSpec, build_forward_return_labels
from .metadata import MetadataProfile, normalize_metadata, profile_metadata
from .purity import PurityResult, evaluate_theme_purity

__all__ = [
    "AlphaResult",
    "BATCH_REGISTRY",
    "BatchSpec",
    "ExportSummary",
    "LabelContract",
    "LabelSpec",
    "MetadataProfile",
    "PitAudit",
    "PurityResult",
    "build_forward_return_labels",
    "evaluate_alpha",
    "evaluate_theme_purity",
    "export_gff_signals",
    "normalize_metadata",
    "profile_metadata",
]
