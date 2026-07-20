"""GraphAlphaLab: labels, purity, alpha evaluation and compact reports."""

from .batch import BATCH_REGISTRY, BatchSpec
from .metadata import MetadataProfile, normalize_metadata, profile_metadata
from .purity import PurityResult, evaluate_theme_purity
from .alpha import AlphaResult, evaluate_alpha

__all__ = [
    "AlphaResult",
    "BATCH_REGISTRY",
    "BatchSpec",
    "MetadataProfile",
    "PurityResult",
    "evaluate_alpha",
    "evaluate_theme_purity",
    "normalize_metadata",
    "profile_metadata",
]
