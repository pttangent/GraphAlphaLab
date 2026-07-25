from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Iterable

from .contracts import LabelContract


DUAL_THEME_BATCH_ID = "dual_theme_igc"
DUAL_THEME_EXPORT_VERSION = "GAL_DUAL_THEME_GFF_EXPORT_V2_SCOPE_SEMANTICS"
DUAL_THEME_ALPHA_VERSION = "GAL_DUAL_THEME_MULTI_HORIZON_ALPHA_V2_SCOPE_SEMANTICS"
DEFAULT_THEME_FAMILIES = ("momentum_state", "residual_return")
DEFAULT_SCOPES = ("global", "within_theme", "inter_theme")
SUPPORTED_VARIANTS = ("node_baseline", "graph_forward", "graph_reverse_placebo")
SUPPORTED_GFF_CAMPAIGN_VERSIONS = (
    "SMI_DUAL_THEME_IGC_FULL_SCOPE_COMPARE_V1",
    "SMI_DUAL_THEME_IGC_FULL_SCOPE_COMPARE_V2_INDUCED_WITHIN",
)
INDUCED_WITHIN_GFF_VERSION = "SMI_DUAL_THEME_IGC_FULL_SCOPE_COMPARE_V2_INDUCED_WITHIN"


@dataclass(frozen=True)
class HorizonSpec:
    name: str
    labels: Path
    label_contract: Path

    def as_dict(self) -> dict[str, str]:
        return {
            "name": self.name,
            "labels": str(self.labels),
            "label_contract": str(self.label_contract),
        }


@dataclass(frozen=True)
class P0Partition:
    scope: str
    theme_family: str
    edges: Path
    nodes: Path
    scope_memberships: Path | None = None


@dataclass(frozen=True)
class DualThemeExportSummary:
    partition_count: int
    output_file_count: int
    output_rows: int
    factor_count: int
    edge_pit_violations: int
    output_root: str
    counts_by_scope_family: dict[str, int]

    def as_dict(self) -> dict[str, object]:
        return {
            "partition_count": self.partition_count,
            "output_file_count": self.output_file_count,
            "output_rows": self.output_rows,
            "factor_count": self.factor_count,
            "edge_pit_violations": self.edge_pit_violations,
            "output_root": self.output_root,
            "counts_by_scope_family": dict(self.counts_by_scope_family),
        }


def _sql_path(path: Path) -> str:
    return path.expanduser().resolve().as_posix().replace("'", "''")


def _path_value(path: Path, key: str) -> str | None:
    prefix = f"{key}="
    for part in path.parts:
        if part.startswith(prefix):
            return part[len(prefix) :]
    return None


def _theme_id_expression(alias: str) -> str:
    return f"replace(CAST({alias}.symbol AS VARCHAR), 'THEME::', '')"


def factor_id(scope: str, theme_family: str, layer_id: str, variant_id: str) -> str:
    return "::".join((str(scope), str(theme_family), str(layer_id), str(variant_id)))


def parse_factor_id(value: object) -> dict[str, str]:
    parts = str(value).split("::", 3)
    if len(parts) != 4:
        return {"scope": "legacy", "theme_family": "legacy"}
    return {
        "scope": parts[0],
        "theme_family": parts[1],
        "factor_layer": parts[2],
        "factor_variant": parts[3],
    }


def load_horizon_manifest(path: str | Path) -> tuple[HorizonSpec, ...]:
    manifest_path = Path(path).expanduser().resolve()
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    raw_rows = payload.get("horizons") if isinstance(payload, dict) else payload
    if not isinstance(raw_rows, list) or not raw_rows:
        raise ValueError("Horizon manifest must contain a non-empty 'horizons' list")
    rows: list[HorizonSpec] = []
    names: set[str] = set()
    for raw in raw_rows:
        if not isinstance(raw, dict):
            raise ValueError(f"Invalid horizon row: {raw!r}")
        labels = Path(str(raw["labels"])).expanduser().resolve()
        contract = Path(str(raw["label_contract"])).expanduser().resolve()
        if not labels.exists():
            raise FileNotFoundError(labels)
        if not contract.exists():
            raise FileNotFoundError(contract)
        label_contract = LabelContract.from_json(contract)
        name = str(raw.get("name") or f"{label_contract.horizon_minutes}m").strip()
        if not name or name in names:
            raise ValueError(f"Duplicate or empty horizon name: {name!r}")
        names.add(name)
        rows.append(HorizonSpec(name, labels, contract))
    return tuple(rows)


def _scope_membership_path(
    campaign_root: Path,
    theme_family: str,
    trade_date: str,
) -> Path:
    exact = (
        campaign_root
        / "graphs"
        / "scope_index"
        / f"theme_family={theme_family}"
        / "date_scope_index"
        / f"date={trade_date}"
        / "memberships.parquet"
    )
    if exact.exists():
        return exact
    matches = sorted(
        (
            campaign_root
            / "graphs"
            / "scope_index"
            / f"theme_family={theme_family}"
        ).rglob(f"date={trade_date}/memberships.parquet")
    )
    if len(matches) != 1:
        raise FileNotFoundError(
            f"Expected one scope membership file for family={theme_family}, "
            f"date={trade_date}; found={matches}"
        )
    return matches[0]


def _validate_induced_within_registry(registry: dict[str, object]) -> None:
    semantics = registry.get("within_theme_semantics")
    if not isinstance(semantics, dict):
        raise ValueError("Core4 V2 registry is missing within_theme_semantics")
    required = {
        "mode": "induced_global_final_edges",
        "source": "governed_global_p0_final_edges",
        "local_residualization": False,
        "local_candidate_generation": False,
        "local_lag_selection": False,
        "local_top_k_or_degree_cap": False,
        "edge_weight_policy": "preserve_global_edge_weight",
        "edge_identity_policy": "preserve_global_edge_key",
        "p1_policy": "rebuild_p1_from_the_induced_edge_graph",
    }
    mismatches = {
        key: {"expected": expected, "observed": semantics.get(key)}
        for key, expected in required.items()
        if semantics.get(key) != expected
    }
    if mismatches:
        raise ValueError(f"Unsupported induced Within-Theme semantics: {mismatches}")
    scope_policy = registry.get("igc_scope_policy")
    if not isinstance(scope_policy, dict):
        raise ValueError("Core4 V2 registry is missing igc_scope_policy")
    if "aggregate_cross-theme edges" not in str(scope_policy.get("inter_theme", "")):
        raise ValueError("Core4 V2 Inter-Theme scope policy is not the governed aggregation")


def load_gff_campaign_contract(campaign_root: str | Path) -> dict[str, object]:
    root = Path(campaign_root).expanduser().resolve()
    path = root / "runs" / "campaign_contract.json"
    if not path.exists():
        raise FileNotFoundError(f"Missing governed GFF campaign contract: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    contract = payload.get("campaign_contract")
    registry = payload.get("registry")
    dates = payload.get("dates")
    if not isinstance(contract, dict) or not isinstance(registry, dict) or not isinstance(dates, list):
        raise ValueError(f"Malformed GFF campaign contract: {path}")
    version = str(
        registry.get("campaign_version")
        or contract.get("campaign_version")
        or payload.get("campaign_implementation_version")
        or ""
    )
    if version not in SUPPORTED_GFF_CAMPAIGN_VERSIONS:
        raise ValueError(
            f"Unsupported GFF campaign version {version!r}; "
            f"supported={SUPPORTED_GFF_CAMPAIGN_VERSIONS}"
        )
    consensus = registry.get("consensus", {})
    if isinstance(consensus, dict) and bool(consensus.get("enabled")):
        raise ValueError("Dual-theme GAL input must not enable Consensus")
    families = tuple(str(value) for value in registry.get("theme_family_order", ()))
    if families and families != DEFAULT_THEME_FAMILIES:
        raise ValueError(f"Unexpected Theme families: {families}")
    if version == INDUCED_WITHIN_GFF_VERSION:
        _validate_induced_within_registry(registry)
    return {
        "path": path,
        "payload": payload,
        "contract": contract,
        "registry": registry,
        "dates": dates,
        "campaign_version": version,
    }


def validate_partition_inventory(
    partitions: Iterable[P0Partition],
    campaign_contract: dict[str, object],
    *,
    theme_families: Iterable[str],
    scopes: Iterable[str],
) -> dict[str, object]:
    selected_families = tuple(theme_families)
    selected_scopes = tuple(scopes)
    rows = tuple(partitions)
    counts: dict[str, int] = {}
    for row in rows:
        key = f"{row.scope}|{row.theme_family}"
        counts[key] = counts.get(key, 0) + 1
    registry = campaign_contract["registry"]
    dates = campaign_contract["dates"]
    if not isinstance(registry, dict) or not isinstance(dates, list):
        raise ValueError("Invalid parsed GFF campaign contract")
    date_count = len(dates)
    if date_count < 1:
        raise ValueError("GFF campaign contract has no dates")
    expected_counts = registry.get("scope_contract_counts", {})
    if not isinstance(expected_counts, dict):
        raise ValueError("GFF registry is missing scope_contract_counts")
    expected: dict[str, int] = {}
    if "global" in selected_scopes:
        expected["global|shared_global"] = int(expected_counts.get("global", 0)) * date_count
    for family in selected_families:
        if "within_theme" in selected_scopes:
            expected[f"within_theme|{family}"] = int(
                expected_counts.get(f"{family}_within_theme", 0)
            ) * date_count
        if "inter_theme" in selected_scopes:
            expected[f"inter_theme|{family}"] = int(
                expected_counts.get(f"{family}_inter_theme", 0)
            ) * date_count
    mismatches = {
        key: {"expected": value, "observed": counts.get(key, 0)}
        for key, value in expected.items()
        if value <= 0 or counts.get(key, 0) != value
    }
    if mismatches:
        raise ValueError(
            f"Incomplete or unexpected dual-theme P0 inventory: {mismatches}"
        )
    return {
        "date_count": date_count,
        "expected_counts": expected,
        "observed_counts": counts,
        "complete": True,
    }


def discover_dual_theme_partitions(
    campaign_root: str | Path,
    *,
    theme_families: Iterable[str] = DEFAULT_THEME_FAMILIES,
    scopes: Iterable[str] = DEFAULT_SCOPES,
) -> tuple[P0Partition, ...]:
    root = Path(campaign_root).expanduser().resolve()
    if not root.exists():
        raise FileNotFoundError(root)
    selected_families = tuple(
        dict.fromkeys(
            str(value).strip() for value in theme_families if str(value).strip()
        )
    )
    selected_scopes = tuple(
        dict.fromkeys(str(value).strip() for value in scopes if str(value).strip())
    )
    unknown_scopes = sorted(set(selected_scopes) - set(DEFAULT_SCOPES))
    if unknown_scopes:
        raise ValueError(f"Unsupported scopes: {unknown_scopes}")
    unknown_families = sorted(set(selected_families) - set(DEFAULT_THEME_FAMILIES))
    if unknown_families:
        raise ValueError(f"Unsupported Theme families: {unknown_families}")
    rows: list[P0Partition] = []
    if "global" in selected_scopes:
        global_root = root / "graphs" / "scope=global" / "batch=IGC_DUAL_THEME_COMPARE"
        for edges in sorted(global_root.rglob("edges.parquet")):
            if "p0" not in edges.parts:
                continue
            nodes = edges.with_name("node_projection.parquet")
            if nodes.exists():
                rows.append(P0Partition("global", "shared_global", edges, nodes))
    for scope in ("within_theme", "inter_theme"):
        if scope not in selected_scopes:
            continue
        for family in selected_families:
            scoped_root = root / "graphs" / f"scope={scope}" / f"theme_family={family}"
            for edges in sorted(scoped_root.rglob("edges.parquet")):
                if "p0" not in edges.parts:
                    continue
                nodes = edges.with_name("node_projection.parquet")
                if not nodes.exists():
                    continue
                trade_date = _path_value(edges, "date")
                if not trade_date:
                    raise ValueError(f"Cannot infer trade_date from {edges}")
                membership = _scope_membership_path(root, family, trade_date)
                rows.append(P0Partition(scope, family, edges, nodes, membership))
    if not rows:
        raise FileNotFoundError(f"No dual-theme GFF P0 partitions found below {root}")
    return tuple(rows)
