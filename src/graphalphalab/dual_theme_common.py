from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Iterable

from .contracts import LabelContract


DUAL_THEME_BATCH_ID = "dual_theme_igc"
DUAL_THEME_EXPORT_VERSION = "GAL_DUAL_THEME_GFF_EXPORT_V1"
DUAL_THEME_ALPHA_VERSION = "GAL_DUAL_THEME_MULTI_HORIZON_ALPHA_V1"
DEFAULT_THEME_FAMILIES = ("momentum_state", "residual_return")
DEFAULT_SCOPES = ("global", "within_theme", "inter_theme")
SUPPORTED_VARIANTS = ("node_baseline", "graph_forward", "graph_reverse_placebo")


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


def _scope_membership_path(campaign_root: Path, theme_family: str, trade_date: str) -> Path:
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
        (campaign_root / "graphs" / "scope_index" / f"theme_family={theme_family}").rglob(
            f"date={trade_date}/memberships.parquet"
        )
    )
    if len(matches) != 1:
        raise FileNotFoundError(
            f"Expected one scope membership file for family={theme_family}, date={trade_date}; found={matches}"
        )
    return matches[0]


def discover_dual_theme_partitions(
    campaign_root: str | Path,
    *,
    theme_families: Iterable[str] = DEFAULT_THEME_FAMILIES,
    scopes: Iterable[str] = DEFAULT_SCOPES,
) -> tuple[P0Partition, ...]:
    root = Path(campaign_root).expanduser().resolve()
    if not root.exists():
        raise FileNotFoundError(root)
    selected_families = tuple(dict.fromkeys(str(value).strip() for value in theme_families if str(value).strip()))
    selected_scopes = tuple(dict.fromkeys(str(value).strip() for value in scopes if str(value).strip()))
    unknown_scopes = sorted(set(selected_scopes) - set(DEFAULT_SCOPES))
    if unknown_scopes:
        raise ValueError(f"Unsupported scopes: {unknown_scopes}")
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
                membership = None
                if scope == "inter_theme":
                    if not trade_date:
                        raise ValueError(f"Cannot infer trade_date from {edges}")
                    membership = _scope_membership_path(root, family, trade_date)
                rows.append(P0Partition(scope, family, edges, nodes, membership))
    if not rows:
        raise FileNotFoundError(f"No dual-theme GFF P0 partitions found below {root}")
    return tuple(rows)
