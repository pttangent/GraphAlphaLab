from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path
from typing import Any


CORE4_INDUCED_GLOBAL_CAMPAIGN_VERSION = (
    "SMI_DUAL_THEME_IGC_FULL_SCOPE_COMPARE_V2_INDUCED_WITHIN"
)
CORE4_INDUCED_GLOBAL_MODE = "induced_global_final_edges"


@dataclass(frozen=True)
class ScopeExecutionSemantics:
    scope: str
    graph_estimation_scope: str
    edge_selection_scope: str
    ranking_scope: str
    execution_unit: str
    canonical_name: str
    is_local_graph_estimate: bool

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


CORE4_SCOPE_EXECUTION_SEMANTICS: dict[str, ScopeExecutionSemantics] = {
    "global": ScopeExecutionSemantics(
        scope="global",
        graph_estimation_scope="global_market",
        edge_selection_scope="global_market",
        ranking_scope="global_stock_cross_section",
        execution_unit="stock",
        canonical_name="global_graph_global_rank",
        is_local_graph_estimate=False,
    ),
    "within_theme": ScopeExecutionSemantics(
        scope="within_theme",
        graph_estimation_scope="global_market",
        edge_selection_scope="same_context_theme_induced_from_global_final_edges",
        ranking_scope="within_context_theme_stock_cross_section",
        execution_unit="stock",
        canonical_name="induced_global_graph_local_rank",
        is_local_graph_estimate=False,
    ),
    "inter_theme": ScopeExecutionSemantics(
        scope="inter_theme",
        graph_estimation_scope="global_market",
        edge_selection_scope="cross_theme_aggregation_from_global_final_edges",
        ranking_scope="inter_theme_cross_section",
        execution_unit="theme_portfolio",
        canonical_name="global_graph_inter_theme_rank",
        is_local_graph_estimate=False,
    ),
}


def _campaign_version(payload: dict[str, Any]) -> str:
    registry = payload.get("registry") or {}
    contract = payload.get("campaign_contract") or {}
    return str(
        registry.get("campaign_version")
        or contract.get("campaign_version")
        or payload.get("campaign_implementation_version")
        or ""
    )


def _required_false(semantics: dict[str, Any], name: str) -> None:
    if semantics.get(name) is not False:
        raise ValueError(
            f"Core4 campaign requires {name}=false for induced-global semantics; "
            f"observed={semantics.get(name)!r}"
        )


def load_core4_campaign_contract(
    campaign_root: str | Path,
    *,
    require_complete: bool = True,
) -> dict[str, Any]:
    root = Path(campaign_root).expanduser().resolve()
    contract_path = root / "runs" / "campaign_contract.json"
    if not contract_path.exists():
        raise FileNotFoundError(f"Missing GFF campaign contract: {contract_path}")
    if require_complete and not (root / "_SUCCESS").exists():
        raise FileNotFoundError(f"GFF campaign is not complete: {root / '_SUCCESS'}")

    payload = json.loads(contract_path.read_text(encoding="utf-8"))
    registry = payload.get("registry")
    contract = payload.get("campaign_contract")
    dates = payload.get("dates")
    if not isinstance(registry, dict) or not isinstance(contract, dict):
        raise ValueError(f"Malformed Core4 campaign contract: {contract_path}")
    if not isinstance(dates, list) or not dates:
        raise ValueError("Core4 campaign contract must contain a non-empty dates list")

    version = _campaign_version(payload)
    if version != CORE4_INDUCED_GLOBAL_CAMPAIGN_VERSION:
        raise ValueError(
            f"Unsupported Core4 campaign version {version!r}; expected "
            f"{CORE4_INDUCED_GLOBAL_CAMPAIGN_VERSION!r}"
        )

    within = registry.get("within_theme_semantics")
    if not isinstance(within, dict):
        raise ValueError("Core4 registry is missing within_theme_semantics")
    if within.get("mode") != CORE4_INDUCED_GLOBAL_MODE:
        raise ValueError(
            f"Wrong Within-Theme mode: {within.get('mode')!r}; expected "
            f"{CORE4_INDUCED_GLOBAL_MODE!r}"
        )
    if within.get("source") != "governed_global_p0_final_edges":
        raise ValueError("Within-Theme source is not governed Global P0 final edges")
    for key in (
        "local_residualization",
        "local_candidate_generation",
        "local_lag_selection",
        "local_top_k_or_degree_cap",
    ):
        _required_false(within, key)
    if within.get("edge_weight_policy") != "preserve_global_edge_weight":
        raise ValueError("Within-Theme does not preserve Global edge weights")
    if within.get("edge_identity_policy") != "preserve_global_edge_key":
        raise ValueError("Within-Theme does not preserve Global edge identity")
    if within.get("p1_policy") != "rebuild_p1_from_the_induced_edge_graph":
        raise ValueError("Within-Theme P1 policy is not induced-graph rebuild")

    counts = registry.get("scope_contract_counts")
    if not isinstance(counts, dict):
        raise ValueError("Core4 registry is missing scope_contract_counts")
    expected_counts = {
        "global": 26,
        "momentum_state_within_theme": 20,
        "momentum_state_inter_theme": 20,
        "residual_return_within_theme": 20,
        "residual_return_inter_theme": 20,
    }
    mismatches = {
        key: {"expected": expected, "observed": counts.get(key)}
        for key, expected in expected_counts.items()
        if int(counts.get(key, -1)) != expected
    }
    if mismatches:
        raise ValueError(f"Unexpected Core4 scope contract counts: {mismatches}")

    interface_warning: str | None = None
    interface_path = root / "gal_interface" / "gal_interface.json"
    if interface_path.exists():
        interface = json.loads(interface_path.read_text(encoding="utf-8"))
        supported = set(str(value) for value in interface.get("supported_scope_types", []))
        if "theme_local" in supported:
            interface_warning = (
                "GFF GAL interface exposes the legacy label 'theme_local'. GAL must "
                "override that label with the campaign contract: the estimator is "
                "Global and only the edge selection/ranking context is theme-scoped."
            )

    return {
        "campaign_root": str(root),
        "campaign_contract_path": str(contract_path),
        "campaign_version": version,
        "within_theme_mode": CORE4_INDUCED_GLOBAL_MODE,
        "date_count": len(dates),
        "start_date": min(str(value) for value in dates),
        "end_date": max(str(value) for value in dates),
        "scope_contract_counts": expected_counts,
        "factor_count_per_horizon": sum(expected_counts.values()) * 3,
        "scope_semantics": {
            scope: semantics.as_dict()
            for scope, semantics in CORE4_SCOPE_EXECUTION_SEMANTICS.items()
        },
        "interface_warning": interface_warning,
        "contract_payload": payload,
    }


def semantics_for_scope(scope: str) -> ScopeExecutionSemantics:
    try:
        return CORE4_SCOPE_EXECUTION_SEMANTICS[str(scope)]
    except KeyError as exc:
        raise ValueError(
            f"Unsupported scope {scope!r}; supported={tuple(CORE4_SCOPE_EXECUTION_SEMANTICS)}"
        ) from exc


def annotate_scope_frame(frame: Any, scope_column: str = "scope") -> Any:
    """Attach non-ambiguous computation and execution semantics to a pandas frame."""
    if frame is None or getattr(frame, "empty", True):
        return frame
    if scope_column not in frame.columns:
        raise ValueError(f"Frame is missing scope column {scope_column!r}")
    result = frame.copy()
    mapping = {
        column: {
            scope: getattr(semantics, column)
            for scope, semantics in CORE4_SCOPE_EXECUTION_SEMANTICS.items()
        }
        for column in (
            "graph_estimation_scope",
            "edge_selection_scope",
            "ranking_scope",
            "execution_unit",
            "canonical_name",
            "is_local_graph_estimate",
        )
    }
    for column, values in mapping.items():
        result[column] = result[scope_column].astype(str).map(values)
    return result
