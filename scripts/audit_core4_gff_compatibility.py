from __future__ import annotations

import argparse
import json
from pathlib import Path

from graphalphalab.core4_execution_semantics import load_core4_campaign_contract
from graphalphalab.dual_theme_common import (
    DEFAULT_SCOPES,
    DEFAULT_THEME_FAMILIES,
    discover_dual_theme_partitions,
    validate_partition_inventory,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Audit a GraphFactorFactory_v2 Core4 induced-global campaign before "
            "GraphAlphaLab export/reporting."
        )
    )
    parser.add_argument("--gff-campaign-root", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    semantic_audit = load_core4_campaign_contract(args.gff_campaign_root)
    partitions = discover_dual_theme_partitions(
        args.gff_campaign_root,
        theme_families=DEFAULT_THEME_FAMILIES,
        scopes=DEFAULT_SCOPES,
    )
    inventory = validate_partition_inventory(
        partitions,
        semantic_audit["contract_payload"],
        theme_families=DEFAULT_THEME_FAMILIES,
        scopes=DEFAULT_SCOPES,
    )
    result = {
        **{key: value for key, value in semantic_audit.items() if key != "contract_payload"},
        "partition_inventory": inventory,
        "partition_count": len(partitions),
        "gal_report_compatible": True,
        "interpretation": (
            "Within-Theme is induced from governed Global final edges. GAL may rank "
            "stocks locally inside context_theme_id, but must not describe the graph "
            "estimator as local."
        ),
    }
    text = json.dumps(result, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text + "\n", encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
