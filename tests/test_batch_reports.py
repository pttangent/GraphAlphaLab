from __future__ import annotations

import json
import pandas as pd
import pytest

from graphalphalab.batch import merge_compact_reports, validate_batch_contracts


def test_batch_contract_requires_explicit_partial() -> None:
    frame = pd.DataFrame({"layer_id": ["A"], "scale_minutes": [15]})
    with pytest.raises(ValueError):
        validate_batch_contracts(frame, "implemented27")
    status = validate_batch_contracts(frame, "implemented27", allow_partial=True)
    assert status["partial"]


def test_compact_merge_does_not_need_raw_partitions(tmp_path) -> None:
    roots = []
    for batch in ("implemented27", "remaining14"):
        root = tmp_path / batch
        root.mkdir()
        (root / "summary.json").write_text(json.dumps({"batch_id": batch, "batch_status": {"partial": False}}), encoding="utf-8")
        pd.DataFrame({
            "batch_id": [batch],
            "factor_id": [batch],
            "mean_spearman_ic": [0.02],
            "net_sharpe_5bps": [1.0],
            "fdr_pass": [True],
        }).to_csv(root / "alpha_metrics.csv", index=False)
        roots.append(root)
    output = merge_compact_reports(roots, tmp_path / "all41")
    summary = json.loads((output / "summary.json").read_text(encoding="utf-8"))
    assert summary["factor_count"] == 2
    assert summary["complete"]
