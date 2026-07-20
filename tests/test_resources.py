from pathlib import Path
import pytest
from graph_alpha_lab.resources import enforce


def test_disk_gate_fails_for_impossible_threshold(tmp_path: Path):
    with pytest.raises(RuntimeError, match="Disk gate failed"):
        enforce(tmp_path, 0, 10**18)
