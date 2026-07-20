import numpy as np
import pandas as pd
from graph_alpha_lab.statistics import cross_sectional_residual, snapshot_metrics


def test_residual_removes_linear_control():
    x = pd.Series(np.arange(100, dtype=float))
    y = 3 * x + 7
    residual = cross_sectional_residual(y, pd.DataFrame({"x": x}))
    assert np.nanmax(np.abs(residual)) < 1e-8


def test_snapshot_metrics_positive_signal():
    x = pd.Series(np.arange(100, dtype=float))
    result = snapshot_metrics(x, x)
    assert result["n"] == 100
    assert result["rank_ic"] > 0.99
    assert result["spread"] > 0
