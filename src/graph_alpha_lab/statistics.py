from __future__ import annotations

import numpy as np
import pandas as pd


def rank_pct(values: pd.Series) -> pd.Series:
    return values.rank(method="average", pct=True)


def cross_sectional_residual(y: pd.Series, controls: pd.DataFrame, min_obs: int = 30) -> pd.Series:
    frame = pd.concat([y.rename("y"), controls], axis=1).replace([np.inf, -np.inf], np.nan).dropna()
    out = pd.Series(np.nan, index=y.index, dtype=float)
    if len(frame) < min_obs:
        return out
    x = np.column_stack([np.ones(len(frame)), frame.drop(columns="y").to_numpy(dtype=float)])
    beta, *_ = np.linalg.lstsq(x, frame["y"].to_numpy(dtype=float), rcond=None)
    out.loc[frame.index] = frame["y"].to_numpy(dtype=float) - x @ beta
    return out


def snapshot_metrics(signal: pd.Series, target: pd.Series) -> dict[str, float | int]:
    frame = pd.DataFrame({"signal": signal, "target": target}).replace([np.inf, -np.inf], np.nan).dropna()
    if len(frame) < 50 or frame.signal.nunique() < 5:
        return {"n": len(frame), "rank_ic": np.nan, "pearson_ic": np.nan, "spread": np.nan}
    s, t = rank_pct(frame.signal), rank_pct(frame.target)
    lo, hi = s.quantile([0.1, 0.9])
    return {"n": len(frame), "rank_ic": float(s.corr(t)), "pearson_ic": float(frame.signal.corr(frame.target)), "spread": float(frame.loc[s >= hi, "target"].mean() - frame.loc[s <= lo, "target"].mean())}
