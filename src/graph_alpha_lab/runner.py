from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
import argparse
import json
import traceback

import duckdb
import pandas as pd

from .contracts import ResearchConfig, stable_hash
from .inventory import discover_partitions
from .resources import enforce, snapshot
from .statistics import cross_sectional_residual, snapshot_metrics


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, default=str), encoding="utf-8")


def _dates_from_month(month: str, root: Path) -> tuple[str, ...]:
    return tuple(sorted(p.name.split("=", 1)[1] for p in (root / "nff" / "gff_core").glob("date=*") if p.name.split("=", 1)[1].startswith(month)))


def build_alpha_base(config: ResearchConfig, day: str, run_dir: Path) -> Path:
    out = run_dir / "cache" / "alpha_base" / f"date={day}" / "data.parquet"
    success = out.parent / "_SUCCESS"
    if config.resume and out.exists() and success.exists():
        return out
    enforce(run_dir, config.min_free_ram_gb, config.min_free_disk_gb)
    gff = config.warehouse_root / "nff" / "gff_core" / f"date={day}" / "data.parquet"
    trades = config.warehouse_root / "nff" / "trades_core" / f"date={day}" / "data.parquet"
    if not gff.exists() or not trades.exists():
        raise FileNotFoundError(f"Missing NFF input for {day}: {gff} / {trades}")
    future = []
    for h in config.horizons:
        future += [
            f"lead(last_trade_price,{h}) over(partition by symbol_id order by decision_time)/nullif(last_trade_price,0)-1 AS fwd_{h}m",
            f"abs(lead(last_trade_price,{h}) over(partition by symbol_id order by decision_time)/nullif(last_trade_price,0)-1) AS fwd_abs_{h}m",
        ]
    out.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"PRAGMA threads={config.duckdb_threads}")
    con.execute(f"PRAGMA memory_limit='{config.memory_limit_gb}GB'")
    sql = f"""
    WITH b AS (
      SELECT g.*,t.last_trade_price,t.dollar_volume_15m,t.regular_trades_15m,{','.join(future)}
      FROM read_parquet('{gff.as_posix()}') g JOIN read_parquet('{trades.as_posix()}') t USING(decision_time,symbol_id)
    ) SELECT * FROM b
    """
    con.execute(f"COPY ({sql}) TO '{out.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    con.close()
    success.write_text("", encoding="utf-8")
    return out


def _fingerprint(config: ResearchConfig, row: dict) -> str:
    return stable_hash([config.fingerprint(), row.get("p0_manifest_hash") or "", row.get("p1_manifest_hash") or ""])


def _run_partition(payload: tuple[dict, dict, str]) -> dict:
    raw_config, row, run_dir_raw = payload
    raw_config["warehouse_root"], raw_config["output_root"] = Path(raw_config["warehouse_root"]), Path(raw_config["output_root"])
    config, run_dir = ResearchConfig(**raw_config), Path(run_dir_raw)
    day, layer, scale = row["trade_date"], row["layer"], int(row["scale"])
    out = run_dir / "shards" / f"date={day}" / f"layer={layer}" / f"scale={scale}" / "experiment=hierarchy"
    out.mkdir(parents=True, exist_ok=True)
    fingerprint = _fingerprint(config, row)
    manifest = out / "manifest.json"
    if config.resume and (out / "_SUCCESS").exists() and manifest.exists() and json.loads(manifest.read_text()).get("fingerprint") == fingerprint:
        return {"status": "reused", "trade_date": day, "layer": layer, "scale": scale}
    enforce(run_dir, config.min_free_ram_gb, config.min_free_disk_gb)
    base = run_dir / "cache" / "alpha_base" / f"date={day}" / "data.parquet"
    node = Path(row["p0_path"]) / "node_projection.parquet"
    edges = Path(row["p0_path"]) / "edges.parquet"
    memberships = Path(row["p1_path"]) / "memberships.parquet"
    con = duckdb.connect()
    con.execute(f"PRAGMA threads={config.duckdb_threads}")
    con.execute(f"PRAGMA memory_limit='{max(4, config.memory_limit_gb // max(config.workers,1))}GB'")
    node_cols = [x[0] for x in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{node.as_posix()}')").fetchall()]
    score = "node_score" if "node_score" in node_cols else next((c for c in node_cols if c.endswith("score")), None)
    if not score:
        raise RuntimeError(f"No score column in {node}")
    query = f"""
    WITH n AS (SELECT decision_time,symbol_id,{score}::DOUBLE node_signal FROM read_parquet('{node.as_posix()}')),
    e2 AS (
      SELECT decision_time,src_symbol_id symbol_id,dst_symbol_id neighbor_id,abs(edge_weight) w FROM read_parquet('{edges.as_posix()}')
      UNION ALL SELECT decision_time,dst_symbol_id,src_symbol_id,abs(edge_weight) FROM read_parquet('{edges.as_posix()}')
    ),
    graph AS (
      SELECT e2.decision_time,e2.symbol_id,sum(w*n.node_signal)/nullif(sum(w),0) graph_signal
      FROM e2 JOIN n ON e2.decision_time=n.decision_time AND e2.neighbor_id=n.symbol_id GROUP BY 1,2
    ),
    m0 AS (
      SELECT m.decision_time,m.symbol_id,m.theme_id,coalesce(m.core_score,1.0) w,n.node_signal
      FROM read_parquet('{memberships.as_posix()}') m JOIN n USING(decision_time,symbol_id)
      WHERE coalesce(m.canonical_eligible,true)
    ),
    ta AS (SELECT decision_time,theme_id,sum(w*node_signal) ss,sum(w) sw,count(*) nn FROM m0 GROUP BY 1,2),
    theme AS (
      SELECT m0.decision_time,m0.symbol_id,avg((ta.ss-m0.w*m0.node_signal)/nullif(ta.sw-m0.w,0)) theme_signal
      FROM m0 JOIN ta USING(decision_time,theme_id) WHERE ta.nn>1 GROUP BY 1,2
    )
    SELECT b.*,n.node_signal,graph.graph_signal,theme.theme_signal
    FROM read_parquet('{base.as_posix()}') b JOIN n USING(decision_time,symbol_id)
    LEFT JOIN graph USING(decision_time,symbol_id) LEFT JOIN theme USING(decision_time,symbol_id)
    WHERE last_trade_price>={config.min_price} AND dollar_volume_15m>={config.min_dollar_volume_15m}
      AND regular_trades_15m>={config.min_regular_trades_15m} AND abs(coalesce(own_ret1,0))<={config.max_abs_ret1}
    """
    frame = con.execute(query).fetchdf()
    con.close()
    controls = [c for c in ["own_ret1","own_ret5","own_ret15","own_flow_rank","own_ofi_rank","realized_vol_15m","amihud_1m","trade_count"] if c in frame.columns]
    records: list[dict] = []
    for decision_time, group in frame.groupby("decision_time", sort=False):
        ctrl = group[controls].rank(pct=True) if controls else pd.DataFrame(index=group.index)
        signals = {
            "node_raw": group.node_signal,
            "node_increment": cross_sectional_residual(group.node_signal.rank(pct=True), ctrl),
            "graph_raw": group.graph_signal,
            "graph_increment": cross_sectional_residual(group.graph_signal.rank(pct=True), pd.concat([ctrl,group[["node_signal"]].rank(pct=True)],axis=1)),
            "p1_raw": group.theme_signal,
            "p1_increment": cross_sectional_residual(group.theme_signal.rank(pct=True), pd.concat([ctrl,group[["node_signal","graph_signal"]].rank(pct=True)],axis=1)),
        }
        for signal_name, signal in signals.items():
            for h in config.horizons:
                for target_type, target in (("direction",group[f"fwd_{h}m"]),("risk",group[f"fwd_abs_{h}m"])):
                    records.append({"trade_date":day,"layer":layer,"scale":scale,"decision_time":decision_time,"signal":signal_name,"target_type":target_type,"horizon":h,**snapshot_metrics(signal,target)})
    result = pd.DataFrame(records)
    result.to_parquet(out / "result.parquet", index=False)
    summary = result.groupby(["signal","target_type","horizon"],as_index=False).agg(snapshots=("rank_ic","count"),mean_ic=("rank_ic","mean"),median_ic=("rank_ic","median"),positive_ic_share=("rank_ic",lambda x:float((x>0).mean())),mean_spread=("spread","mean"))
    summary.to_csv(out / "summary.csv", index=False)
    _write_json(manifest,{"fingerprint":fingerprint,"rows":len(result),"created_at":datetime.now(timezone.utc).isoformat()})
    (out / "_SUCCESS").write_text("",encoding="utf-8")
    return {"status":"completed","trade_date":day,"layer":layer,"scale":scale,"rows":len(result)}


def _dashboard(run_dir: Path, inventory: pd.DataFrame, progress: list[dict]) -> None:
    counts = pd.Series([x["status"] for x in progress]).value_counts().to_dict() if progress else {}
    state = snapshot(run_dir)
    lines = ["# GraphAlphaLab Progress","",f"Updated: {datetime.now(timezone.utc).isoformat()}","",f"Partitions: {len(inventory)}",f"Completed: {counts.get('completed',0)}",f"Reused: {counts.get('reused',0)}",f"Failed: {counts.get('failed',0)}","",f"Available RAM: {state.available_ram_gb:.1f} GB",f"Free disk: {state.free_disk_gb:.1f} GB"]
    (run_dir / "dashboard.md").write_text("\n".join(lines)+"\n",encoding="utf-8")
    _write_json(run_dir / "progress.json",{"counts":counts,"tasks":progress,"resource":asdict(state)})


def _compile_report(run_dir: Path, inventory: pd.DataFrame) -> None:
    daily = []
    for path in run_dir.glob("shards/**/summary.csv"):
        frame = pd.read_csv(path)
        bits = {p.split("=",1)[0]:p.split("=",1)[1] for p in path.parts if "=" in p}
        frame.insert(0,"trade_date",bits["date"]); frame.insert(1,"layer",bits["layer"]); frame.insert(2,"scale",int(bits["scale"])); daily.append(frame)
    if not daily:
        return
    per_day = pd.concat(daily,ignore_index=True)
    matrix = per_day.groupby(["layer","scale","signal","target_type","horizon"],as_index=False).agg(trading_days=("trade_date","nunique"),mean_ic=("mean_ic","mean"),positive_day_share=("mean_ic",lambda x:float((x>0).mean())),mean_spread=("mean_spread","mean"))
    reports = run_dir / "reports"; reports.mkdir(parents=True,exist_ok=True)
    per_day.to_csv(reports / "PER_DAY_RESULTS.csv",index=False); matrix.to_csv(reports / "PARTITION_MATRIX.csv",index=False)
    lines = ["# Monthly Alpha Report","","> In-sample research over governed NFF/P0/P1 outputs.","",f"Dates: {inventory.trade_date.nunique()}",f"Partitions: {len(inventory)}",f"Governance failures: {(inventory.governance_status!='OK').sum()}","","| Layer | Scale | Signal | Target | Horizon | Mean IC | Positive days | Spread bps |","|---|---:|---|---|---:|---:|---:|---:|"]
    for r in matrix.sort_values("mean_ic",key=lambda s:s.abs(),ascending=False).head(50).itertuples():
        lines.append(f"| {r.layer} | {r.scale} | {r.signal} | {r.target_type} | {r.horizon} | {r.mean_ic:.4f} | {r.positive_day_share:.1%} | {r.mean_spread*1e4:.2f} |")
    (reports / "MONTHLY_ALPHA_REPORT.md").write_text("\n".join(lines)+"\n",encoding="utf-8")


def run(config: ResearchConfig, only_layers: set[str] | None = None) -> Path:
    run_dir = config.output_root / f"month-{config.dates[0][:7]}-{config.variant}-{config.fingerprint()[:8]}"
    run_dir.mkdir(parents=True,exist_ok=True)
    _write_json(run_dir / "config_resolved.json",{**asdict(config),"warehouse_root":str(config.warehouse_root),"output_root":str(config.output_root),"fingerprint":config.fingerprint()})
    inventory = discover_partitions(config.warehouse_root,set(config.dates),config.variant)
    if only_layers: inventory = inventory[inventory.layer.isin(only_layers)].copy()
    inventory.to_csv(run_dir / "partition_inventory.csv",index=False)
    inventory[inventory.governance_status!="OK"].to_csv(run_dir / "governance_findings.csv",index=False)
    for day in config.dates: build_alpha_base(config,day,run_dir)
    runnable = inventory[(inventory.governance_status=="OK") & inventory.p1_success & inventory.memberships_exists]
    raw = asdict(config); raw["warehouse_root"],raw["output_root"] = str(config.warehouse_root),str(config.output_root)
    payloads = [(raw,row._asdict(),str(run_dir)) for row in runnable.itertuples(index=False)]
    progress: list[dict] = []
    with ProcessPoolExecutor(max_workers=config.workers) as pool:
        futures = {pool.submit(_run_partition,p):p for p in payloads}
        for future in as_completed(futures):
            try: progress.append(future.result())
            except Exception as exc:
                _,row,_ = futures[future]
                progress.append({"status":"failed","trade_date":row["trade_date"],"layer":row["layer"],"scale":row["scale"],"error":str(exc),"traceback":traceback.format_exc()})
            _dashboard(run_dir,inventory,progress)
    pd.DataFrame([x for x in progress if x["status"]=="failed"]).to_csv(run_dir / "failures.csv",index=False)
    _compile_report(run_dir,inventory)
    return run_dir


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run PIT-safe monthly GraphAlphaLab research")
    p.add_argument("--warehouse-root",type=Path,required=True); p.add_argument("--output-root",type=Path,default=Path("artifacts")); p.add_argument("--variant",default="candidate_v2_shadow")
    group = p.add_mutually_exclusive_group(required=True); group.add_argument("--month"); group.add_argument("--dates")
    p.add_argument("--only-layer",default=""); p.add_argument("--workers",type=int,default=16); p.add_argument("--duckdb-threads",type=int,default=3); p.add_argument("--memory-limit-gb",type=int,default=72); p.add_argument("--min-free-disk-gb",type=int,default=200); p.add_argument("--min-free-ram-gb",type=int,default=32); p.add_argument("--resume",action=argparse.BooleanOptionalAction,default=True)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    a = parse_args(argv)
    dates = tuple(x.strip() for x in a.dates.split(",")) if a.dates else _dates_from_month(a.month,a.warehouse_root)
    if not dates: raise SystemExit("No dates discovered")
    config = ResearchConfig(a.warehouse_root,a.output_root,a.variant,dates,a.workers,a.duckdb_threads,a.memory_limit_gb,a.min_free_disk_gb,a.min_free_ram_gb,resume=a.resume)
    print(run(config,set(filter(None,(x.strip() for x in a.only_layer.split(",")))) or None))
    return 0
