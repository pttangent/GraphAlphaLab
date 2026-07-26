# Core4 July Campaign (c4_260701_260722_induced_v2) — 本机运行环境适配、治理妥协与修改要求

- 日期: 2026-07-26
- GAL pinned: `agent/dual-theme-alpha-reporting` @ `2d6902b74ee694f252ed7cac099e1b31143494b7`(未改一行代码)
- GFF 输入: `D:\G4C\campaign=c4_260701_260722_induced_v2`(15 个 XNYS 交易日, 2026-07-01 ~ 2026-07-22)
- 输出: `D:\DEV\AnotherNetworkFactory\warehouses\GFF_warehouse\GAL_alpha\campaign=c4_260701_260722_induced_v2\{signals,reports}`

本文件记录为让 pinned commit 在这台机器上完成正式 run 所做的全部环境适配与治理妥协, 以及对代码的修改要求。**GAL 源码零改动**, 所有适配都在仓库之外。

---

## 1) 为本机环境做的调整(含路径)

| 项 | 路径 | 原因 |
|---|---|---|
| 独立 venv | `D:\GAL\venv311` (Python 3.11 + pandas 2.3.3 + duckdb 1.5.5) | 系统默认 `C:\Python314` 装的是 pandas 3.0.1, `streaming.py:316` / `alpha.py:449` 的 `stack(dropna=False)` 在 pandas 3.0 下直接 ValueError。pandas 2.3.3 符合 pyproject `pandas>=2.2,<4`。venv 下 29 tests 全过 |
| 受治理 labels | `D:\GAL\labels\campaign=c4_260701_260722_induced_v2\forward_{5,15,30,60,120,180}m\date=<d>\data.parquet` | 仓库内外均无现成 forward-return labels。用 `graphalphalab.labels.build_forward_return_labels` 从 NFF 1m bars (`D:\DEV\AnotherNetworkFactory\warehouses\NFF_warehouse\canonical\bars_1m\schema=v1\date=*`) 构建; decision grid 取自 GFF campaign edges(76×5min, 13:45–20:00 UTC, 逐日验证); 每个 horizon 过 `validate_label_frame` 审计 |
| Label 构建脚本 | `D:\GAL\scripts\build_labels_c4_260701_260722_induced_v2.py` | 含 grid 校验、horizon tolerance ±60s 过滤、metadata symbol_id 富化 |
| Horizon manifest | `D:\GAL\contracts\dual_theme_horizons.c4_260701_260722_induced_v2.json` | runbook 建议放 repo `contracts/`, 但该目录未 gitignore, 写入会破坏 runner 的 clean-tree 检查, 故放仓库外 |
| Label contracts | `D:\GAL\contracts\forward_{5,15,30,60,120,180}m.json` | LabelContract JSON(horizon_minutes, entry_lag=1, tolerance=60s, overlapping=true) |
| 富化 metadata | `D:\GAL\metadata\symbol_metadata.with_symbol_id.parquet` | runbook 指定的 `NFF_warehouse\reference\symbol_metadata.parquet` 不存在; 实际源 `warehouses\metadata\symbol_metadata.parquet` 无 `symbol_id` 列, 用 NFF bars 的 symbol→symbol_id 映射富化(4715/5002 匹配) |
| 等价 run wrapper | `D:\GAL\scripts\run_gal_alpha_c4_260701_260722.ps1` | repo 的 `run_dual_theme_alpha_20260102.ps1` 不支持 `--correlation-sample-modulus`, 手工 wrapper 复制其全部治理检查(branch/commit/clean/_SUCCESS/contract/py_compile/输出验收)后直调 CLI |
| 监控/看门狗 | `D:\GAL\scripts\monitor_gal.cmd`, `D:\GAL\scripts\watchdog_gal.ps1`, 日志 `D:\GAL\logs\` | 后台运行与无人值守验收 |
| quarantine 迁出 | `D:\G4C_cache\quarantine_purge\campaign=c4_260701_260722_induced_v2\...` | 见 2.4 |

## 2) 为治理现况做的妥协

### 2.1 `--correlation-sample-modulus 0`(跳过 score_correlation 诊断)
- **原因**: `streaming.py:316`(及 `alpha.py:449`)固有 bug——`corr()` 结果 index/columns 同名 `factor_key`, `stack()` 后层级重名, `reset_index()` 必抛 `cannot insert factor_key, already exists`。与 pandas 版本无关, ≥2 factors 必现, 说明该路径从未被真正执行成功过(测试 factor 数 <2 不触发)。
- **影响**: `score_correlation.csv` 为空; `reports.py` 的 REPORT.md 模板声称它 "rather than an empty placeholder", 文案与实际不符。`summary.complete` 及全部受治理必需输出不受影响(complete 只取决于 factor 数齐全)。
- **补救**: owner 修复并推新 commit 后, 按 runbook 2.3 节确认新 ExpectedCommit, 用默认 modulus=1000 重跑一次补齐。

### 2.2 绕过 repo ps1 wrapper
- **原因**: ps1 不透传 `--correlation-sample-modulus`。
- **补偿**: 手工 wrapper 逐项复制 ps1 的治理检查(见 1 表)。未使用 `-AllowPartial` / `-ForceExport` / `-SkipCleanCheck`。

### 2.3 metadata 路径与 runbook 不一致
- runbook 路径不存在, 改用 `warehouses\metadata` 并自行富化 `symbol_id`。287 个 symbol 无 NFF 映射, 未进入 slices。

### 2.4 _quarantine 残留导致 inventory 校验失败
- `graphs\scope=global\...\p0\date=2026-07-07\layer=momentum_state_to_return\scale=60\_quarantine\` 内有损坏 `edges.parquet`(magic bytes 缺失)且带 `node_projection.parquet` + `_SUCCESS`, GAL `discover_dual_theme_partitions` 会将其计入, 导致 global 391 vs 期望 390。
- **处理**: 经操作者批准, 整体**移动**(非删除)到 `D:\G4C_cache\quarantine_purge\`。正式 partition(带 `_SUCCESS`)完好。这构成对 `D:\G4C` 输入目录的一次改动, 已记录。

## 3) 修改要求(请 owner 处理)

1. **pandas 兼容**: 修复 `streaming.py:316` / `alpha.py:449` 的 `stack(dropna=False)` 用法以兼容 pandas 3; 或在 pyproject 收窄 `pandas>=2.2,<3` 直至适配完成。
2. **score-correlation bug**: 同上两行, `.stack(...)` 后改 `.rename_axis(["factor_a", "factor_b"]).reset_index()`; 增加 ≥2 factors 触发相关性块的回归测试。
3. **最细粒度 checkpoint(重点)**: 当前 export 阶段已有 partition×variant 文件级续算, 但 **alpha 阶段完全没有 checkpoint**——`dual_theme_reporting.py:608` 的 horizon 循环无条件重算, 崩溃一次损失全部 alpha 工时(本次约 85 分钟/次)。请求:
   - horizon 级: `if (horizon_output / "_SUCCESS").exists() and not force: continue`, 并在最终合并阶段从已有 bundle 读回 metrics;
   - 更细一级(可选): `_run_governed_scope_alpha` 内按 scope(global/within/inter)落 checkpoint;
   - 最终合并报表逻辑与单 horizon 已有 bundle 的复用需要一并设计, 保证合并输出仍由同一 governed run 产出。
4. **discovery 健壮性**: `discover_dual_theme_partitions` 应跳过 `_quarantine` / `_pending` / `_failed` / `_locks` 等 `_` 前缀目录(或显式校验 `_SUCCESS`), 避免把失败残留计入正式 inventory。
5. **ps1 wrapper 参数透传**: `run_dual_theme_alpha_20260102.ps1` 建议暴露 `-CorrelationSampleModulus`(默认 1000), 避免为传参绕过 wrapper。

## 附: 两次失败时间线

| 时间 | 事件 |
|---|---|
| 04:56 | attempt 1 启动(C:\Python314, pandas 3.0.1); 05:25 export 完成(4770 文件, 318 factors, 8.79 亿行, edge PIT=0) |
| 06:26 | attempt 1 崩于 `stack(dropna=)`(pandas 3)→ 建 venv311 |
| 06:30 | attempt 2 启动(venv311); 07:24 越过前崩溃点 |
| 07:55 | attempt 2 崩于 `reset_index` 重名层级(固有 bug)→ 加 modulus=0 |
| 10:06 | attempt 3 启动(venv311 + modulus=0 + 手工 wrapper) |
