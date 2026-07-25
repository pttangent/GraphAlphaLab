# Core4 V2 Dual-Theme Alpha Runbook

本手册用于让本地 agent 将 `GraphFactorFactory_v2` 的 Core4 V2 结果交给
`GraphAlphaLab`，完成 Global、Within-Theme、Inter-Theme 三种不同金融语义的
多 horizon Alpha 计算。

## 1. 固定仓库与输入身份

### GFF

```text
Repository: ptangent/GraphFactorFactory_v2
Branch: agent/core4-batched-halfyear
Required campaign version:
SMI_DUAL_THEME_IGC_FULL_SCOPE_COMPARE_V2_INDUCED_WITHIN
Expected campaign root:
D:\DEV\AnotherNetworkFactory\warehouses\GFF_warehouse\campaign=c4_260105_260722_induced_v2
```

### GAL

```text
Repository: ptangent/GraphAlphaLab
Branch: agent/dual-theme-alpha-reporting
```

不得从 GFF 的临时目录、`_pending`、失败目录或单个 partition 手工拼接报告。
GAL 只接受带有：

```text
_SUCCESS
runs/campaign_contract.json
完整 scope inventory
有效 partition manifest
```

的 campaign。

## 2. 先检查 GFF 实际日期，不要相信脚本文件名

当前 GFF 分支中的 `scripts/run_core4_20260105_20260722.ps1` 文件名表示
`2026-01-05..2026-07-22`，但当前脚本内容实际固定为：

```text
2026-07-01..2026-07-22
15 XNYS sessions
3303 DAG tasks
```

因此在 Alpha 前必须读取 campaign contract。若实际首日不是 `2026-01-05`，
不得把该结果描述为半年 run；先回到 GFF 修正日期、补跑并完成 `_SUCCESS`。

在 PowerShell 执行：

```powershell
$CampaignRoot = "D:\DEV\AnotherNetworkFactory\warehouses\GFF_warehouse\campaign=c4_260105_260722_induced_v2"
$ContractPath = Join-Path $CampaignRoot "runs\campaign_contract.json"

if (-not (Test-Path (Join-Path $CampaignRoot "_SUCCESS"))) {
    throw "GFF campaign is incomplete: missing _SUCCESS"
}
if (-not (Test-Path $ContractPath)) {
    throw "Missing campaign contract: $ContractPath"
}

$Contract = Get-Content $ContractPath -Raw | ConvertFrom-Json
$Version = $Contract.registry.campaign_version
$Dates = @($Contract.dates)

if ($Version -ne "SMI_DUAL_THEME_IGC_FULL_SCOPE_COMPARE_V2_INDUCED_WITHIN") {
    throw "Wrong GFF campaign version: $Version"
}
if ($Contract.registry.within_theme_semantics.mode -ne "induced_global_final_edges") {
    throw "Wrong Within-Theme mode"
}
if ($Contract.registry.within_theme_semantics.local_residualization -ne $false) {
    throw "Unexpected local Within-Theme residualization"
}
if ($Dates.Count -lt 20) {
    throw "Fewer than 20 sessions: diagnostic only, not promotion evidence"
}

Write-Host "Actual first date: $($Dates[0])"
Write-Host "Actual last date:  $($Dates[-1])"
Write-Host "Actual sessions:   $($Dates.Count)"
```

半年目标必须额外通过：

```powershell
if ($Dates[0] -ne "2026-01-05" -or $Dates[-1] -ne "2026-07-22") {
    throw "Campaign is not the requested 2026-01-05..2026-07-22 range"
}
```

## 3. 三种 Alpha 的金融语义

### Global Alpha

单位是股票。每个 decision time 在全市场股票横截面上比较 graph score 与未来收益。

### Within-Theme Alpha

单位仍是股票，但只能在同一 PIT theme 内比较：

1. 使用该 decision time 已知的 canonical theme membership。
2. 在 `decision_time × context_theme_id` 内对 graph score 排名。
3. 在同一 theme 内使用 `own_score` 做横截面残差化。
4. 从每只股票未来收益中减去该 theme 的成员平均未来收益。
5. 计算的是“同一主题内部谁相对更强”，不是主题涨跌方向。
6. 小于 `MinThemeSize` 的 theme 不进入计算。

这避免把板块整体上涨误当成 theme 内选股 Alpha。

### Inter-Theme Alpha

单位是主题组合，不是股票：

1. GFF 的 Inter-Theme 节点分数只在主题层级有意义。
2. GAL 将同一主题成员股票的未来收益按 PIT `membership_weight` 聚合为一个主题组合未来收益。
3. 每个 decision time、每个 theme 只能有一个 score 和一个 target return。
4. 横截面排序发生在 themes 之间。
5. 同一主题广播到成员股票的 score 若不完全一致，立即失败，禁止任意 tie-break。
6. `MinThemeCrossSection` 控制每个 decision 至少需要多少个可比较主题。

Inter-Theme 的 turnover 与 1/2/5/10 bps 成本目前是主题名义仓位诊断。
正式可交易结论还必须加入成分股换手、membership migration、流动性与篮子执行成本。

## 4. PIT 与标签契约

每个 horizon 必须有独立 label contract。标签至少包含：

```text
trade_date
decision_time
symbol_id
label_id
target_return
entry_time
exit_time
label_available_time
```

硬性规则：

```text
signal_available_time <= decision_time
同一 label_id 下 (trade_date, decision_time, symbol_id) 不重复
entry_time > decision_time
exit_time > entry_time
label_available_time >= exit_time
exit_time - entry_time 与 horizon_minutes 一致
```

不得用当日收盘后才知道的信息回填盘中 signal。不得将未来形成的 theme membership
应用到过去 decision time。Overlapping labels 不允许报告有效 annualized Sharpe。

从 `examples/dual_theme_horizons.example.json` 复制 horizon manifest，并替换为真实路径。
建议至少计算：

```text
5m, 15m, 30m, 60m, 120m, 180m
```

## 5. 安装与代码验收

```powershell
cd D:\DEV\AnotherNetworkFactory\GraphAlphaLab

git fetch origin agent/dual-theme-alpha-reporting
git switch agent/dual-theme-alpha-reporting
git pull --ff-only origin agent/dual-theme-alpha-reporting

if (git status --porcelain) {
    throw "GAL working tree must be clean"
}

python -m pip install -e ".[test]"
python -m compileall -q src scripts
python -m pytest -q --tb=short
if ($LASTEXITCODE -ne 0) {
    throw "GAL tests failed"
}
```

## 6. 正式执行

先设定路径：

```powershell
$CampaignRoot = "D:\DEV\AnotherNetworkFactory\warehouses\GFF_warehouse\campaign=c4_260105_260722_induced_v2"
$HorizonManifest = "D:\DEV\AnotherNetworkFactory\GraphAlphaLab\contracts\dual_theme_horizons.json"
$SignalsOutput = "D:\DEV\AnotherNetworkFactory\warehouses\GAL_warehouse\signals\c4_260105_260722_induced_v2"
$ReportOutput = "D:\DEV\AnotherNetworkFactory\warehouses\GAL_warehouse\reports\c4_260105_260722_induced_v2"
$Metadata = "D:\DEV\AnotherNetworkFactory\warehouses\NFF_warehouse\reference\symbol_metadata.parquet"
$TempDirectory = "D:\GAL_duckdb_tmp"
```

执行：

```powershell
.\scripts\run_dual_theme_alpha_20260102.ps1 `
  -GffCampaignRoot $CampaignRoot `
  -HorizonManifest $HorizonManifest `
  -SignalsOutput $SignalsOutput `
  -ReportOutput $ReportOutput `
  -Metadata $Metadata `
  -MemoryLimitGb 64 `
  -Threads 12 `
  -TempDirectory $TempDirectory `
  -MinCrossSection 100 `
  -MinThemeSize 5 `
  -MinThemeCrossSection 5
```

说明：

- 脚本名称保留旧的 one-day 名称，但参数支持完整 campaign。
- 正式 run 不使用 `-AllowPartial`。
- 正常续跑不使用 `-ForceExport`；已有且受治理的导出文件会复用。
- 只有确认 GFF campaign 尚未完成且只是 schema 诊断时，才允许
  `-AllowPartial` 或 `-SkipCleanCheck`，且结果不得进入正式比较。
- GAL 的 `expected-git-commit` 固定 GAL 自己的 clean HEAD；GFF lineage
  由 `campaign_contract.json` 与各 partition manifest 记录。

## 7. 必须存在的输出

Signal export：

```text
signals\export_manifest.json
signals\_SUCCESS
```

总报告：

```text
reports\global_alpha_metrics.csv
reports\within_theme_alpha_metrics.csv
reports\inter_theme_alpha_metrics.csv
reports\cross_scope_comparison.csv
reports\scope_family_horizon_summary.csv
reports\matched_variant_comparison.csv
reports\ranking.csv
reports\summary.json
reports\REPORT.md
reports\_SUCCESS
```

每个 horizon：

```text
reports\horizon=<name>\alpha_metrics.csv
reports\horizon=<name>\daily_ic.csv
reports\horizon=<name>\portfolio_returns.csv
reports\horizon=<name>\quantile_returns.csv
reports\horizon=<name>\stability_slices.csv
reports\horizon=<name>\summary.json
reports\horizon=<name>\_SUCCESS
```

## 8. 最终验收

运行后执行：

```powershell
$Export = Get-Content (Join-Path $SignalsOutput "export_manifest.json") -Raw | ConvertFrom-Json
$Summary = Get-Content (Join-Path $ReportOutput "summary.json") -Raw | ConvertFrom-Json

if ($Export.export_version -ne "GAL_DUAL_THEME_GFF_EXPORT_V2_SCOPE_SEMANTICS") {
    throw "Wrong GAL export semantics"
}
if ($Export.gff_campaign_version -ne "SMI_DUAL_THEME_IGC_FULL_SCOPE_COMPARE_V2_INDUCED_WITHIN") {
    throw "Wrong upstream GFF campaign"
}
if ([int]$Export.edge_pit_violations -ne 0) {
    throw "GFF signal PIT violations detected"
}
if ($Summary.complete -ne $true) {
    throw "GAL report is partial"
}
foreach ($scope in @("global", "within_theme", "inter_theme")) {
    $Path = Join-Path $ReportOutput "${scope}_alpha_metrics.csv"
    if (-not (Test-Path $Path)) {
        throw "Missing scope output: $Path"
    }
}
```

人工审核必须回答：

1. Global、Within、Inter 是否均有非空 graph-forward、node baseline、reverse placebo。
2. Within 的结果是否来自 theme-demeaned return，而不是全市场收益。
3. Inter 是否每个 decision/theme 只有一个组合收益观测。
4. Graph-forward 是否稳定优于 node baseline 与 reverse placebo。
5. IC 是否跨日期稳定，而非由单日或尾盘支配。
6. 5 bps 后是否仍存活。
7. Momentum 与 residual-return 两个 theme family 的结论是否一致或互补。
8. direction 是否为事前声明；`default-direction=auto` 只能做探索，不能直接晋升。
9. 至少 20 个交易日后才允许进入统计筛选；半年结果仍需 walk-forward、regime、
   liquidity 与执行成本反证。

## 9. 本地 agent 的停止条件

出现任何一项立即停止，不要以 `AllowPartial` 绕过：

```text
GFF 缺少 _SUCCESS
campaign version 不符
实际日期范围与任务描述不符
scope inventory 不完整
edge PIT violation > 0
标签 PIT audit 失败
Inter 同主题广播 score 不一致
任一正式 horizon 缺少 factor
GAL 输出为 _PARTIAL
```

停止时报告具体文件、scope、theme family、layer、scale、date 与错误计数，不要删除
有效 checkpoint，不要重新计算 GFF 图，不要把诊断结果描述为生产 Alpha。
