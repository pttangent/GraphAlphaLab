[CmdletBinding()]
param(
    [string]$RepoRoot = "D:\DEV\AnotherNetworkFactory\GraphAlphaLab",
    [string]$GffCampaignRoot = "D:\DEV\AnotherNetworkFactory\warehouses\GFF_warehouse\campaign=c4_260105_260722_induced_v2",
    [string]$BarsRoot = "D:\DEV\AnotherNetworkFactory\warehouses\NFF_warehouse\canonical\bars_1m\schema=v1",
    [string]$OutputRoot = "C:\GAL",
    [string]$StartDate = "2026-04-01",
    [string]$EndDate = "2026-07-22",
    [string]$Metadata = "",
    [ValidateSet("core", "extended")]
    [string]$DailyLabelProfile = "core",
    [string]$RollingSchedule = "5:1,10:1,15:2,20:2,30:5",
    [string]$PromotionWindows = "15,20,30",
    [int]$MinDirectionTrainDates = 15,
    [int]$DirectionRollingDates = 30,
    [double]$MinCoverageRatio = 0.80,
    [int]$FactorWorkers = 24,
    [int]$MemoryLimitGb = 120,
    [int]$Threads = 24,
    [int]$MinCrossSection = 100,
    [int]$MinThemeSize = 5,
    [int]$MinThemeCrossSection = 5,
    [string]$ExpectedCommit = "auto",
    [switch]$SkipInstall,
    [switch]$SkipTests,
    [switch]$ForceExport
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$ExpectedBranch = "agent/induced-global-frequency-gates"
$CampaignTag = "c4_260401_260722_induced_v2"

function Assert-Exit([string]$Step) {
    if ($LASTEXITCODE -ne 0) { throw "$Step failed with exit code $LASTEXITCODE" }
}
function Require-Path([string]$Path, [string]$Label) {
    if (-not (Test-Path $Path)) { throw "Missing $Label`: $Path" }
}

Require-Path $RepoRoot "GAL repository"
Require-Path $GffCampaignRoot "GFF campaign"
Require-Path (Join-Path $GffCampaignRoot "_SUCCESS") "GFF campaign _SUCCESS"
Require-Path (Join-Path $GffCampaignRoot "runs\campaign_contract.json") "GFF campaign contract"
Require-Path $BarsRoot "NFF canonical bars"
if ($Metadata) { Require-Path $Metadata "metadata" }
if ($Threads -lt $FactorWorkers) { throw "Threads must be >= FactorWorkers" }
if (($MemoryLimitGb / $FactorWorkers) -lt 4) { throw "MemoryLimitGb must provide at least 4GB per worker" }
if ($FactorWorkers -ne 24) { Write-Warning "Canonical configuration is 24 workers; observed $FactorWorkers" }

$SignalsOutput = Join-Path $OutputRoot "signals\$CampaignTag"
$LabelOutput = Join-Path $OutputRoot "labels\$CampaignTag"
$ReportOutput = Join-Path $OutputRoot "reports\$CampaignTag"
$RollingOutput = Join-Path $ReportOutput "rolling_alpha"
$DiscussionOutput = Join-Path $ReportOutput "discussion_pack"
$TempDirectory = Join-Path $OutputRoot "tmp\$CampaignTag"
foreach ($Path in @($OutputRoot, $SignalsOutput, $LabelOutput, $ReportOutput, $TempDirectory)) {
    New-Item -ItemType Directory -Force -Path $Path | Out-Null
}

Push-Location $RepoRoot
try {
    if (git status --porcelain) { throw "GAL worktree must be clean before the governed run" }
    git fetch origin $ExpectedBranch
    Assert-Exit "git fetch"
    git switch $ExpectedBranch
    Assert-Exit "git switch"
    git pull --ff-only origin $ExpectedBranch
    Assert-Exit "git pull --ff-only"
    $Branch = (git branch --show-current).Trim()
    if ($Branch -ne $ExpectedBranch) { throw "Expected branch $ExpectedBranch, got $Branch" }
    $Head = (git rev-parse HEAD).Trim()
    Assert-Exit "git rev-parse"
    if ($ExpectedCommit -eq "auto") { $ExpectedCommit = $Head }
    elseif ($ExpectedCommit -ne $Head) { throw "HEAD mismatch expected=$ExpectedCommit actual=$Head" }

    if (-not $SkipInstall) {
        python -m pip install -e ".[test]"
        Assert-Exit "editable install"
    }
    python -m compileall -q src scripts tests
    Assert-Exit "compileall"
    if (-not $SkipTests) {
        python -m pytest -q --tb=short
        Assert-Exit "pytest"
    }

    $ArgsList = @(
        "scripts/run_core4_260401_260722_24w.py",
        "--gff-campaign-root", $GffCampaignRoot,
        "--bars-root", $BarsRoot,
        "--output-root", $OutputRoot,
        "--start-date", $StartDate,
        "--end-date", $EndDate,
        "--daily-label-profile", $DailyLabelProfile,
        "--rolling-schedule", $RollingSchedule,
        "--promotion-windows", $PromotionWindows,
        "--min-direction-train-dates", "$MinDirectionTrainDates",
        "--direction-rolling-dates", "$DirectionRollingDates",
        "--min-coverage-ratio", "$MinCoverageRatio",
        "--factor-workers", "$FactorWorkers",
        "--memory-limit-gb", "$MemoryLimitGb",
        "--threads", "$Threads",
        "--min-cross-section", "$MinCrossSection",
        "--min-theme-size", "$MinThemeSize",
        "--min-theme-cross-section", "$MinThemeCrossSection",
        "--expected-git-commit", $ExpectedCommit,
        "--require-clean"
    )
    if ($Metadata) { $ArgsList += @("--metadata", $Metadata) }
    if ($ForceExport) { $ArgsList += "--force-export" }
    python @ArgsList
    Assert-Exit "Core4 combined intraday/daily Alpha"

    foreach ($Path in @(
        (Join-Path $SignalsOutput "_SUCCESS"),
        (Join-Path $SignalsOutput "export_manifest.json"),
        (Join-Path $LabelOutput "combined_horizon_manifest.json"),
        (Join-Path $LabelOutput "combined_label_manifest.json"),
        (Join-Path $LabelOutput "intraday\_SUCCESS"),
        (Join-Path $LabelOutput "daily_next_open\_SUCCESS"),
        (Join-Path $ReportOutput "_SUCCESS"),
        (Join-Path $ReportOutput "_checkpoints\dual_theme_alpha\global_dag_progress.json"),
        (Join-Path $RollingOutput "_SUCCESS"),
        (Join-Path $RollingOutput "rolling_scope_summary.csv"),
        (Join-Path $RollingOutput "rolling_factor_stability.csv"),
        (Join-Path $RollingOutput "rolling_promotion_candidates.csv"),
        (Join-Path $RollingOutput "rolling_shard_index.csv"),
        (Join-Path $DiscussionOutput "_SUCCESS"),
        (Join-Path $DiscussionOutput "overview\factor_master_compact.csv"),
        (Join-Path $DiscussionOutput "overview\factor_master_summary.csv"),
        (Join-Path $DiscussionOutput "overview\full_period_scope_horizon_summary.csv"),
        (Join-Path $DiscussionOutput "overview\top_bottom_factors_by_horizon_scope.csv"),
        (Join-Path $DiscussionOutput "factor_shard_index.csv"),
        (Join-Path $DiscussionOutput "UPLOAD_MANIFEST.csv"),
        (Join-Path $DiscussionOutput "README_UPLOAD.md")
    )) { Require-Path $Path "governed output" }

    Write-Host ""
    Write-Host "=== CORE4 24-WORKER GLOBAL DAG COMPLETE ==="
    Write-Host "Branch: $Branch"
    Write-Host "Commit: $Head"
    Write-Host "GFF input: $GffCampaignRoot"
    Write-Host "Analysis dates: $StartDate through $EndDate"
    Write-Host "Output root: $OutputRoot"
    Write-Host "Factor workers: $FactorWorkers"
    Write-Host "Scope barrier: none"
    Write-Host "Horizon barrier: none"
    Write-Host "Intraday horizons: 5/15/30/60/120/180m"
    Write-Host "Daily entry: next trading-session open"
    Write-Host "Rolling schedule: $RollingSchedule"
    Write-Host "Promotion windows: $PromotionWindows"
    Write-Host "Discussion upload pack: $DiscussionOutput"
}
finally { Pop-Location }
