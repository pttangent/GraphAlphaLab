[CmdletBinding()]
param(
    [string]$RepoRoot = "D:\DEV\AnotherNetworkFactory\GraphAlphaLab",
    [string]$GffCampaignRoot = "D:\G4H\campaign=c4_260105_260722_induced_v2",
    [string]$BarsRoot = "D:\DEV\AnotherNetworkFactory\warehouses\NFF_warehouse\canonical\bars_1m\schema=v1",
    [string]$SignalsOutput = "D:\DEV\AnotherNetworkFactory\warehouses\GAL_warehouse\signals\c4_260105_260722_induced_v2",
    [string]$LabelOutput = "D:\DEV\AnotherNetworkFactory\warehouses\GAL_warehouse\labels\c4_260105_260722_daily70",
    [string]$ReportOutput = "D:\DEV\AnotherNetworkFactory\warehouses\GAL_warehouse\reports\c4_260105_260722_daily70",
    [string]$Metadata = "",
    [int]$DateCount = 70,
    [string]$EndDate = "2026-07-22",
    [ValidateSet("core", "extended")]
    [string]$LabelProfile = "core",
    [string]$RollingWindows = "20,40,60",
    [int]$RollingStep = 5,
    [int]$FactorWorkers = 6,
    [int]$MemoryLimitGb = 64,
    [int]$Threads = 12,
    [string]$TempDirectory = "D:\GAL\duckdb_tmp\c4_260105_260722_daily70",
    [string]$ExpectedCommit = "auto",
    [switch]$SkipInstall,
    [switch]$SkipTests,
    [switch]$ForceExport
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$ExpectedBranch = "agent/induced-global-frequency-gates"

function Assert-Exit([string]$Step) {
    if ($LASTEXITCODE -ne 0) { throw "$Step failed with exit code $LASTEXITCODE" }
}
function Require-Path([string]$Path, [string]$Label) {
    if (-not (Test-Path $Path)) { throw "Missing $Label: $Path" }
}

Require-Path $RepoRoot "GAL repository"
Require-Path $GffCampaignRoot "GFF campaign"
Require-Path (Join-Path $GffCampaignRoot "_SUCCESS") "GFF _SUCCESS"
Require-Path $BarsRoot "canonical bars"
if ($Metadata) { Require-Path $Metadata "metadata" }
if ($DateCount -lt 20) { throw "DateCount must be at least 20" }
if ($FactorWorkers -lt 1) { throw "FactorWorkers must be positive" }

Push-Location $RepoRoot
try {
    if (git status --porcelain) { throw "GAL worktree must be clean before the governed run" }
    git fetch origin $ExpectedBranch
    Assert-Exit "git fetch"
    git switch $ExpectedBranch
    Assert-Exit "git switch"
    git pull --ff-only origin $ExpectedBranch
    Assert-Exit "git pull"
    $Branch = (git branch --show-current).Trim()
    if ($Branch -ne $ExpectedBranch) { throw "Wrong branch: $Branch" }
    $Head = (git rev-parse HEAD).Trim()
    if ($ExpectedCommit -eq "auto") { $ExpectedCommit = $Head }
    elseif ($ExpectedCommit -ne $Head) { throw "HEAD mismatch expected=$ExpectedCommit actual=$Head" }

    if (-not $SkipInstall) {
        python -m pip install -e ".[test]"
        Assert-Exit "install"
    }
    python -m compileall -q src scripts tests
    Assert-Exit "compileall"
    if (-not $SkipTests) {
        python -m pytest -q --tb=short
        Assert-Exit "pytest"
    }

    New-Item -ItemType Directory -Force -Path $TempDirectory | Out-Null
    $ArgsList = @(
        "scripts/run_core4_daily_alpha_70d.py",
        "--gff-campaign-root", $GffCampaignRoot,
        "--bars-root", $BarsRoot,
        "--signals-output", $SignalsOutput,
        "--label-output", $LabelOutput,
        "--report-output", $ReportOutput,
        "--date-count", "$DateCount",
        "--end-date", $EndDate,
        "--label-profile", $LabelProfile,
        "--rolling-windows", $RollingWindows,
        "--rolling-step", "$RollingStep",
        "--factor-workers", "$FactorWorkers",
        "--memory-limit-gb", "$MemoryLimitGb",
        "--threads", "$Threads",
        "--temp-directory", $TempDirectory,
        "--expected-git-commit", $ExpectedCommit,
        "--require-clean"
    )
    if ($Metadata) { $ArgsList += @("--metadata", $Metadata) }
    if ($ForceExport) { $ArgsList += "--force-export" }
    python @ArgsList
    Assert-Exit "daily and rolling Alpha"

    foreach ($Path in @(
        (Join-Path $LabelOutput "_SUCCESS"),
        (Join-Path $LabelOutput "daily_horizon_manifest.json"),
        (Join-Path $LabelOutput "diagnostics\daily_label_manifest.json"),
        (Join-Path $LabelOutput "diagnostics\daily_label_coverage.csv"),
        (Join-Path $ReportOutput "_SUCCESS"),
        (Join-Path $ReportOutput "global_alpha_metrics.csv"),
        (Join-Path $ReportOutput "within_theme_alpha_metrics.csv"),
        (Join-Path $ReportOutput "inter_theme_alpha_metrics.csv"),
        (Join-Path $ReportOutput "rolling_alpha\_SUCCESS"),
        (Join-Path $ReportOutput "rolling_alpha\rolling_alpha_metrics.csv"),
        (Join-Path $ReportOutput "rolling_alpha\rolling_scope_summary.csv"),
        (Join-Path $ReportOutput "rolling_alpha\rolling_stability_summary.csv")
    )) { Require-Path $Path "governed output" }

    Write-Host ""
    Write-Host "=== DUAL-THEME DAILY 70-SESSION ALPHA COMPLETE ==="
    Write-Host "Branch: $Branch"
    Write-Host "Commit: $Head"
    Write-Host "Analysis dates: last $DateCount campaign sessions through $EndDate"
    Write-Host "Tail policy: future labels may be absent only at the trailing edge"
    Write-Host "Daily labels: $LabelOutput"
    Write-Host "Alpha: $ReportOutput"
    Write-Host "Rolling Alpha: $(Join-Path $ReportOutput 'rolling_alpha')"
}
finally { Pop-Location }
