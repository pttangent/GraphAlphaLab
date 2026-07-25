[CmdletBinding()]
param(
    [string]$RepoRoot = "D:\DEV\AnotherNetworkFactory\GraphAlphaLab",

    [string]$GffCampaignRoot = "D:\DEV\AnotherNetworkFactory\warehouses\GFF_warehouse\campaign=c4_260105_260722_induced_v2",

    [Parameter(Mandatory = $true)]
    [string]$HorizonManifest,

    [string]$SignalsOutput = "D:\DEV\AnotherNetworkFactory\warehouses\GAL_warehouse\signals\c4_260105_260722_induced_v2",

    [string]$ReportOutput = "D:\DEV\AnotherNetworkFactory\warehouses\GAL_warehouse\reports\c4_260105_260722_induced_v2",

    [string]$Metadata,
    [string]$ExpectedCommit = "auto",
    [string]$ExpectedGffCampaignVersion = "SMI_DUAL_THEME_IGC_FULL_SCOPE_COMPARE_V2_INDUCED_WITHIN",
    [string]$ExpectedStartDate = "2026-01-05",
    [string]$ExpectedEndDate = "2026-07-22",
    [int]$ExpectedSessionCount = 137,
    [int]$ExpectedFactorCount = 318,
    [int]$MemoryLimitGb = 24,
    [int]$Threads = 8,
    [string]$TempDirectory = "D:\GAL\duckdb_tmp",
    [int]$MinCrossSection = 100,
    [int]$MinThemeSize = 5,
    [int]$MinThemeCrossSection = 5,
    [switch]$ForceExport,
    [switch]$AllowPartial,
    [switch]$SkipInstall,
    [switch]$SkipTests,
    [switch]$SkipCleanCheck
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Assert-LastExitCode([string]$Step) {
    if ($LASTEXITCODE -ne 0) {
        throw "$Step failed with exit code $LASTEXITCODE"
    }
}

function Require-Path([string]$Path, [string]$Label) {
    if (-not (Test-Path $Path)) {
        throw "Missing $Label: $Path"
    }
}

$ExpectedBranch = "agent/dual-theme-alpha-reporting"
$ContractPath = Join-Path $GffCampaignRoot "runs\campaign_contract.json"
$GffSuccess = Join-Path $GffCampaignRoot "_SUCCESS"

Write-Host "=== GraphAlphaLab Core4 half-year Alpha ==="
Write-Host "GAL repo: $RepoRoot"
Write-Host "GFF campaign: $GffCampaignRoot"
Write-Host "Expected range: $ExpectedStartDate .. $ExpectedEndDate ($ExpectedSessionCount sessions)"
Write-Host "Signals: $SignalsOutput"
Write-Host "Reports: $ReportOutput"
Write-Host ""

Require-Path $RepoRoot "GAL repository"
Require-Path $GffCampaignRoot "GFF campaign root"
Require-Path $GffSuccess "GFF campaign _SUCCESS"
Require-Path $ContractPath "GFF campaign contract"
Require-Path $HorizonManifest "horizon manifest"
if ($Metadata) { Require-Path $Metadata "metadata" }

$ContractPayload = Get-Content $ContractPath -Raw | ConvertFrom-Json
$Registry = $ContractPayload.registry
$CampaignContract = $ContractPayload.campaign_contract
$GffVersion = if ($Registry.campaign_version) {
    [string]$Registry.campaign_version
} elseif ($CampaignContract.campaign_version) {
    [string]$CampaignContract.campaign_version
} else {
    [string]$ContractPayload.campaign_implementation_version
}
if ($GffVersion -ne $ExpectedGffCampaignVersion) {
    throw "Unsupported GFF campaign version '$GffVersion'; expected '$ExpectedGffCampaignVersion'."
}
if ($Registry.consensus.enabled -eq $true) {
    throw "Core4 comparison campaign unexpectedly enables Consensus."
}
if ($Registry.within_theme_semantics.mode -ne "induced_global_final_edges") {
    throw "Wrong Within-Theme mode: $($Registry.within_theme_semantics.mode)"
}
if ($Registry.within_theme_semantics.local_residualization -ne $false) {
    throw "GFF Within-Theme unexpectedly performs local residualization/re-estimation."
}
if ($Registry.within_theme_semantics.p1_policy -ne "rebuild_p1_from_the_induced_edge_graph") {
    throw "Wrong Within-Theme P1 policy: $($Registry.within_theme_semantics.p1_policy)"
}

$Dates = @($ContractPayload.dates)
if ($Dates.Count -ne $ExpectedSessionCount) {
    throw "GFF campaign contains $($Dates.Count) dates; expected $ExpectedSessionCount. The current GFF script may still be configured for 2026-07-01..2026-07-22 despite its half-year filename."
}
$SortedDates = @($Dates | ForEach-Object { [string]$_ } | Sort-Object)
if ($SortedDates[0] -ne $ExpectedStartDate -or $SortedDates[-1] -ne $ExpectedEndDate) {
    throw "GFF date range is $($SortedDates[0])..$($SortedDates[-1]); expected $ExpectedStartDate..$ExpectedEndDate."
}

$Counts = $Registry.scope_contract_counts
$PerDateContracts = [int]$Counts.global +
    [int]$Counts.momentum_state_within_theme +
    [int]$Counts.momentum_state_inter_theme +
    [int]$Counts.residual_return_within_theme +
    [int]$Counts.residual_return_inter_theme
if ($PerDateContracts -ne 106) {
    throw "Unexpected Core4 IGC contract count per date: $PerDateContracts; expected 106."
}

Push-Location $RepoRoot
try {
    git fetch origin $ExpectedBranch
    Assert-LastExitCode "git fetch"
    git switch $ExpectedBranch
    Assert-LastExitCode "git switch"
    git pull --ff-only origin $ExpectedBranch
    Assert-LastExitCode "git pull --ff-only"

    $Branch = (git branch --show-current).Trim()
    if ($Branch -ne $ExpectedBranch) {
        throw "Expected GAL branch $ExpectedBranch, got '$Branch'."
    }
    if (-not $SkipCleanCheck -and (git status --porcelain)) {
        throw "GAL working tree must be clean. Use -SkipCleanCheck only for an explicit diagnostic."
    }
    $Head = (git rev-parse HEAD).Trim()
    Assert-LastExitCode "git rev-parse HEAD"
    if ($ExpectedCommit -eq "auto") {
        $ExpectedCommit = $Head
    } elseif ($ExpectedCommit -ne $Head) {
        throw "GAL HEAD mismatch: expected $ExpectedCommit, got $Head."
    }

    if (-not $SkipInstall) {
        python -m pip install -e ".[test]"
        Assert-LastExitCode "GAL editable install"
    }

    python -m compileall -q src scripts
    Assert-LastExitCode "GAL compileall"

    if (-not $SkipTests) {
        python -m pytest -q --tb=short
        Assert-LastExitCode "GAL tests"
    }

    New-Item -ItemType Directory -Force -Path $TempDirectory | Out-Null
    New-Item -ItemType Directory -Force -Path (Split-Path $SignalsOutput -Parent) | Out-Null
    New-Item -ItemType Directory -Force -Path (Split-Path $ReportOutput -Parent) | Out-Null

    $ArgsList = @(
        "scripts/run_dual_theme_alpha.py", "full",
        "--gff-campaign-root", $GffCampaignRoot,
        "--signals-output", $SignalsOutput,
        "--horizon-manifest", $HorizonManifest,
        "--output", $ReportOutput,
        "--theme-families", "momentum_state,residual_return",
        "--scopes", "global,within_theme,inter_theme",
        "--variants", "node_baseline,graph_forward,graph_reverse_placebo",
        "--memory-limit-gb", "$MemoryLimitGb",
        "--threads", "$Threads",
        "--temp-directory", $TempDirectory,
        "--min-cross-section", "$MinCrossSection",
        "--min-theme-size", "$MinThemeSize",
        "--min-theme-cross-section", "$MinThemeCrossSection",
        "--default-direction", "auto",
        "--control-columns", "own_score",
        "--expected-git-commit", $ExpectedCommit
    )
    if (-not $SkipCleanCheck) { $ArgsList += "--require-clean" }
    if ($Metadata) {
        $ArgsList += @(
            "--metadata", $Metadata,
            "--slice-dimensions", "sector_code,industry_code,country,market_cap_bucket"
        )
    }
    if ($ForceExport) { $ArgsList += "--force-export" }
    if ($AllowPartial) { $ArgsList += "--allow-partial" }

    python @ArgsList
    Assert-LastExitCode "Core4 GAL Alpha workflow"

    $RequiredOutputs = @(
        (Join-Path $SignalsOutput "_SUCCESS"),
        (Join-Path $SignalsOutput "export_manifest.json"),
        (Join-Path $ReportOutput "_SUCCESS"),
        (Join-Path $ReportOutput "summary.json"),
        (Join-Path $ReportOutput "direct_return_alpha_metrics.csv"),
        (Join-Path $ReportOutput "regime_candidate_metrics.csv"),
        (Join-Path $ReportOutput "within_theme_alpha_metrics.csv"),
        (Join-Path $ReportOutput "inter_theme_alpha_metrics.csv"),
        (Join-Path $ReportOutput "cross_scope_comparison.csv"),
        (Join-Path $ReportOutput "financial_semantics_summary.csv"),
        (Join-Path $ReportOutput "matched_variant_comparison.csv"),
        (Join-Path $ReportOutput "ranking.csv")
    )
    foreach ($Path in $RequiredOutputs) { Require-Path $Path "governed GAL output" }

    $ExportManifest = Get-Content (Join-Path $SignalsOutput "export_manifest.json") -Raw | ConvertFrom-Json
    if ([int]$ExportManifest.factor_count -ne $ExpectedFactorCount) {
        throw "Exported factor_count=$($ExportManifest.factor_count); expected $ExpectedFactorCount. Inspect empty outputs and contract coverage before using --allow-partial."
    }
    if ([int]$ExportManifest.edge_pit_violations -ne 0) {
        throw "Export contains edge PIT violations: $($ExportManifest.edge_pit_violations)"
    }

    $Summary = Get-Content (Join-Path $ReportOutput "summary.json") -Raw | ConvertFrom-Json
    if ($Summary.complete -ne $true) {
        throw "GAL report is not complete."
    }
    if ([int]$Summary.expected_factor_count_per_horizon -ne $ExpectedFactorCount) {
        throw "Report expected factor count mismatch: $($Summary.expected_factor_count_per_horizon)"
    }

    Write-Host ""
    Write-Host "=== CORE4 GAL ALPHA PASS ==="
    Write-Host "GAL commit: $ExpectedCommit"
    Write-Host "GFF version: $GffVersion"
    Write-Host "Dates: $($Dates.Count) ($($SortedDates[0])..$($SortedDates[-1]))"
    Write-Host "Factors per horizon: $($ExportManifest.factor_count)"
    Write-Host "Direct return Alpha: $(Join-Path $ReportOutput 'direct_return_alpha_metrics.csv')"
    Write-Host "Regime candidates: $(Join-Path $ReportOutput 'regime_candidate_metrics.csv')"
    Write-Host "Full report: $ReportOutput"
}
finally {
    Pop-Location
}
