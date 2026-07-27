[CmdletBinding()]
param(
    [string]$RepoRoot = "D:\DEV\AnotherNetworkFactory\GraphAlphaLab",
    [string]$GffCampaignRoot = "D:\G4H\campaign=c4_260105_260722_induced_v2",

    [Parameter(Mandatory = $true)]
    [string]$HorizonManifest,

    [string]$SignalsOutput = "D:\DEV\AnotherNetworkFactory\warehouses\GAL_warehouse\signals\c4_260105_260722_induced_v2",
    [string]$ReportOutput = "D:\DEV\AnotherNetworkFactory\warehouses\GAL_warehouse\reports\c4_260105_260722_induced_v2",
    [string]$FrequencyOutput = "",
    [string]$Metadata,
    [string]$ExpectedCommit = "auto",
    [int]$MemoryLimitGb = 64,
    [int]$Threads = 12,
    [int]$FactorWorkers = 6,
    [int]$FrequencyWorkers = 6,
    [int]$CandidatesPerHorizon = 12,
    [int]$MinCandidateDates = 20,
    [string]$TempDirectory = "D:\GAL\duckdb_tmp\c4_260105_260722_induced_v2",
    [int]$MinCrossSection = 100,
    [int]$MinThemeSize = 5,
    [int]$MinThemeCrossSection = 5,
    [switch]$SkipFrequency,
    [switch]$SkipInstall,
    [switch]$SkipTests,
    [switch]$SkipCleanCheck
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$ExpectedBranch = "agent/induced-global-frequency-gates"
$ExpectedGffVersion = "SMI_DUAL_THEME_IGC_FULL_SCOPE_COMPARE_V2_INDUCED_WITHIN"
$ExpectedCampaignId = "c4_260105_260722_induced_v2"
$ExpectedDateCount = 137
$ExpectedFactorCount = 318

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

Require-Path $RepoRoot "GAL repository"
Require-Path $GffCampaignRoot "GFF Core4 campaign"
Require-Path (Join-Path $GffCampaignRoot "_SUCCESS") "GFF campaign _SUCCESS"
Require-Path (Join-Path $GffCampaignRoot "runs\campaign_contract.json") "GFF campaign contract"
Require-Path (Join-Path $GffCampaignRoot "gal_interface") "GFF GAL interface"
Require-Path $HorizonManifest "horizon manifest"
if ($Metadata) { Require-Path $Metadata "metadata" }
if ($FactorWorkers -lt 1) { throw "FactorWorkers must be >= 1" }
if ($FrequencyWorkers -lt 1) { throw "FrequencyWorkers must be >= 1" }
if ($CandidatesPerHorizon -lt 1) { throw "CandidatesPerHorizon must be >= 1" }
if ($MinCandidateDates -lt 1) { throw "MinCandidateDates must be >= 1" }
if (-not $FrequencyOutput) {
    $FrequencyOutput = Join-Path $ReportOutput "execution_frequency"
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
        throw "GAL working tree must be clean."
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
    $AuditOutput = Join-Path $ReportOutput "input_compatibility_audit.json"
    New-Item -ItemType Directory -Force -Path $ReportOutput | Out-Null

    python scripts/audit_core4_gff_compatibility.py `
      --gff-campaign-root $GffCampaignRoot `
      --output $AuditOutput
    Assert-LastExitCode "Core4 GFF compatibility audit"

    $Audit = Get-Content $AuditOutput -Raw | ConvertFrom-Json
    if ($Audit.gal_report_compatible -ne $true) {
        throw "GFF campaign did not pass the GAL compatibility audit."
    }
    if ($Audit.campaign_version -ne $ExpectedGffVersion) {
        throw "Unexpected GFF version: $($Audit.campaign_version)"
    }
    if ([int]$Audit.date_count -ne $ExpectedDateCount) {
        throw "Unexpected GFF date count: $($Audit.date_count)"
    }
    if ([int]$Audit.factor_count_per_horizon -ne $ExpectedFactorCount) {
        throw "Unexpected factor count per horizon: $($Audit.factor_count_per_horizon)"
    }
    if ($Audit.scope_semantics.within_theme.graph_estimation_scope -ne "global_market") {
        throw "Within-Theme was not resolved as a Global graph estimate."
    }
    if ($Audit.scope_semantics.within_theme.is_local_graph_estimate -ne $false) {
        throw "Within-Theme was incorrectly marked as a local graph estimate."
    }

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
        "--factor-workers", "$FactorWorkers",
        "--temp-directory", $TempDirectory,
        "--min-cross-section", "$MinCrossSection",
        "--min-theme-size", "$MinThemeSize",
        "--min-theme-cross-section", "$MinThemeCrossSection",
        "--default-direction", "auto",
        "--control-columns", "own_score",
        "--correlation-sample-modulus", "0",
        "--expected-git-commit", $ExpectedCommit
    )
    if (-not $SkipCleanCheck) { $ArgsList += "--require-clean" }
    if ($Metadata) {
        $ArgsList += @(
            "--metadata", $Metadata,
            "--slice-dimensions", "sector_code,industry_code,country,market_cap_bucket"
        )
    }

    python @ArgsList
    Assert-LastExitCode "Core4 induced-global GAL Alpha"

    python scripts/annotate_induced_global_report.py `
      --gff-campaign-root $GffCampaignRoot `
      --report-root $ReportOutput
    Assert-LastExitCode "induced-global semantic report annotation"

    if (-not $SkipFrequency) {
        New-Item -ItemType Directory -Force -Path $FrequencyOutput | Out-Null
        $CandidateManifest = Join-Path $FrequencyOutput "execution_candidates.json"
        $CandidateTable = Join-Path $FrequencyOutput "execution_candidates.csv"
        $FrequencyTemp = Join-Path $TempDirectory "execution_frequency"
        New-Item -ItemType Directory -Force -Path $FrequencyTemp | Out-Null

        python scripts/select_dual_theme_frequency_candidates.py `
          --metrics (Join-Path $ReportOutput "direct_return_alpha_metrics.csv") `
          --output $CandidateManifest `
          --table-output $CandidateTable `
          --min-date-count $MinCandidateDates `
          --max-per-horizon $CandidatesPerHorizon `
          --min-per-horizon 2 `
          --scopes "global,within_theme,inter_theme"
        Assert-LastExitCode "execution candidate selection"

        python scripts/run_dual_theme_frequency_dag.py `
          --signals-root $SignalsOutput `
          --horizon-manifest $HorizonManifest `
          --candidate-manifest $CandidateManifest `
          --output $FrequencyOutput `
          --workers $FrequencyWorkers `
          --memory-limit-gb $MemoryLimitGb `
          --threads $Threads `
          --temp-directory $FrequencyTemp `
          --min-theme-size $MinThemeSize `
          --min-theme-cross-section $MinThemeCrossSection `
          --min-direction-train-dates 20 `
          --direction-rolling-dates 60 `
          --control-columns "own_score" `
          --quantiles 5
        Assert-LastExitCode "execution frequency global DAG"
    }

    foreach ($Path in @(
        (Join-Path $SignalsOutput "_SUCCESS"),
        (Join-Path $SignalsOutput "export_manifest.json"),
        (Join-Path $ReportOutput "_SUCCESS"),
        (Join-Path $ReportOutput "summary.json"),
        (Join-Path $ReportOutput "direct_return_alpha_metrics.csv"),
        (Join-Path $ReportOutput "regime_candidate_metrics.csv"),
        (Join-Path $ReportOutput "within_theme_alpha_metrics.csv"),
        (Join-Path $ReportOutput "inter_theme_alpha_metrics.csv"),
        (Join-Path $ReportOutput "execution_semantics\scope_semantics.json"),
        (Join-Path $ReportOutput "execution_semantics\REPORT.md"),
        (Join-Path $ReportOutput "_checkpoints\dual_theme_alpha\global_dag_progress.json")
    )) {
        Require-Path $Path "governed GAL output"
    }

    if (-not $SkipFrequency) {
        foreach ($Path in @(
            (Join-Path $FrequencyOutput "_SUCCESS"),
            (Join-Path $FrequencyOutput "summary.json"),
            (Join-Path $FrequencyOutput "execution_candidates.json"),
            (Join-Path $FrequencyOutput "frequency_policy_metrics.csv"),
            (Join-Path $FrequencyOutput "frequency_policy_returns.csv"),
            (Join-Path $FrequencyOutput "frequency_pareto_frontier.csv"),
            (Join-Path $FrequencyOutput "gate_effectiveness.csv"),
            (Join-Path $FrequencyOutput "frequency_horizon_matrix.csv"),
            (Join-Path $FrequencyOutput "candidate_execution_recommendations.csv"),
            (Join-Path $FrequencyOutput "_checkpoints\execution_frequency\global_dag_progress.json")
        )) {
            Require-Path $Path "governed execution-frequency output"
        }
    }

    Write-Host ""
    Write-Host "=== CORE4 INDUCED-GLOBAL GAL PASS ==="
    Write-Host "GAL branch: $ExpectedBranch"
    Write-Host "GAL commit: $ExpectedCommit"
    Write-Host "GFF campaign: $GffCampaignRoot"
    Write-Host "GFF version: $ExpectedGffVersion"
    Write-Host "Campaign ID: $ExpectedCampaignId"
    Write-Host "Graph semantics: Global estimate; Within is induced same-theme edge selection."
    Write-Host "Ranking semantics: Within uses local rank in decision_time x context_theme_id."
    Write-Host "Alpha factor workers: $FactorWorkers"
    Write-Host "Reports: $ReportOutput"
    if (-not $SkipFrequency) {
        Write-Host "Execution frequency workers: $FrequencyWorkers"
        Write-Host "Candidates per horizon: up to $CandidatesPerHorizon"
        Write-Host "Execution frequency reports: $FrequencyOutput"
    }
}
finally {
    Pop-Location
}
