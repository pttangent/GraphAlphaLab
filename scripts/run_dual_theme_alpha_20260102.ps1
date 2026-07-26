[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)] [string]$GffCampaignRoot,
    [Parameter(Mandatory = $true)] [string]$HorizonManifest,
    [Parameter(Mandatory = $true)] [string]$SignalsOutput,
    [Parameter(Mandatory = $true)] [string]$ReportOutput,
    [string]$Metadata,
    [string]$ExpectedCommit = "auto",
    [int]$MemoryLimitGb = 24,
    [int]$Threads = 8,
    [string]$TempDirectory = "D:\GAL\duckdb_tmp",
    [int]$MinCrossSection = 100,
    [int]$MinThemeSize = 5,
    [int]$MinThemeCrossSection = 5,
    [int]$CorrelationSampleModulus = 1000,
    [switch]$AllowPartial,
    [switch]$ForceExport,
    [switch]$SkipCleanCheck
)
$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Push-Location $repoRoot
try {
    $expectedBranch = "agent/dual-theme-factor-checkpoint"
    $branch = (git branch --show-current).Trim()
    if ($branch -ne $expectedBranch) {
        throw "Expected branch $expectedBranch, got '$branch'."
    }
    $head = (git rev-parse HEAD).Trim()
    if ($ExpectedCommit -ne "auto" -and $ExpectedCommit -ne $head) {
        throw "Commit mismatch: expected $ExpectedCommit, got $head."
    }
    if (-not $SkipCleanCheck -and (git status --porcelain)) {
        throw "Working tree must be clean. Use -SkipCleanCheck only for an explicit diagnostic."
    }
    foreach ($path in @($GffCampaignRoot, $HorizonManifest)) {
        if (-not (Test-Path $path)) { throw "Missing required path: $path" }
    }
    if (-not (Test-Path (Join-Path $GffCampaignRoot "_SUCCESS"))) {
        throw "GFF campaign is incomplete: missing _SUCCESS."
    }
    if (-not (Test-Path (Join-Path $GffCampaignRoot "runs\campaign_contract.json"))) {
        throw "GFF campaign is missing runs\campaign_contract.json."
    }
    if ($Metadata -and -not (Test-Path $Metadata)) { throw "Missing metadata: $Metadata" }

    python -m py_compile `
        src/graphalphalab/checkpoint.py `
        src/graphalphalab/dual_theme_common.py `
        src/graphalphalab/dual_theme_sql.py `
        src/graphalphalab/dual_theme_export.py `
        src/graphalphalab/dual_theme_scope_alpha.py `
        src/graphalphalab/dual_theme_reporting.py `
        src/graphalphalab/dual_theme_resumable.py `
        src/graphalphalab/dual_theme.py `
        src/graphalphalab/dual_theme_cli.py `
        scripts/run_dual_theme_alpha.py
    if ($LASTEXITCODE -ne 0) { throw "Dual-theme GAL compile check failed." }

    $argsList = @(
        "scripts/run_dual_theme_alpha.py", "full",
        "--gff-campaign-root", $GffCampaignRoot,
        "--signals-output", $SignalsOutput,
        "--horizon-manifest", $HorizonManifest,
        "--output", $ReportOutput,
        "--theme-families", "momentum_state,residual_return",
        "--scopes", "global,within_theme,inter_theme",
        "--variants", "node_baseline,graph_forward,graph_reverse_placebo",
        "--memory-limit-gb", $MemoryLimitGb,
        "--threads", $Threads,
        "--temp-directory", $TempDirectory,
        "--min-cross-section", $MinCrossSection,
        "--min-theme-size", $MinThemeSize,
        "--min-theme-cross-section", $MinThemeCrossSection,
        "--correlation-sample-modulus", $CorrelationSampleModulus,
        "--expected-git-commit", $head
    )
    if (-not $SkipCleanCheck) { $argsList += "--require-clean" }
    if ($Metadata) {
        $argsList += @(
            "--metadata", $Metadata,
            "--slice-dimensions", "sector_code,industry_code,country,market_cap_bucket"
        )
    }
    if ($AllowPartial) { $argsList += "--allow-partial" }
    if ($ForceExport) { $argsList += "--force-export" }

    python @argsList
    if ($LASTEXITCODE -ne 0) { throw "Dual-theme GAL workflow failed with exit code $LASTEXITCODE." }

    foreach ($marker in @(
        (Join-Path $SignalsOutput "_SUCCESS"),
        (Join-Path $SignalsOutput "export_manifest.json"),
        (Join-Path $ReportOutput "_SUCCESS"),
        (Join-Path $ReportOutput "global_alpha_metrics.csv"),
        (Join-Path $ReportOutput "within_theme_alpha_metrics.csv"),
        (Join-Path $ReportOutput "inter_theme_alpha_metrics.csv"),
        (Join-Path $ReportOutput "cross_scope_comparison.csv"),
        (Join-Path $ReportOutput "scope_family_horizon_summary.csv"),
        (Join-Path $ReportOutput "matched_variant_comparison.csv"),
        (Join-Path $ReportOutput "ranking.csv")
    )) {
        if (-not (Test-Path $marker)) { throw "Missing governed output: $marker" }
    }
    Write-Host "Dual-theme scope-correct factor-resumable Alpha workflow complete."
    Write-Host "Signals: $SignalsOutput"
    Write-Host "Reports: $ReportOutput"
    Write-Host "Checkpoints: $(Join-Path $ReportOutput '_checkpoints\dual_theme_alpha')"
}
finally {
    Pop-Location
}
