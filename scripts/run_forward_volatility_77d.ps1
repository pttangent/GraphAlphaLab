[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$SignalsRoot,

    [Parameter(Mandatory = $true)]
    [string]$GffCoreTemplate,

    [Parameter(Mandatory = $true)]
    [string]$OutputRoot,

    [string]$StartDate = "2026-01-05",
    [string]$EndDate = "2026-07-22",
    [string]$Horizons = "30,60,120,180",
    [string]$Targets = "forward_log_rv,forward_vol_expansion,forward_downside_semivar,forward_downside_share,forward_jump_var,forward_jump_share,forward_max_abs_return,forward_max_drawdown,forward_tail_event",
    [int]$LabelWorkers = 4,
    [int]$PredictionWorkers = 6,
    [int]$MinWorkers = 2,
    [double]$MemoryLimitGb = 96,
    [int]$Threads = 18,
    [string]$TempDirectory = "D:\GAL\duckdb_tmp\forward_volatility_77d",
    [switch]$AllowDirty
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Push-Location $RepoRoot
try {
    $Branch = (git branch --show-current).Trim()
    if ($Branch -ne "agent/forward-volatility-labels") {
        throw "Run this entrypoint from branch agent/forward-volatility-labels; current branch is '$Branch'."
    }

    $Head = (git rev-parse HEAD).Trim()
    if ([string]::IsNullOrWhiteSpace($Head)) {
        throw "Cannot resolve Git HEAD."
    }

    $SignalSuccess = Join-Path $SignalsRoot "_SUCCESS"
    $ExportManifest = Join-Path $SignalsRoot "export_manifest.json"
    if (-not (Test-Path $SignalSuccess)) {
        throw "The GAL signal export is not complete: $SignalSuccess"
    }
    if (-not (Test-Path $ExportManifest)) {
        throw "Missing governed signal export manifest: $ExportManifest"
    }

    New-Item -ItemType Directory -Force -Path $OutputRoot | Out-Null
    New-Item -ItemType Directory -Force -Path $TempDirectory | Out-Null

    $env:GAL_EXPECTED_GIT_COMMIT = $Head
    $PythonArgs = @(
        "scripts/run_forward_volatility_patch.py",
        "--signals-root", $SignalsRoot,
        "--gff-core-template", $GffCoreTemplate,
        "--output-root", $OutputRoot,
        "--start-date", $StartDate,
        "--end-date", $EndDate,
        "--horizons", $Horizons,
        "--targets", $Targets,
        "--label-workers", $LabelWorkers,
        "--prediction-workers", $PredictionWorkers,
        "--min-workers", $MinWorkers,
        "--memory-limit-gb", $MemoryLimitGb,
        "--threads", $Threads,
        "--temp-directory", $TempDirectory,
        "--expected-git-commit", $Head
    )
    if (-not $AllowDirty) {
        $PythonArgs += "--require-clean"
    }

    Write-Host "GraphAlphaLab forward-volatility patch"
    Write-Host "  Branch:              $Branch"
    Write-Host "  Commit:              $Head"
    Write-Host "  Signals:             $SignalsRoot"
    Write-Host "  NFF gff_core:        $GffCoreTemplate"
    Write-Host "  Output:              $OutputRoot"
    Write-Host "  Dates:               $StartDate .. $EndDate"
    Write-Host "  Horizons:            $Horizons"
    Write-Host "  Label workers:       $LabelWorkers"
    Write-Host "  Prediction workers:  $PredictionWorkers"
    Write-Host "  Minimum workers:     $MinWorkers"
    Write-Host ""
    Write-Host "Valid checkpoints are reused. Broken worker pools step down and requeue uncommitted units."

    & python @PythonArgs
    if ($LASTEXITCODE -ne 0) {
        throw "Forward-volatility patch failed with exit code $LASTEXITCODE. Re-run the same command to resume."
    }

    $Success = Join-Path $OutputRoot "_SUCCESS"
    if (-not (Test-Path $Success)) {
        throw "The process exited without publishing final _SUCCESS: $Success"
    }
    Write-Host "Completed. Report: $(Join-Path $OutputRoot 'prediction_report\REPORT.md')"
}
finally {
    Pop-Location
}
