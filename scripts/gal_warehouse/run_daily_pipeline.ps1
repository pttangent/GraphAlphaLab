param(
  [int]$MaxParallel = 3,
  [int]$MaxAttempts = 5
)

$ErrorActionPreference = 'Stop'

$wt = 'C:\Users\A001\.config\superpowers\worktrees\GraphAlphaLab\gal-report-9e67'
$scripts = Join-Path $wt 'scripts\gal_warehouse'
$root = 'D:\DEV\AnotherNetworkFactory\warehouses\GAL_warehouse'
$logs = Join-Path $root 'logs\daily_pipeline'
$pipelineLog = Join-Path $logs 'pipeline.log'
New-Item -ItemType Directory -Force $logs | Out-Null

function Write-PipelineLog($message) {
  Add-Content -LiteralPath $pipelineLog -Value ("{0} {1}" -f (Get-Date -Format s), $message)
}

$labelScript = Join-Path $scripts 'run_daily_label_build.ps1'
$alphaScript = Join-Path $scripts 'run_daily_alpha_scheduler.ps1'
if (-not (Test-Path -LiteralPath $labelScript)) { throw "Missing version-controlled label wrapper: $labelScript" }
if (-not (Test-Path -LiteralPath $alphaScript)) { throw "Missing version-controlled daily alpha scheduler: $alphaScript" }

Write-PipelineLog 'daily pipeline started'
Write-PipelineLog 'building or resuming daily labels'
& powershell.exe -NoProfile -ExecutionPolicy Bypass -File $labelScript
if ($LASTEXITCODE -ne 0) { throw "Daily label build failed with exit code $LASTEXITCODE" }
Write-PipelineLog 'daily labels built'

$verification = Join-Path $root 'label_diagnostics\daily_forward_rth_verified.json'
$success = Join-Path $root 'label_diagnostics\daily_forward_SUCCESS.json'
if (-not (Test-Path -LiteralPath $verification)) { throw "Daily label verification was not created: $verification" }
if (-not (Test-Path -LiteralPath $success)) { throw "Daily label final success marker was not created: $success" }
$payload = Get-Content -LiteralPath $verification -Raw | ConvertFrom-Json
if ([int64]$payload.invalid_decision_rows -ne 0) {
  throw "Daily label verification failed: invalid_decision_rows=$($payload.invalid_decision_rows)"
}
Write-PipelineLog ("daily labels verified; label_count={0}; trade_date_count={1}; row_count={2}" -f $payload.label_count, $payload.trade_date_count, $payload.row_count)

Write-PipelineLog 'starting resumable daily alpha scheduler'
& powershell.exe -NoProfile -ExecutionPolicy Bypass -File $alphaScript -MaxParallel $MaxParallel -MaxAttempts $MaxAttempts
if ($LASTEXITCODE -ne 0) { throw "Daily alpha scheduler failed with exit code $LASTEXITCODE" }
Write-PipelineLog 'daily pipeline finished'
