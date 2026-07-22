$ErrorActionPreference = 'Stop'

$root = 'D:\DEV\AnotherNetworkFactory\warehouses\GAL_warehouse'
$logs = Join-Path $root 'logs\daily_pipeline'
$pipelineLog = Join-Path $logs 'pipeline.log'
New-Item -ItemType Directory -Force $logs | Out-Null

function Write-PipelineLog($message) {
  Add-Content -LiteralPath $pipelineLog -Value ("{0} {1}" -f (Get-Date -Format s), $message)
}

Write-PipelineLog 'daily pipeline started'

Write-PipelineLog 'building daily labels'
& powershell.exe -NoProfile -ExecutionPolicy Bypass -File (Join-Path $root 'run_daily_label_build.ps1')
Write-PipelineLog 'daily labels built'

$verification = Join-Path $root 'label_diagnostics\daily_forward_rth_verified.json'
if (-not (Test-Path $verification)) {
  throw "Daily label verification was not created: $verification"
}
$payload = Get-Content -LiteralPath $verification -Raw | ConvertFrom-Json
if ([int64]$payload.invalid_decision_rows -ne 0) {
  throw "Daily label verification failed: invalid_decision_rows=$($payload.invalid_decision_rows)"
}
Write-PipelineLog ("daily labels verified; label_count={0}; trade_date_count={1}; row_count={2}" -f $payload.label_count, $payload.trade_date_count, $payload.row_count)

Write-PipelineLog 'starting daily alpha scheduler'
& powershell.exe -NoProfile -ExecutionPolicy Bypass -File (Join-Path $root 'run_daily_alpha_scheduler.ps1')
Write-PipelineLog 'daily pipeline finished'
