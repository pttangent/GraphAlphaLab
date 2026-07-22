param(
  [int]$MaxParallel = 3,
  [int]$MaxAttempts = 5,
  [int]$PollSeconds = 60
)

$ErrorActionPreference = 'Stop'

$wt = 'C:\Users\A001\.config\superpowers\worktrees\GraphAlphaLab\gal-report-9e67'
$python = Join-Path $wt '.venv\Scripts\python.exe'
$resumable = Join-Path $wt 'scripts\gal_warehouse\run_resumable_alpha_report.py'
$head = (& git -C $wt rev-parse HEAD).Trim()
$root = 'D:\DEV\AnotherNetworkFactory\warehouses\GAL_warehouse'
$metadata = Join-Path $root 'metadata\symbol_metadata.parquet'
$labels = Join-Path $root 'labels\daily_forward.parquet'
$contracts = Join-Path $root 'contracts'
$rthVerification = Join-Path $root 'label_diagnostics\daily_forward_rth_verified.json'
$logs = Join-Path $root 'logs\daily_alpha'
$schedulerLog = Join-Path $logs 'scheduler.log'
$attemptsPath = Join-Path $logs 'attempts.json'
$dashboardJson = Join-Path $logs 'DASHBOARD.json'
$dashboardMd = Join-Path $logs 'DASHBOARD.md'
$maxParallel = [Math]::Max(1, $MaxParallel)
$maxAttempts = [Math]::Max(1, $MaxAttempts)
$pollSeconds = [Math]::Max(10, $PollSeconds)
New-Item -ItemType Directory -Force $logs | Out-Null

function Write-SchedulerLog($message) { Add-Content -LiteralPath $schedulerLog -Value ("{0} {1}" -f (Get-Date -Format s), $message) }

if (-not (Test-Path $labels)) { throw "Daily label file is missing: $labels. Run build_daily_labels_only.py first." }
if (-not (Test-Path $rthVerification)) { throw "Daily label RTH verification is missing: $rthVerification." }
$rthPayload = Get-Content -LiteralPath $rthVerification -Raw | ConvertFrom-Json
if ([int64]$rthPayload.invalid_decision_rows -ne 0) { throw "Daily label RTH verification failed: invalid_decision_rows=$($rthPayload.invalid_decision_rows)." }

$labelIds = Get-ChildItem -LiteralPath $contracts -Filter 'daily_*.json' | ForEach-Object { $_.BaseName } | Sort-Object
if ($labelIds.Count -eq 0) { throw "No daily label contracts found under $contracts." }

$jobs = @()
foreach ($batch in @('implemented27', 'remaining14')) {
  foreach ($labelId in $labelIds) {
    $jobs += @{
      Name = "$($batch)_$($labelId)"
      Batch = $batch
      Signals = Join-Path $root "signals\$batch"
      Contract = Join-Path $contracts "$labelId.json"
      Out = Join-Path $root "reports\daily_alpha\$($batch)_$($labelId)"
      Temp = Join-Path $root "duckdb_tmp\daily_alpha\$($batch)_$($labelId)"
    }
  }
}

function Load-Attempts {
  $result = @{}
  if (Test-Path -LiteralPath $attemptsPath) {
    try {
      $payload = Get-Content -LiteralPath $attemptsPath -Raw | ConvertFrom-Json
      foreach ($property in $payload.PSObject.Properties) { $result[$property.Name] = [int]$property.Value }
    } catch { Write-SchedulerLog "WARN could not parse attempts.json: $($_.Exception.Message)" }
  }
  return $result
}

function Save-Attempts($attempts) {
  $ordered = [ordered]@{}
  foreach ($key in ($attempts.Keys | Sort-Object)) { $ordered[$key] = [int]$attempts[$key] }
  $temp = "$attemptsPath.part"
  $ordered | ConvertTo-Json | Set-Content -LiteralPath $temp -Encoding UTF8
  Move-Item -LiteralPath $temp -Destination $attemptsPath -Force
}

function Get-ActiveDailyAlphaNames {
  $active = Get-CimInstance Win32_Process | Where-Object {
    $_.Name -in @('python.exe', 'gal.exe') -and $_.CommandLine -like '*run_resumable_alpha_report.py*' -and $_.CommandLine -like '*reports\daily_alpha*'
  }
  $names = @()
  foreach ($p in $active) {
    if ($p.CommandLine -match 'reports\\daily_alpha\\([^\s"]+)') { $names += $Matches[1] }
  }
  $names | Sort-Object -Unique
}

function Test-JobReady($jobSpec) {
  return (Test-Path -LiteralPath $jobSpec.Signals) -and (Test-Path -LiteralPath $jobSpec.Contract) -and (Test-Path -LiteralPath $labels) -and (Test-Path -LiteralPath $metadata)
}

function Start-DailyAlphaJob($jobSpec, $attempt) {
  New-Item -ItemType Directory -Force $jobSpec.Temp | Out-Null
  $args = @(
    $resumable,
    '--batch-id', $jobSpec.Batch,
    '--signals', $jobSpec.Signals,
    '--labels', $labels,
    '--label-contract', $jobSpec.Contract,
    '--metadata', $metadata,
    '--slice-dimensions', 'sector_code,industry_code,country,market_cap_bucket',
    '--join-keys', 'trade_date,decision_time,symbol_id',
    '--control-columns', 'own_score',
    '--default-direction', 'auto',
    '--memory-limit-gb', '24',
    '--threads', '8',
    '--temp-directory', $jobSpec.Temp,
    '--min-cross-section', '100',
    '--correlation-sample-modulus', '0',
    '--output', $jobSpec.Out,
    '--expected-git-commit', $head,
    '--require-clean'
  )
  Start-Process -FilePath $python -ArgumentList $args -WorkingDirectory $wt -RedirectStandardOutput "$logs\$($jobSpec.Name).stdout.log" -RedirectStandardError "$logs\$($jobSpec.Name).stderr.log" -WindowStyle Hidden | Out-Null
  Write-SchedulerLog "STARTED $($jobSpec.Name) attempt=$attempt"
}

function Write-Dashboard($activeNames, $attempts) {
  $rows = @()
  foreach ($jobSpec in $jobs) {
    $success = Test-Path (Join-Path $jobSpec.Out '_SUCCESS')
    $active = $activeNames -contains $jobSpec.Name
    $progressPath = Join-Path $jobSpec.Out 'progress.json'
    $progress = $null
    if (Test-Path $progressPath) { try { $progress = Get-Content -LiteralPath $progressPath -Raw | ConvertFrom-Json } catch { $progress = $null } }
    $attempt = [int]$attempts[$jobSpec.Name]
    $status = if ($success) { 'complete' } elseif ($active) { 'running' } elseif ($attempt -ge $maxAttempts) { 'exhausted' } elseif (Test-JobReady $jobSpec) { 'retryable' } else { 'waiting_input' }
    $rows += [ordered]@{
      name = $jobSpec.Name; batch = $jobSpec.Batch; status = $status; attempts = $attempt
      completed_units = if ($progress) { [int]$progress.completed_units } else { 0 }
      total_units = if ($progress) { [int]$progress.total_units } else { 0 }
      progress_pct = if ($progress) { [double]$progress.progress_pct } else { 0.0 }
      current_unit = if ($progress) { [string]$progress.current_unit } else { '' }
      output = $jobSpec.Out
    }
  }
  $payload = [ordered]@{ updated_at = (Get-Date -Format o); max_parallel = $maxParallel; max_attempts = $maxAttempts; jobs = $rows }
  $tmpJson = "$dashboardJson.part"
  $payload | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $tmpJson -Encoding UTF8
  Move-Item -LiteralPath $tmpJson -Destination $dashboardJson -Force
  $lines = @('# GAL Daily Alpha Scheduler', '', "Updated: $($payload.updated_at)", '', '| Job | Status | Attempts | Progress | Current |', '|---|---:|---:|---:|---|')
  foreach ($row in $rows) { $lines += "| $($row.name) | $($row.status) | $($row.attempts) | $($row.completed_units)/$($row.total_units) ($($row.progress_pct)%) | $($row.current_unit) |" }
  $tmpMd = "$dashboardMd.part"
  $lines | Set-Content -LiteralPath $tmpMd -Encoding UTF8
  Move-Item -LiteralPath $tmpMd -Destination $dashboardMd -Force
}

$attempts = Load-Attempts
Write-SchedulerLog ("scheduler started; commit={0}; job_count={1}; max_parallel={2}; max_attempts={3}" -f $head, $jobs.Count, $maxParallel, $maxAttempts)

while ($true) {
  $activeNames = @(Get-ActiveDailyAlphaNames)
  Write-Dashboard $activeNames $attempts
  Write-SchedulerLog ("ACTIVE {0} [{1}]" -f $activeNames.Count, ($activeNames -join ','))

  while ($activeNames.Count -lt $maxParallel) {
    $next = $null
    foreach ($jobSpec in $jobs) {
      $success = Test-Path (Join-Path $jobSpec.Out '_SUCCESS')
      $attempt = [int]$attempts[$jobSpec.Name]
      if ((Test-JobReady $jobSpec) -and (-not $success) -and ($activeNames -notcontains $jobSpec.Name) -and $attempt -lt $maxAttempts) { $next = $jobSpec; break }
    }
    if ($null -eq $next) { break }
    $attempts[$next.Name] = ([int]$attempts[$next.Name]) + 1
    Save-Attempts $attempts
    Start-DailyAlphaJob $next $attempts[$next.Name]
    Start-Sleep -Seconds 5
    $activeNames = @(Get-ActiveDailyAlphaNames)
  }

  $allDone = $true
  $exhausted = @()
  foreach ($jobSpec in $jobs) {
    if (-not (Test-Path (Join-Path $jobSpec.Out '_SUCCESS'))) {
      $allDone = $false
      if ([int]$attempts[$jobSpec.Name] -ge $maxAttempts -and ($activeNames -notcontains $jobSpec.Name)) { $exhausted += $jobSpec.Name }
    }
  }
  Write-Dashboard $activeNames $attempts
  if ($allDone -and $activeNames.Count -eq 0) { Write-SchedulerLog 'scheduler finished'; break }
  if ($exhausted.Count -gt 0 -and $activeNames.Count -eq 0) { throw "Daily alpha scheduler exhausted retries: $($exhausted -join ', ')" }
  Start-Sleep -Seconds $pollSeconds
}
