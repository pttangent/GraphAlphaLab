param(
  [int]$MaxParallel = 3,
  [int]$MaxAttempts = 5,
  [int]$PollSeconds = 60
)

$ErrorActionPreference = 'Stop'

$wt = 'C:\Users\A001\.config\superpowers\worktrees\GraphAlphaLab\gal-report-9e67'
$python = Join-Path $wt '.venv\Scripts\python.exe'
$gal = Join-Path $wt '.venv\Scripts\gal.exe'
$resumable = Join-Path $wt 'scripts\gal_warehouse\run_resumable_p1_report.py'
$head = (& git -C $wt rev-parse HEAD).Trim()
$root = 'D:\DEV\AnotherNetworkFactory\warehouses\GAL_warehouse'
$gff = 'D:\DEV\AnotherNetworkFactory\warehouses\GFF_warehouse'
$metadata = Join-Path $root 'metadata\symbol_metadata.parquet'
$logs = Join-Path $root 'logs\p1_parallel'
$schedulerLog = Join-Path $logs 'scheduler.log'
$attemptsPath = Join-Path $logs 'attempts.json'
$dashboardJson = Join-Path $logs 'DASHBOARD.json'
$dashboardMd = Join-Path $logs 'DASHBOARD.md'
$maxParallel = [Math]::Max(1, $MaxParallel)
$maxAttempts = [Math]::Max(1, $MaxAttempts)
$pollSeconds = [Math]::Max(10, $PollSeconds)
New-Item -ItemType Directory -Force $logs | Out-Null

$jobs = @(
  @{
    Name = 'similarity10_p1'
    Batch = 'similarity10'
    P1 = "$gff\similarity_p1_recursive_20260601_20260717\p1"
    Range = "$gff\similarity_range_recursive_20260601_20260717\similarity_p1_range"
    Dimensions = 'sector_code,industry_code,country,market_cap_bucket'
    Extra = @('--require-consensus')
  },
  @{
    Name = 'implemented27_p1'
    Batch = 'implemented27'
    P1 = "$gff\interaction_implemented27_20260601_20260717_reverse"
    Range = $null
    Dimensions = 'sector_code,industry_code,country,market_cap_bucket'
    Extra = @()
  },
  @{
    Name = 'remaining14_p1'
    Batch = 'remaining14'
    P1 = "$gff\interaction_remaining14_nff_native_20260601_20260717_reverse\p1"
    Range = $null
    Dimensions = 'sector_code,industry_code,country,market_cap_bucket'
    Extra = @()
  }
)

function Write-SchedulerLog($message) {
  Add-Content -LiteralPath $schedulerLog -Value ("{0} {1}" -f (Get-Date -Format s), $message)
}

function Load-Attempts {
  $result = @{}
  if (Test-Path -LiteralPath $attemptsPath) {
    try {
      $payload = Get-Content -LiteralPath $attemptsPath -Raw | ConvertFrom-Json
      foreach ($property in $payload.PSObject.Properties) {
        $result[$property.Name] = [int]$property.Value
      }
    } catch {
      Write-SchedulerLog "WARN could not parse attempts.json: $($_.Exception.Message)"
    }
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

function Get-ActiveP1Names {
  $active = Get-CimInstance Win32_Process | Where-Object {
    $_.Name -in @('python.exe', 'gal.exe') -and (
      $_.CommandLine -like '*run_resumable_p1_report.py*' -or $_.CommandLine -like '*p1-report*'
    )
  }
  $names = @()
  foreach ($p in $active) {
    if ($p.CommandLine -match 'reports\\([^\s"]+)') { $names += $Matches[1] }
  }
  $names | Sort-Object -Unique
}

function Get-Output($jobSpec) { Join-Path $root "reports\$($jobSpec.Name)" }

function Test-JobReady($jobSpec) {
  if (-not (Test-Path -LiteralPath $jobSpec.P1)) { return $false }
  if (-not (Test-Path -LiteralPath $metadata)) { return $false }
  if ($jobSpec.Range -and -not (Test-Path -LiteralPath $jobSpec.Range)) { return $false }
  return $true
}

function Start-P1Job($jobSpec, $attempt) {
  $out = Get-Output $jobSpec
  $temp = Join-Path $root "duckdb_tmp\$($jobSpec.Name)"
  New-Item -ItemType Directory -Force $temp | Out-Null
  $args = @(
    $resumable,
    '--batch-id', $jobSpec.Batch,
    '--p1-root', $jobSpec.P1,
    '--metadata', $metadata,
    '--dimensions', $jobSpec.Dimensions,
    '--start-date', '2026-06-01',
    '--end-date', '2026-07-17',
    '--expected-date-count', '33',
    '--memory-limit-gb', '24',
    '--threads', '8',
    '--temp-directory', $temp,
    '--output', $out,
    '--gal-exe', $gal,
    '--expected-git-commit', $head,
    '--require-clean'
  )
  if ($jobSpec.Range) { $args += @('--range-root', $jobSpec.Range) }
  $args += $jobSpec.Extra
  Start-Process -FilePath $python -ArgumentList $args -WorkingDirectory $wt -RedirectStandardOutput "$logs\$($jobSpec.Name).stdout.log" -RedirectStandardError "$logs\$($jobSpec.Name).stderr.log" -WindowStyle Hidden | Out-Null
  Write-SchedulerLog "STARTED $($jobSpec.Name) attempt=$attempt output=$out"
}

function Write-Dashboard($activeNames, $attempts) {
  $rows = @()
  foreach ($jobSpec in $jobs) {
    $out = Get-Output $jobSpec
    $success = Test-Path (Join-Path $out '_SUCCESS')
    $active = $activeNames -contains $jobSpec.Name
    $progressPath = Join-Path $out 'progress.json'
    $progress = $null
    if (Test-Path $progressPath) {
      try { $progress = Get-Content -LiteralPath $progressPath -Raw | ConvertFrom-Json } catch { $progress = $null }
    }
    $status = if ($success) { 'complete' } elseif ($active) { 'running' } elseif ([int]$attempts[$jobSpec.Name] -ge $maxAttempts) { 'exhausted' } elseif (Test-JobReady $jobSpec) { 'retryable' } else { 'waiting_input' }
    $rows += [ordered]@{
      name = $jobSpec.Name
      batch = $jobSpec.Batch
      status = $status
      attempts = [int]$attempts[$jobSpec.Name]
      completed_units = if ($progress) { [int]$progress.completed_units } else { 0 }
      total_units = if ($progress) { [int]$progress.total_units } else { 0 }
      progress_pct = if ($progress) { [double]$progress.progress_pct } else { 0.0 }
      current_unit = if ($progress) { [string]$progress.current_unit } else { '' }
      updated_at = if ($progress) { [string]$progress.updated_at } else { '' }
      output = $out
    }
  }
  $payload = [ordered]@{ updated_at = (Get-Date -Format o); max_parallel = $maxParallel; max_attempts = $maxAttempts; jobs = $rows }
  $tmpJson = "$dashboardJson.part"
  $payload | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $tmpJson -Encoding UTF8
  Move-Item -LiteralPath $tmpJson -Destination $dashboardJson -Force
  $lines = @('# GAL P1 Scheduler', '', "Updated: $($payload.updated_at)", '', '| Job | Status | Attempts | Progress | Current |', '|---|---:|---:|---:|---|')
  foreach ($row in $rows) { $lines += "| $($row.name) | $($row.status) | $($row.attempts) | $($row.completed_units)/$($row.total_units) ($($row.progress_pct)%) | $($row.current_unit) |" }
  $tmpMd = "$dashboardMd.part"
  $lines | Set-Content -LiteralPath $tmpMd -Encoding UTF8
  Move-Item -LiteralPath $tmpMd -Destination $dashboardMd -Force
}

$attempts = Load-Attempts
Write-SchedulerLog "scheduler started commit=$head max_parallel=$maxParallel max_attempts=$maxAttempts"

while ($true) {
  $activeNames = @(Get-ActiveP1Names)
  Write-Dashboard $activeNames $attempts
  Write-SchedulerLog ("ACTIVE {0} [{1}]" -f $activeNames.Count, ($activeNames -join ','))

  while ($activeNames.Count -lt $maxParallel) {
    $next = $null
    foreach ($jobSpec in $jobs) {
      $out = Get-Output $jobSpec
      $success = Test-Path (Join-Path $out '_SUCCESS')
      $attempt = [int]$attempts[$jobSpec.Name]
      if ((Test-JobReady $jobSpec) -and (-not $success) -and ($activeNames -notcontains $jobSpec.Name) -and $attempt -lt $maxAttempts) {
        $next = $jobSpec
        break
      }
    }
    if ($null -eq $next) { break }
    $attempts[$next.Name] = ([int]$attempts[$next.Name]) + 1
    Save-Attempts $attempts
    Start-P1Job $next $attempts[$next.Name]
    Start-Sleep -Seconds 5
    $activeNames = @(Get-ActiveP1Names)
  }

  $allDone = $true
  $exhausted = @()
  foreach ($jobSpec in $jobs) {
    if (-not (Test-Path (Join-Path (Get-Output $jobSpec) '_SUCCESS'))) {
      $allDone = $false
      if ([int]$attempts[$jobSpec.Name] -ge $maxAttempts -and ($activeNames -notcontains $jobSpec.Name)) { $exhausted += $jobSpec.Name }
    }
  }
  Write-Dashboard $activeNames $attempts
  if ($allDone -and $activeNames.Count -eq 0) {
    Write-SchedulerLog 'scheduler finished'
    break
  }
  if ($exhausted.Count -gt 0 -and $activeNames.Count -eq 0) {
    throw "P1 scheduler exhausted retries: $($exhausted -join ', ')"
  }
  Start-Sleep -Seconds $pollSeconds
}
