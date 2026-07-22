$ErrorActionPreference = 'Stop'

$wt = 'C:\Users\A001\.config\superpowers\worktrees\GraphAlphaLab\gal-report-9e67'
$exe = Join-Path $wt '.venv\Scripts\gal.exe'
$head = '9e67c2cbd1a713652c16ae41fe3704624950f2bc'
$root = 'D:\DEV\AnotherNetworkFactory\warehouses\GAL_warehouse'
$metadata = Join-Path $root 'metadata\symbol_metadata.parquet'
$labels = Join-Path $root 'labels\daily_forward.parquet'
$contracts = Join-Path $root 'contracts'
$rthVerification = Join-Path $root 'label_diagnostics\daily_forward_rth_verified.json'
$logs = Join-Path $root 'logs\daily_alpha'
$schedulerLog = Join-Path $logs 'scheduler.log'
$maxParallel = 10
New-Item -ItemType Directory -Force $logs | Out-Null

function Write-SchedulerLog($message) {
  Add-Content -LiteralPath $schedulerLog -Value ("{0} {1}" -f (Get-Date -Format s), $message)
}

function Get-ActiveDailyAlphaNames {
  $active = Get-CimInstance Win32_Process | Where-Object {
    $_.Name -in @('gal.exe', 'python.exe') -and $_.CommandLine -like '*alpha-report*' -and $_.CommandLine -like '*reports\daily_alpha*'
  }
  $names = @()
  foreach ($p in $active) {
    if ($p.CommandLine -match '--output\s+([^\s"]+)') {
      $names += Split-Path $Matches[1] -Leaf
    } elseif ($p.CommandLine -match 'reports\\daily_alpha\\([^\s"]+)') {
      $names += $Matches[1]
    }
  }
  $names | Sort-Object -Unique
}

if (-not (Test-Path $labels)) {
  throw "Daily label file is missing: $labels. Run build_bars_labels.py first."
}

if (-not (Test-Path $rthVerification)) {
  throw "Daily label RTH verification is missing: $rthVerification. Rebuild labels with run_daily_label_build.ps1 before scheduling daily alpha."
}

$rthPayload = Get-Content -LiteralPath $rthVerification -Raw | ConvertFrom-Json
if ([int64]$rthPayload.invalid_decision_rows -ne 0) {
  throw "Daily label RTH verification failed: invalid_decision_rows=$($rthPayload.invalid_decision_rows). Rebuild labels before scheduling daily alpha."
}

$labelIds = Get-ChildItem -LiteralPath $contracts -Filter 'daily_*.json' |
  ForEach-Object { $_.BaseName } |
  Sort-Object

if ($labelIds.Count -eq 0) {
  throw "No daily label contracts found under $contracts. Run build_bars_labels.py first."
}

$jobs = @()
foreach ($batch in @('implemented27', 'remaining14')) {
  foreach ($labelId in $labelIds) {
    $jobs += @{
      Name = "$($batch)_$($labelId)"
      Batch = $batch
      Signals = Join-Path $root "signals\$batch"
      LabelId = $labelId
      Contract = Join-Path $contracts "$labelId.json"
      Out = Join-Path $root "reports\daily_alpha\$($batch)_$($labelId)"
      Temp = Join-Path $root "duckdb_tmp\daily_alpha\$($batch)_$($labelId)"
    }
  }
}

function Start-DailyAlphaJob($jobSpec) {
  New-Item -ItemType Directory -Force $jobSpec.Temp | Out-Null
  $args = @(
    'alpha-report',
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
  Start-Process -FilePath $exe -ArgumentList $args -WorkingDirectory $wt -RedirectStandardOutput "$logs\$($jobSpec.Name).stdout.log" -RedirectStandardError "$logs\$($jobSpec.Name).stderr.log" -WindowStyle Hidden | Out-Null
  Write-SchedulerLog "STARTED $($jobSpec.Name)"
}

Write-SchedulerLog ("scheduler started; job_count={0}; max_parallel={1}" -f $jobs.Count, $maxParallel)

while ($true) {
  $activeNames = @(Get-ActiveDailyAlphaNames)
  Write-SchedulerLog ("ACTIVE {0} [{1}]" -f $activeNames.Count, ($activeNames -join ','))

  while ($activeNames.Count -lt $maxParallel) {
    $next = $null
    foreach ($jobSpec in $jobs) {
      $success = Test-Path (Join-Path $jobSpec.Out '_SUCCESS')
      $exists = Test-Path $jobSpec.Out
      if ((-not $success) -and (-not $exists) -and ($activeNames -notcontains $jobSpec.Name)) {
        $next = $jobSpec
        break
      }
    }
    if ($null -eq $next) {
      break
    }
    Start-DailyAlphaJob $next
    Start-Sleep -Seconds 5
    $activeNames = @(Get-ActiveDailyAlphaNames)
  }

  $allDone = $true
  foreach ($jobSpec in $jobs) {
    if (-not (Test-Path (Join-Path $jobSpec.Out '_SUCCESS'))) {
      $allDone = $false
      break
    }
  }
  if ($allDone -and $activeNames.Count -eq 0) {
    Write-SchedulerLog 'scheduler finished'
    break
  }

  Start-Sleep -Seconds 60
}
