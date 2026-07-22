$ErrorActionPreference = 'Stop'

$wt = 'C:\Users\A001\.config\superpowers\worktrees\GraphAlphaLab\gal-report-9e67'
$exe = Join-Path $wt '.venv\Scripts\gal.exe'
$head = '9e67c2cbd1a713652c16ae41fe3704624950f2bc'
$root = 'D:\DEV\AnotherNetworkFactory\warehouses\GAL_warehouse'
$metadata = Join-Path $root 'metadata\symbol_metadata.parquet'
$logs = Join-Path $root 'logs\alpha_parallel'
$schedulerLog = Join-Path $logs 'scheduler.log'
$maxParallel = 10
New-Item -ItemType Directory -Force $logs | Out-Null

$jobs = @(
  @{ Name = 'remaining14_15m'; Batch = 'remaining14'; Signals = "$root\signals\remaining14"; Label = "$root\labels\forward_15m.parquet"; Contract = "$root\contracts\forward_15m.json"; Temp = "$root\duckdb_tmp\remaining14_15m"; Out = "$root\reports\remaining14_15m" },
  @{ Name = 'implemented27_30m'; Batch = 'implemented27'; Signals = "$root\signals\implemented27"; Label = "$root\labels\forward_30m.parquet"; Contract = "$root\contracts\forward_30m.json"; Temp = "$root\duckdb_tmp\implemented27_30m"; Out = "$root\reports\implemented27_30m" },
  @{ Name = 'remaining14_30m'; Batch = 'remaining14'; Signals = "$root\signals\remaining14"; Label = "$root\labels\forward_30m.parquet"; Contract = "$root\contracts\forward_30m.json"; Temp = "$root\duckdb_tmp\remaining14_30m"; Out = "$root\reports\remaining14_30m" },
  @{ Name = 'implemented27_60m'; Batch = 'implemented27'; Signals = "$root\signals\implemented27"; Label = "$root\labels\forward_60m.parquet"; Contract = "$root\contracts\forward_60m.json"; Temp = "$root\duckdb_tmp\implemented27_60m"; Out = "$root\reports\implemented27_60m" },
  @{ Name = 'remaining14_60m'; Batch = 'remaining14'; Signals = "$root\signals\remaining14"; Label = "$root\labels\forward_60m.parquet"; Contract = "$root\contracts\forward_60m.json"; Temp = "$root\duckdb_tmp\remaining14_60m"; Out = "$root\reports\remaining14_60m" },
  @{ Name = 'implemented27_120m'; Batch = 'implemented27'; Signals = "$root\signals\implemented27"; Label = "$root\labels\forward_120m.parquet"; Contract = "$root\contracts\forward_120m.json"; Temp = "$root\duckdb_tmp\implemented27_120m"; Out = "$root\reports\implemented27_120m" },
  @{ Name = 'remaining14_120m'; Batch = 'remaining14'; Signals = "$root\signals\remaining14"; Label = "$root\labels\forward_120m.parquet"; Contract = "$root\contracts\forward_120m.json"; Temp = "$root\duckdb_tmp\remaining14_120m"; Out = "$root\reports\remaining14_120m" },
  @{ Name = 'implemented27_180m'; Batch = 'implemented27'; Signals = "$root\signals\implemented27"; Label = "$root\labels\forward_180m.parquet"; Contract = "$root\contracts\forward_180m.json"; Temp = "$root\duckdb_tmp\implemented27_180m"; Out = "$root\reports\implemented27_180m" },
  @{ Name = 'remaining14_180m'; Batch = 'remaining14'; Signals = "$root\signals\remaining14"; Label = "$root\labels\forward_180m.parquet"; Contract = "$root\contracts\forward_180m.json"; Temp = "$root\duckdb_tmp\remaining14_180m"; Out = "$root\reports\remaining14_180m" }
)

function Write-SchedulerLog($message) {
  Add-Content -LiteralPath $schedulerLog -Value ("{0} {1}" -f (Get-Date -Format s), $message)
}

function Get-ActiveAlphaNames {
  $active = Get-CimInstance Win32_Process | Where-Object {
    $_.Name -in @('gal.exe', 'python.exe') -and $_.CommandLine -like '*alpha-report*'
  }
  $names = @()
  foreach ($p in $active) {
    if ($p.CommandLine -match 'reports\\([^\s"]+)') {
      $names += $Matches[1]
    }
  }
  $names | Sort-Object -Unique
}

function Start-AlphaJob($jobSpec) {
  New-Item -ItemType Directory -Force $jobSpec.Temp | Out-Null
  $args = @(
    'alpha-report',
    '--batch-id', $jobSpec.Batch,
    '--signals', $jobSpec.Signals,
    '--labels', $jobSpec.Label,
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

Write-SchedulerLog 'scheduler started'

while ($true) {
  $activeNames = @(Get-ActiveAlphaNames)
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
    Start-AlphaJob $next
    Start-Sleep -Seconds 5
    $activeNames = @(Get-ActiveAlphaNames)
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
