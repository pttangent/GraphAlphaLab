$ErrorActionPreference = 'Stop'

$wt = 'C:\Users\A001\.config\superpowers\worktrees\GraphAlphaLab\gal-report-9e67'
$exe = Join-Path $wt '.venv\Scripts\gal.exe'
$head = '9e67c2cbd1a713652c16ae41fe3704624950f2bc'
$root = 'D:\DEV\AnotherNetworkFactory\warehouses\GAL_warehouse'
$gff = 'D:\DEV\AnotherNetworkFactory\warehouses\GFF_warehouse'
$metadata = Join-Path $root 'metadata\symbol_metadata.parquet'
$logs = Join-Path $root 'logs\p1_parallel'
$schedulerLog = Join-Path $logs 'scheduler.log'
$maxParallel = 3
New-Item -ItemType Directory -Force $logs | Out-Null

$jobs = @(
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
  },
  @{
    Name = 'similarity10_p1'
    Batch = 'similarity10'
    P1 = "$gff\similarity_p1_recursive_20260601_20260717\p1"
    Range = "$gff\similarity_range_recursive_20260601_20260717\similarity_p1_range"
    Dimensions = 'sector_code,industry_code,country,market_cap_bucket'
    Extra = @('--require-consensus')
  }
)

function Write-SchedulerLog($message) {
  Add-Content -LiteralPath $schedulerLog -Value ("{0} {1}" -f (Get-Date -Format s), $message)
}

function Get-ActiveP1Names {
  $active = Get-CimInstance Win32_Process | Where-Object {
    $_.Name -in @('gal.exe', 'python.exe') -and $_.CommandLine -like '*p1-report*'
  }
  $names = @()
  foreach ($p in $active) {
    if ($p.CommandLine -match 'reports\\([^\s"]+)') {
      $names += $Matches[1]
    }
  }
  $names | Sort-Object -Unique
}

function Start-P1Job($jobSpec) {
  $out = Join-Path $root "reports\$($jobSpec.Name)"
  $temp = Join-Path $root "duckdb_tmp\$($jobSpec.Name)"
  New-Item -ItemType Directory -Force $temp | Out-Null
  $args = @(
    'p1-report',
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
    '--expected-git-commit', $head,
    '--require-clean'
  )
  if ($jobSpec.Range) {
    $args += @('--range-root', $jobSpec.Range)
  }
  $args += $jobSpec.Extra
  Start-Process -FilePath $exe -ArgumentList $args -WorkingDirectory $wt -RedirectStandardOutput "$logs\$($jobSpec.Name).stdout.log" -RedirectStandardError "$logs\$($jobSpec.Name).stderr.log" -WindowStyle Hidden | Out-Null
  Write-SchedulerLog "STARTED $($jobSpec.Name)"
}

Write-SchedulerLog 'scheduler started'

while ($true) {
  $activeNames = @(Get-ActiveP1Names)
  Write-SchedulerLog ("ACTIVE {0} [{1}]" -f $activeNames.Count, ($activeNames -join ','))

  while ($activeNames.Count -lt $maxParallel) {
    $next = $null
    foreach ($jobSpec in $jobs) {
      $out = Join-Path $root "reports\$($jobSpec.Name)"
      $success = Test-Path (Join-Path $out '_SUCCESS')
      $exists = Test-Path $out
      if ((-not $success) -and (-not $exists) -and ($activeNames -notcontains $jobSpec.Name)) {
        $next = $jobSpec
        break
      }
    }
    if ($null -eq $next) {
      break
    }
    Start-P1Job $next
    Start-Sleep -Seconds 5
    $activeNames = @(Get-ActiveP1Names)
  }

  $allDone = $true
  foreach ($jobSpec in $jobs) {
    $out = Join-Path $root "reports\$($jobSpec.Name)"
    if (-not (Test-Path (Join-Path $out '_SUCCESS'))) {
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
