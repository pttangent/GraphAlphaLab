param(
  [string]$ThemeMemberships = 'D:\DEV\AnotherNetworkFactory\warehouses\GFF_warehouse\similarity_p1_recursive_20260601_20260717\p1',
  [int]$MaxParallel = 3,
  [int]$MaxAttempts = 5,
  [int]$PollSeconds = 60
)

$ErrorActionPreference = 'Stop'

$wt = 'C:\Users\A001\.config\superpowers\worktrees\GraphAlphaLab\gal-report-9e67'
$python = Join-Path $wt '.venv\Scripts\python.exe'
$resumableAlphaScript = Join-Path $wt 'scripts\gal_warehouse\run_resumable_alpha_report.py'
$withinThemeScript = Join-Path $wt 'scripts\gal_warehouse\run_within_theme_alpha.py'
$head = (& git -C $wt rev-parse HEAD).Trim()
$root = 'D:\DEV\AnotherNetworkFactory\warehouses\GAL_warehouse'
$metadata = Join-Path $root 'metadata\symbol_metadata.parquet'
$logs = Join-Path $root 'logs\alpha_parallel'
$schedulerLog = Join-Path $logs 'scheduler.log'
$attemptsPath = Join-Path $logs 'attempts.json'
$dashboardJson = Join-Path $logs 'DASHBOARD.json'
$dashboardMd = Join-Path $logs 'DASHBOARD.md'
$maxParallel = [Math]::Max(1, $MaxParallel)
$maxAttempts = [Math]::Max(1, $MaxAttempts)
$pollSeconds = [Math]::Max(10, $PollSeconds)
New-Item -ItemType Directory -Force $logs | Out-Null

$jobs = @(
  @{ Kind = 'WithinTheme'; Name = 'within_theme_trade_intensity_30m'; Batch = 'implemented27'; Signals = "$root\signals\implemented27"; Label = "$root\labels\forward_30m.parquet"; Contract = "$root\contracts\forward_30m.json"; Theme = $ThemeMemberships; FactorIds = 'trade_intensity_to_volatility__graph_forward'; Variants = 'graph_forward'; Direction = 'negative'; Temp = "$root\duckdb_tmp\within_theme_trade_intensity_30m"; Out = "$root\reports\within_theme_trade_intensity_30m" },
  @{ Kind = 'WithinTheme'; Name = 'within_theme_trade_intensity_60m'; Batch = 'implemented27'; Signals = "$root\signals\implemented27"; Label = "$root\labels\forward_60m.parquet"; Contract = "$root\contracts\forward_60m.json"; Theme = $ThemeMemberships; FactorIds = 'trade_intensity_to_volatility__graph_forward'; Variants = 'graph_forward'; Direction = 'negative'; Temp = "$root\duckdb_tmp\within_theme_trade_intensity_60m"; Out = "$root\reports\within_theme_trade_intensity_60m" },
  @{ Kind = 'WithinTheme'; Name = 'within_theme_trade_intensity_120m'; Batch = 'implemented27'; Signals = "$root\signals\implemented27"; Label = "$root\labels\forward_120m.parquet"; Contract = "$root\contracts\forward_120m.json"; Theme = $ThemeMemberships; FactorIds = 'trade_intensity_to_volatility__graph_forward'; Variants = 'graph_forward'; Direction = 'negative'; Temp = "$root\duckdb_tmp\within_theme_trade_intensity_120m"; Out = "$root\reports\within_theme_trade_intensity_120m" },
  @{ Kind = 'WithinTheme'; Name = 'within_theme_implemented27_30m'; Batch = 'implemented27'; Signals = "$root\signals\implemented27"; Label = "$root\labels\forward_30m.parquet"; Contract = "$root\contracts\forward_30m.json"; Theme = $ThemeMemberships; FactorIds = ''; Variants = 'graph_forward'; Direction = 'auto'; Temp = "$root\duckdb_tmp\within_theme_implemented27_30m"; Out = "$root\reports\within_theme_implemented27_30m" },
  @{ Kind = 'WithinTheme'; Name = 'within_theme_remaining14_30m'; Batch = 'remaining14'; Signals = "$root\signals\remaining14"; Label = "$root\labels\forward_30m.parquet"; Contract = "$root\contracts\forward_30m.json"; Theme = $ThemeMemberships; FactorIds = ''; Variants = 'graph_forward'; Direction = 'auto'; Temp = "$root\duckdb_tmp\within_theme_remaining14_30m"; Out = "$root\reports\within_theme_remaining14_30m" },
  @{ Kind = 'WithinTheme'; Name = 'within_theme_implemented27_60m'; Batch = 'implemented27'; Signals = "$root\signals\implemented27"; Label = "$root\labels\forward_60m.parquet"; Contract = "$root\contracts\forward_60m.json"; Theme = $ThemeMemberships; FactorIds = ''; Variants = 'graph_forward'; Direction = 'auto'; Temp = "$root\duckdb_tmp\within_theme_implemented27_60m"; Out = "$root\reports\within_theme_implemented27_60m" },
  @{ Kind = 'WithinTheme'; Name = 'within_theme_remaining14_60m'; Batch = 'remaining14'; Signals = "$root\signals\remaining14"; Label = "$root\labels\forward_60m.parquet"; Contract = "$root\contracts\forward_60m.json"; Theme = $ThemeMemberships; FactorIds = ''; Variants = 'graph_forward'; Direction = 'auto'; Temp = "$root\duckdb_tmp\within_theme_remaining14_60m"; Out = "$root\reports\within_theme_remaining14_60m" },
  @{ Kind = 'WithinTheme'; Name = 'within_theme_implemented27_120m'; Batch = 'implemented27'; Signals = "$root\signals\implemented27"; Label = "$root\labels\forward_120m.parquet"; Contract = "$root\contracts\forward_120m.json"; Theme = $ThemeMemberships; FactorIds = ''; Variants = 'graph_forward'; Direction = 'auto'; Temp = "$root\duckdb_tmp\within_theme_implemented27_120m"; Out = "$root\reports\within_theme_implemented27_120m" },
  @{ Kind = 'WithinTheme'; Name = 'within_theme_remaining14_120m'; Batch = 'remaining14'; Signals = "$root\signals\remaining14"; Label = "$root\labels\forward_120m.parquet"; Contract = "$root\contracts\forward_120m.json"; Theme = $ThemeMemberships; FactorIds = ''; Variants = 'graph_forward'; Direction = 'auto'; Temp = "$root\duckdb_tmp\within_theme_remaining14_120m"; Out = "$root\reports\within_theme_remaining14_120m" },
  @{ Kind = 'Global'; Name = 'remaining14_15m'; Batch = 'remaining14'; Signals = "$root\signals\remaining14"; Label = "$root\labels\forward_15m.parquet"; Contract = "$root\contracts\forward_15m.json"; Temp = "$root\duckdb_tmp\remaining14_15m"; Out = "$root\reports\remaining14_15m" },
  @{ Kind = 'Global'; Name = 'implemented27_30m'; Batch = 'implemented27'; Signals = "$root\signals\implemented27"; Label = "$root\labels\forward_30m.parquet"; Contract = "$root\contracts\forward_30m.json"; Temp = "$root\duckdb_tmp\implemented27_30m"; Out = "$root\reports\implemented27_30m" },
  @{ Kind = 'Global'; Name = 'remaining14_30m'; Batch = 'remaining14'; Signals = "$root\signals\remaining14"; Label = "$root\labels\forward_30m.parquet"; Contract = "$root\contracts\forward_30m.json"; Temp = "$root\duckdb_tmp\remaining14_30m"; Out = "$root\reports\remaining14_30m" },
  @{ Kind = 'Global'; Name = 'implemented27_60m'; Batch = 'implemented27'; Signals = "$root\signals\implemented27"; Label = "$root\labels\forward_60m.parquet"; Contract = "$root\contracts\forward_60m.json"; Temp = "$root\duckdb_tmp\implemented27_60m"; Out = "$root\reports\implemented27_60m" },
  @{ Kind = 'Global'; Name = 'remaining14_60m'; Batch = 'remaining14'; Signals = "$root\signals\remaining14"; Label = "$root\labels\forward_60m.parquet"; Contract = "$root\contracts\forward_60m.json"; Temp = "$root\duckdb_tmp\remaining14_60m"; Out = "$root\reports\remaining14_60m" },
  @{ Kind = 'Global'; Name = 'implemented27_120m'; Batch = 'implemented27'; Signals = "$root\signals\implemented27"; Label = "$root\labels\forward_120m.parquet"; Contract = "$root\contracts\forward_120m.json"; Temp = "$root\duckdb_tmp\implemented27_120m"; Out = "$root\reports\implemented27_120m" },
  @{ Kind = 'Global'; Name = 'remaining14_120m'; Batch = 'remaining14'; Signals = "$root\signals\remaining14"; Label = "$root\labels\forward_120m.parquet"; Contract = "$root\contracts\forward_120m.json"; Temp = "$root\duckdb_tmp\remaining14_120m"; Out = "$root\reports\remaining14_120m" },
  @{ Kind = 'Global'; Name = 'implemented27_180m'; Batch = 'implemented27'; Signals = "$root\signals\implemented27"; Label = "$root\labels\forward_180m.parquet"; Contract = "$root\contracts\forward_180m.json"; Temp = "$root\duckdb_tmp\implemented27_180m"; Out = "$root\reports\implemented27_180m" },
  @{ Kind = 'Global'; Name = 'remaining14_180m'; Batch = 'remaining14'; Signals = "$root\signals\remaining14"; Label = "$root\labels\forward_180m.parquet"; Contract = "$root\contracts\forward_180m.json"; Temp = "$root\duckdb_tmp\remaining14_180m"; Out = "$root\reports\remaining14_180m" }
)

function Write-SchedulerLog($message) { Add-Content -LiteralPath $schedulerLog -Value ("{0} {1}" -f (Get-Date -Format s), $message) }

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

function Test-ThemeReady {
  if (-not (Test-Path -LiteralPath $ThemeMemberships)) { return $false }
  $membership = Get-ChildItem -LiteralPath $ThemeMemberships -Recurse -Filter 'memberships.parquet' -File -ErrorAction SilentlyContinue | Select-Object -First 1
  return $null -ne $membership
}

function Test-JobReady($jobSpec, [bool]$themeReady) {
  if (-not (Test-Path -LiteralPath $jobSpec.Signals)) { return $false }
  if (-not (Test-Path -LiteralPath $jobSpec.Label)) { return $false }
  if (-not (Test-Path -LiteralPath $jobSpec.Contract)) { return $false }
  if (-not (Test-Path -LiteralPath $metadata)) { return $false }
  if ($jobSpec.Kind -eq 'WithinTheme' -and -not $themeReady) { return $false }
  return $true
}

function Get-ActiveAlphaNames {
  $active = Get-CimInstance Win32_Process | Where-Object {
    $_.Name -in @('python.exe', 'gal.exe') -and (
      $_.CommandLine -like '*run_resumable_alpha_report.py*' -or
      $_.CommandLine -like '*run_within_theme_alpha.py*' -or
      $_.CommandLine -like '*alpha-report*'
    )
  }
  $names = @()
  foreach ($p in $active) { if ($p.CommandLine -match 'reports\\([^\s"]+)') { $names += $Matches[1] } }
  $names | Sort-Object -Unique
}

function Start-GlobalAlphaJob($jobSpec) {
  $args = @(
    $resumableAlphaScript,
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
  Start-Process -FilePath $python -ArgumentList $args -WorkingDirectory $wt -RedirectStandardOutput "$logs\$($jobSpec.Name).stdout.log" -RedirectStandardError "$logs\$($jobSpec.Name).stderr.log" -WindowStyle Hidden | Out-Null
}

function Start-WithinThemeAlphaJob($jobSpec) {
  $args = @(
    $withinThemeScript,
    '--batch-id', $jobSpec.Batch,
    '--signals', $jobSpec.Signals,
    '--labels', $jobSpec.Label,
    '--label-contract', $jobSpec.Contract,
    '--memberships', $jobSpec.Theme,
    '--theme-layer-id', 'similarity_consensus',
    '--variant-ids', $jobSpec.Variants,
    '--default-direction', $jobSpec.Direction,
    '--quantiles', '5',
    '--min-theme-size', '20',
    '--min-themes-per-decision', '2',
    '--control-columns', 'own_score',
    '--cost-bps', '0,1,2,5,10',
    '--memory-limit-gb', '24',
    '--threads', '8',
    '--temp-directory', $jobSpec.Temp,
    '--output', $jobSpec.Out,
    '--force'
  )
  if ($jobSpec.FactorIds) { $args += @('--factor-ids', $jobSpec.FactorIds) }
  Start-Process -FilePath $python -ArgumentList $args -WorkingDirectory $wt -RedirectStandardOutput "$logs\$($jobSpec.Name).stdout.log" -RedirectStandardError "$logs\$($jobSpec.Name).stderr.log" -WindowStyle Hidden | Out-Null
}

function Start-AlphaJob($jobSpec, $attempt) {
  New-Item -ItemType Directory -Force $jobSpec.Temp | Out-Null
  if ($jobSpec.Kind -eq 'WithinTheme') { Start-WithinThemeAlphaJob $jobSpec } else { Start-GlobalAlphaJob $jobSpec }
  Write-SchedulerLog "STARTED $($jobSpec.Name) kind=$($jobSpec.Kind) attempt=$attempt"
}

function Write-Dashboard($activeNames, $attempts, [bool]$themeReady) {
  $rows = @()
  foreach ($jobSpec in $jobs) {
    $success = Test-Path (Join-Path $jobSpec.Out '_SUCCESS')
    $active = $activeNames -contains $jobSpec.Name
    $progressPath = Join-Path $jobSpec.Out 'progress.json'
    $progress = $null
    if (Test-Path $progressPath) { try { $progress = Get-Content -LiteralPath $progressPath -Raw | ConvertFrom-Json } catch { $progress = $null } }
    $attempt = [int]$attempts[$jobSpec.Name]
    $status = if ($success) { 'complete' } elseif ($active) { 'running' } elseif ($attempt -ge $maxAttempts) { 'exhausted' } elseif (Test-JobReady $jobSpec $themeReady) { 'retryable' } else { 'waiting_input' }
    $rows += [ordered]@{
      name = $jobSpec.Name; kind = $jobSpec.Kind; status = $status; attempts = $attempt
      completed_units = if ($progress) { [int]$progress.completed_units } else { 0 }
      total_units = if ($progress) { [int]$progress.total_units } else { 0 }
      progress_pct = if ($progress) { [double]$progress.progress_pct } else { 0.0 }
      current_unit = if ($progress) { [string]$progress.current_unit } else { '' }
      output = $jobSpec.Out
    }
  }
  $payload = [ordered]@{ updated_at = (Get-Date -Format o); theme_ready = $themeReady; max_parallel = $maxParallel; max_attempts = $maxAttempts; jobs = $rows }
  $tmpJson = "$dashboardJson.part"
  $payload | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $tmpJson -Encoding UTF8
  Move-Item -LiteralPath $tmpJson -Destination $dashboardJson -Force
  $lines = @('# GAL Alpha Scheduler', '', "Updated: $($payload.updated_at)", "Theme ready: $themeReady", '', '| Job | Kind | Status | Attempts | Progress | Current |', '|---|---|---:|---:|---:|---|')
  foreach ($row in $rows) { $lines += "| $($row.name) | $($row.kind) | $($row.status) | $($row.attempts) | $($row.completed_units)/$($row.total_units) ($($row.progress_pct)%) | $($row.current_unit) |" }
  $tmpMd = "$dashboardMd.part"
  $lines | Set-Content -LiteralPath $tmpMd -Encoding UTF8
  Move-Item -LiteralPath $tmpMd -Destination $dashboardMd -Force
}

$attempts = Load-Attempts
Write-SchedulerLog "scheduler started commit=$head max_parallel=$maxParallel max_attempts=$MaxAttempts theme_memberships=$ThemeMemberships"

while ($true) {
  $themeReady = Test-ThemeReady
  $activeNames = @(Get-ActiveAlphaNames)
  Write-Dashboard $activeNames $attempts $themeReady
  Write-SchedulerLog ("ACTIVE {0} [{1}] theme_ready={2}" -f $activeNames.Count, ($activeNames -join ','), $themeReady)

  while ($activeNames.Count -lt $maxParallel) {
    $next = $null
    foreach ($jobSpec in $jobs) {
      $success = Test-Path (Join-Path $jobSpec.Out '_SUCCESS')
      $attempt = [int]$attempts[$jobSpec.Name]
      if ((Test-JobReady $jobSpec $themeReady) -and (-not $success) -and ($activeNames -notcontains $jobSpec.Name) -and $attempt -lt $maxAttempts) { $next = $jobSpec; break }
    }
    if ($null -eq $next) { break }
    $attempts[$next.Name] = ([int]$attempts[$next.Name]) + 1
    Save-Attempts $attempts
    Start-AlphaJob $next $attempts[$next.Name]
    Start-Sleep -Seconds 5
    $activeNames = @(Get-ActiveAlphaNames)
  }

  $allDone = $true
  $exhausted = @()
  foreach ($jobSpec in $jobs) {
    if (-not (Test-Path (Join-Path $jobSpec.Out '_SUCCESS'))) {
      $allDone = $false
      if ([int]$attempts[$jobSpec.Name] -ge $maxAttempts -and ($activeNames -notcontains $jobSpec.Name)) { $exhausted += $jobSpec.Name }
    }
  }
  Write-Dashboard $activeNames $attempts $themeReady
  if ($allDone -and $activeNames.Count -eq 0) { Write-SchedulerLog 'scheduler finished'; break }
  if ($exhausted.Count -gt 0 -and $activeNames.Count -eq 0) { throw "Alpha scheduler exhausted retries: $($exhausted -join ', ')" }
  if (-not $themeReady) { Write-SchedulerLog 'WAITING for Similarity consensus memberships; global jobs may continue meanwhile' }
  Start-Sleep -Seconds $pollSeconds
}
