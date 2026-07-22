param(
  [string]$ThemeMemberships = 'D:\DEV\AnotherNetworkFactory\warehouses\GFF_warehouse\similarity_p1_recursive_20260601_20260717\p1',
  [int]$MaxParallel = 3
)

$ErrorActionPreference = 'Stop'

$wt = 'C:\Users\A001\.config\superpowers\worktrees\GraphAlphaLab\gal-report-9e67'
$exe = Join-Path $wt '.venv\Scripts\gal.exe'
$python = Join-Path $wt '.venv\Scripts\python.exe'
$withinThemeScript = Join-Path $wt 'scripts\gal_warehouse\run_within_theme_alpha.py'
$head = (& git -C $wt rev-parse HEAD).Trim()
$root = 'D:\DEV\AnotherNetworkFactory\warehouses\GAL_warehouse'
$metadata = Join-Path $root 'metadata\symbol_metadata.parquet'
$logs = Join-Path $root 'logs\alpha_parallel'
$schedulerLog = Join-Path $logs 'scheduler.log'
$maxParallel = [Math]::Max(1, $MaxParallel)
New-Item -ItemType Directory -Force $logs | Out-Null

# Priority order:
# 1. Fixed-direction trade_intensity falsification inside lagged Similarity consensus themes.
# 2. Broad graph-forward discovery inside lagged themes for IG27/RM14.
# 3. Existing global cross-sectional alpha jobs.
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

function Write-SchedulerLog($message) {
  Add-Content -LiteralPath $schedulerLog -Value ("{0} {1}" -f (Get-Date -Format s), $message)
}

function Test-ThemeReady {
  if (-not (Test-Path -LiteralPath $ThemeMemberships)) {
    return $false
  }
  $membership = Get-ChildItem -LiteralPath $ThemeMemberships -Recurse -Filter 'memberships.parquet' -File -ErrorAction SilentlyContinue | Select-Object -First 1
  return $null -ne $membership
}

function Test-JobReady($jobSpec, [bool]$themeReady) {
  if (-not (Test-Path -LiteralPath $jobSpec.Signals)) { return $false }
  if (-not (Test-Path -LiteralPath $jobSpec.Label)) { return $false }
  if (-not (Test-Path -LiteralPath $jobSpec.Contract)) { return $false }
  if ($jobSpec.Kind -eq 'WithinTheme' -and -not $themeReady) { return $false }
  return $true
}

function Get-ActiveAlphaNames {
  $active = Get-CimInstance Win32_Process | Where-Object {
    $_.Name -in @('gal.exe', 'python.exe') -and (
      $_.CommandLine -like '*alpha-report*' -or $_.CommandLine -like '*run_within_theme_alpha.py*'
    )
  }
  $names = @()
  foreach ($p in $active) {
    if ($p.CommandLine -match 'reports\\([^\s"]+)') {
      $names += $Matches[1]
    }
  }
  $names | Sort-Object -Unique
}

function Start-GlobalAlphaJob($jobSpec) {
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
    '--output', $jobSpec.Out
  )
  if ($jobSpec.FactorIds) {
    $args += @('--factor-ids', $jobSpec.FactorIds)
  }
  Start-Process -FilePath $python -ArgumentList $args -WorkingDirectory $wt -RedirectStandardOutput "$logs\$($jobSpec.Name).stdout.log" -RedirectStandardError "$logs\$($jobSpec.Name).stderr.log" -WindowStyle Hidden | Out-Null
}

function Start-AlphaJob($jobSpec) {
  New-Item -ItemType Directory -Force $jobSpec.Temp | Out-Null
  if ($jobSpec.Kind -eq 'WithinTheme') {
    Start-WithinThemeAlphaJob $jobSpec
  } else {
    Start-GlobalAlphaJob $jobSpec
  }
  Write-SchedulerLog "STARTED $($jobSpec.Name) kind=$($jobSpec.Kind)"
}

Write-SchedulerLog "scheduler started commit=$head max_parallel=$maxParallel theme_memberships=$ThemeMemberships"

while ($true) {
  $themeReady = Test-ThemeReady
  $activeNames = @(Get-ActiveAlphaNames)
  Write-SchedulerLog ("ACTIVE {0} [{1}] theme_ready={2}" -f $activeNames.Count, ($activeNames -join ','), $themeReady)

  while ($activeNames.Count -lt $maxParallel) {
    $next = $null
    foreach ($jobSpec in $jobs) {
      $success = Test-Path (Join-Path $jobSpec.Out '_SUCCESS')
      $exists = Test-Path $jobSpec.Out
      $ready = Test-JobReady $jobSpec $themeReady
      if ($ready -and (-not $success) -and (-not $exists) -and ($activeNames -notcontains $jobSpec.Name)) {
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

  if (-not $themeReady) {
    Write-SchedulerLog 'WAITING for Similarity consensus memberships; global jobs may continue meanwhile'
  }
  Start-Sleep -Seconds 60
}
