$ErrorActionPreference = 'Stop'

$wt = 'C:\Users\A001\.config\superpowers\worktrees\GraphAlphaLab\gal-report-9e67'
$python = Join-Path $wt '.venv\Scripts\python.exe'
$script = Join-Path $wt 'scripts\gal_warehouse\build_daily_labels_only.py'
$root = 'D:\DEV\AnotherNetworkFactory\warehouses\GAL_warehouse'
$logs = Join-Path $root 'logs\daily_label_build'
New-Item -ItemType Directory -Force $logs | Out-Null

if (-not (Test-Path -LiteralPath $script)) {
  throw "Version-controlled label builder is missing: $script"
}

$log = Join-Path $logs 'build_daily_labels_only.log'
& $python $script *> $log
if ($LASTEXITCODE -ne 0) {
  throw "Daily label builder failed with exit code $LASTEXITCODE. See $log"
}
