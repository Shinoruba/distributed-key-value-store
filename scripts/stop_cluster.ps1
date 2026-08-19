$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RootDir = Split-Path -Parent $ScriptDir
$PidFile = Join-Path $RootDir "data\cluster.pids"

Write-Host "Stopping DistributedKVStore Cluster..." -ForegroundColor Yellow

if (Test-Path $PidFile) {
    $Pids = Get-Content $PidFile
    foreach ($pid_str in $Pids) {
        $p = $pid_str.Trim()
        if ($p) {
            try {
                Stop-Process -Id ([int]$p) -Force -ErrorAction SilentlyContinue
                Write-Host "  [Stopped] Process PID $p" -ForegroundColor Green
            } catch {
            }
        }
    }
    Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
}

Get-Process "kvstore-server" -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue

Write-Host "Cluster stopped successfully." -ForegroundColor Green