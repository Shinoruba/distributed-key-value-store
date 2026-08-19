$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RootDir = Split-Path -Parent $ScriptDir
$BinPath = Join-Path $RootDir "build\DistributedKVStore\Release\kvstore-server.exe"

if (-not (Test-Path $BinPath)) {
    $BinPath = Join-Path $RootDir "build\DistributedKVStore\kvstore-server.exe"
}

if (-not (Test-Path $BinPath)) {
    Write-Error "Server binary not found at $BinPath. Please build the project first."
    exit 1
}

$DataDir = Join-Path $RootDir "data"
if (-not (Test-Path $DataDir)) {
    New-Item -ItemType Directory -Path $DataDir | Out-Null
}

$Peers1 = "node_2=127.0.0.1:7081,node_3=127.0.0.1:7082"
$Peers2 = "node_1=127.0.0.1:7080,node_3=127.0.0.1:7082"
$Peers3 = "node_1=127.0.0.1:7080,node_2=127.0.0.1:7081"

Write-Host "Starting DistributedKVStore 3-Node Cluster..." -ForegroundColor Cyan

function Start-NodeProcess($id, $port, $raftPort, $peers, $wal) {
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $BinPath
    $psi.Arguments = "--id $id --port $port --raft-port $raftPort --peers `"$peers`" --wal `"$wal`""
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $true
    $proc = [System.Diagnostics.Process]::Start($psi)
    return $proc
}

$Proc1 = Start-NodeProcess "node_1" 6380 7080 $Peers1 "$DataDir\node1.wal"
Write-Host "  [Started] Node 1 (Client: 6380, Raft: 7080, PID: $($Proc1.Id))" -ForegroundColor Green

$Proc2 = Start-NodeProcess "node_2" 6381 7081 $Peers2 "$DataDir\node2.wal"
Write-Host "  [Started] Node 2 (Client: 6381, Raft: 7081, PID: $($Proc2.Id))" -ForegroundColor Green

$Proc3 = Start-NodeProcess "node_3" 6382 7082 $Peers3 "$DataDir\node3.wal"
Write-Host "  [Started] Node 3 (Client: 6382, Raft: 7082, PID: $($Proc3.Id))" -ForegroundColor Green

$PidFile = Join-Path $DataDir "cluster.pids"
"$($Proc1.Id)`n$($Proc2.Id)`n$($Proc3.Id)" | Set-Content -Path $PidFile

Start-Sleep -Milliseconds 600

Write-Host "`nCluster successfully started!" -ForegroundColor Green
Write-Host "Connect via CLI: .\build\DistributedKVStore\Release\kvstore-cli.exe --port 6380"