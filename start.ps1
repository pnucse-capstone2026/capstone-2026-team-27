# 관리자 대시보드 백엔드+프론트엔드 한 번에 실행
# 실행: 프로젝트 루트에서 powershell -ExecutionPolicy Bypass -File start.ps1
#       (또는 PowerShell에서 .\start.ps1)

$RootDir = $PSScriptRoot

function Test-PortFree {
    param([int]$Port)
    $conn = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($conn) {
        $proc = Get-Process -Id $conn.OwningProcess -ErrorAction SilentlyContinue
        $procName = if ($proc) { $proc.ProcessName } else { "알 수 없음" }
        Write-Host "포트 $Port 이 이미 사용 중입니다 (PID $($conn.OwningProcess), 프로세스: $procName)." -ForegroundColor Red
        Write-Host "  먼저 종료하세요: Stop-Process -Id $($conn.OwningProcess) -Force" -ForegroundColor Yellow
        return $false
    }
    return $true
}

$portsOk = (Test-PortFree -Port 8000) -and (Test-PortFree -Port 3000)
if (-not $portsOk) {
    Write-Host "포트 충돌을 해결한 뒤 다시 실행하세요." -ForegroundColor Red
    exit 1
}

Write-Host "1) postgres 컨테이너 확인/기동..."
docker compose -f "$RootDir\docker-compose.yml" up -d postgres
if ($LASTEXITCODE -ne 0) {
    Write-Host "postgres 기동 실패 - Docker Desktop이 켜져 있는지 확인하세요." -ForegroundColor Red
    exit 1
}

Write-Host "2) 백엔드를 새 창에서 실행 (http://localhost:8000)..."
Start-Process powershell -ArgumentList "-NoExit", "-Command", "Set-Location '$RootDir'; python -m api.main"

Write-Host "3) 프론트엔드를 새 창에서 실행 (http://localhost:3000)..."
Start-Process powershell -ArgumentList "-NoExit", "-Command", "Set-Location '$RootDir\frontend'; npm run dev"

Write-Host ""
Write-Host "백엔드/프론트엔드가 각각 새 창에서 뜹니다. 준비되면 http://localhost:3000 접속하세요." -ForegroundColor Green
Write-Host "끌 때는 그 두 창을 각각 닫으면 됩니다 (postgres는 'docker compose stop postgres'로 별도 종료)."
