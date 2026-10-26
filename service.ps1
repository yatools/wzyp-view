param(
    [ValidateSet('start', 'stop', 'restart', 'status')]
    [string]$Action = 'status',
    [ValidateRange(1, 300)]
    [int]$StartupTimeout = 30,
    [switch]$OpenBrowser
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProjectDir = $PSScriptRoot
$DataDir = Join-Path $ProjectDir 'data'
$StateFile = Join-Path $DataDir 'service.json'
$StdoutLog = Join-Path $DataDir 'service.stdout.log'
$StderrLog = Join-Path $DataDir 'service.stderr.log'

function Get-ServiceState {
    if (-not (Test-Path -LiteralPath $StateFile)) { return $null }
    $state = Get-Content -LiteralPath $StateFile -Raw -Encoding UTF8 | ConvertFrom-Json
    if ($state.project -ne $ProjectDir -or [int]$state.pid -le 0) {
        throw "服务记录无效，请检查：$StateFile"
    }
    $serviceProcess = Get-Process -Id ([int]$state.pid) -ErrorAction SilentlyContinue
    if ($null -eq $serviceProcess -or
        $serviceProcess.StartTime.ToUniversalTime().Ticks.ToString() -ne $state.startTicks -or
        $serviceProcess.Path -ne $state.executable) {
        Remove-Item -LiteralPath $StateFile -Force
        return $null
    }
    return $state
}

function Test-ServiceHealth([string]$Url) {
    $response = $null
    try {
        $request = [System.Net.HttpWebRequest]::Create("$Url/api/health")
        $request.Proxy = $null
        $request.Timeout = 1000
        $request.ReadWriteTimeout = 1000
        $request.AllowAutoRedirect = $false
        $response = $request.GetResponse()
        if ([int]$response.StatusCode -ne 200) { return $false }
        $reader = New-Object System.IO.StreamReader($response.GetResponseStream())
        try { return (($reader.ReadToEnd() | ConvertFrom-Json).ok -eq $true) }
        finally { $reader.Dispose() }
    }
    catch { return $false }
    finally { if ($null -ne $response) { $response.Close() } }
}

function Open-ServicePage([string]$Url) {
    if ($OpenBrowser) {
        try { Start-Process -FilePath $Url | Out-Null }
        catch { Write-Warning "浏览器未能自动打开，请手动访问 $Url" }
    }
}

function Stop-ServiceProcess {
    $state = Get-ServiceState
    if ($null -eq $state) {
        Write-Host '服务未运行。'
        return
    }
    # 虚拟环境的 Python 启动器可能有子进程，必须一起结束，避免端口残留。
    # PID、创建时间和可执行文件均核对后才停止，避免 PID 被其他程序复用。
    # A browser child may exit while taskkill walks the tree. Windows PowerShell
    # treats native stderr as terminating under Stop, before we can verify the PID.
    $stopErrorPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        $killOutput = & (Join-Path $env:SystemRoot 'System32/taskkill.exe') /PID $state.pid /T /F 2>&1
        $killExitCode = $LASTEXITCODE
    }
    finally { $ErrorActionPreference = $stopErrorPreference }
    if ($killExitCode -ne 0 -and $null -ne (Get-ServiceState)) {
        throw "停止失败：$killOutput"
    }
    $deadline = [DateTime]::UtcNow.AddSeconds(10)
    while ($null -ne (Get-ServiceState)) {
        if ([DateTime]::UtcNow -ge $deadline) { throw '停止超时，请稍后重新执行 stop。' }
        Start-Sleep -Milliseconds 200
    }
    Write-Host '服务已停止，数据和配置已保留。'
}

function Find-ProjectPython {
    $venvPython = Join-Path $ProjectDir '.venv\Scripts\python.exe'
    if (Test-Path -LiteralPath $venvPython) { return $venvPython }
    foreach ($name in @('python', 'py')) {
        $command = Get-Command $name -ErrorAction SilentlyContinue
        if ($null -eq $command) { continue }
        $resolved = & $command.Source -c 'import sys; print(sys.executable)'
        if ($LASTEXITCODE -eq 0 -and $resolved -and (Test-Path -LiteralPath "$resolved")) {
            return "$resolved"
        }
    }
    throw '未找到 Python。请先按照 README 创建 .venv 并安装 requirements.txt。'
}

function Start-ServiceProcess {
    $state = Get-ServiceState
    if ($null -ne $state) {
        if (-not (Test-ServiceHealth $state.url)) {
            throw "服务进程存在但健康检查失败。请查看 $StderrLog，或执行 restart。"
        }
        Write-Host "服务已在运行：$($state.url)（PID $($state.pid)）"
        Open-ServicePage $state.url
        return
    }

    $bindHost = '127.0.0.1'
    if ($env:WZYP_HOST) { $bindHost = $env:WZYP_HOST }
    $port = 8770
    if ($env:WZYP_PORT) {
        if (-not [int]::TryParse($env:WZYP_PORT, [ref]$port)) { throw 'WZYP_PORT 必须是整数。' }
    }
    if ($port -lt 1 -or $port -gt 65535) { throw 'WZYP_PORT 必须在 1 到 65535 之间。' }
    $listeners = [System.Net.NetworkInformation.IPGlobalProperties]::GetIPGlobalProperties().GetActiveTcpListeners()
    if ($listeners | Where-Object { $_.Port -eq $port }) {
        throw "端口 $port 已被占用。请关闭原服务，或设置 WZYP_PORT 后重试。"
    }

    $python = Find-ProjectPython
    & $python -c 'import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)'
    if ($LASTEXITCODE -ne 0) { throw '需要 Python 3.11 或更新版本。' }
    & $python -c 'import uvicorn, fastapi, requests, httpx, bs4'
    if ($LASTEXITCODE -ne 0) {
        throw "依赖不完整，请先运行：powershell -ExecutionPolicy Bypass -File `"$ProjectDir\setup.ps1`""
    }

    $urlHost = $bindHost
    if ($bindHost -eq '0.0.0.0') { $urlHost = '127.0.0.1' }
    if ($bindHost -eq '::') { $urlHost = '::1' }
    if ($urlHost.Contains(':')) { $urlHost = "[$urlHost]" }
    $url = 'http://{0}:{1}' -f $urlHost, $port
    $serviceProcess = $null
    try {
        $serviceProcess = Start-Process -FilePath $python -WorkingDirectory $ProjectDir `
            -ArgumentList @('-X', 'utf8', '-u', ('"{0}"' -f (Join-Path $ProjectDir 'run.py'))) `
            -WindowStyle Hidden -RedirectStandardOutput $StdoutLog -RedirectStandardError $StderrLog -PassThru
        $state = [ordered]@{
            project = $ProjectDir
            pid = $serviceProcess.Id
            startTicks = $serviceProcess.StartTime.ToUniversalTime().Ticks.ToString()
            executable = $serviceProcess.Path
            url = $url
        }
        $state | ConvertTo-Json | Set-Content -LiteralPath $StateFile -Encoding UTF8
        $deadline = [DateTime]::UtcNow.AddSeconds($StartupTimeout)
        while ([DateTime]::UtcNow -lt $deadline) {
            if ($null -eq (Get-ServiceState)) { throw '服务进程提前退出。' }
            if (Test-ServiceHealth $url) {
                Write-Host "服务已启动：$url（PID $($state.pid)）"
                Write-Host "日志：$StderrLog"
                Open-ServicePage $url
                return
            }
            Start-Sleep -Milliseconds 300
        }
        throw "启动超时（$StartupTimeout 秒）。"
    }
    catch {
        $startError = $_.Exception.Message
        if ($null -ne $serviceProcess) {
            if (Test-Path -LiteralPath $StateFile) {
                Stop-ServiceProcess
            }
            elseif (-not $serviceProcess.HasExited) {
                & (Join-Path $env:SystemRoot 'System32/taskkill.exe') /PID $serviceProcess.Id /T /F | Out-Null
            }
        }
        throw "$startError 请查看日志：$StderrLog 和 $StdoutLog"
    }
}

$lock = $null
try {
    New-Item -ItemType Directory -Path $DataDir -Force | Out-Null
    try {
        $lock = [System.IO.File]::Open((Join-Path $DataDir 'service.lock'),
            [System.IO.FileMode]::OpenOrCreate, [System.IO.FileAccess]::ReadWrite, [System.IO.FileShare]::None)
    }
    catch { throw '另一个启停操作正在执行，请稍后重试。' }
    switch ($Action) {
        'start' { Start-ServiceProcess }
        'stop' { Stop-ServiceProcess }
        'restart' { Stop-ServiceProcess; Start-ServiceProcess }
        'status' {
            $state = Get-ServiceState
            if ($null -eq $state) { Write-Host '服务未运行。'; exit 1 }
            if (-not (Test-ServiceHealth $state.url)) {
                throw "服务进程存在但健康检查失败，请查看：$StderrLog"
            }
            Write-Host "服务运行正常：$($state.url)（PID $($state.pid)）"
        }
    }
    exit 0
}
catch {
    [Console]::Error.WriteLine($_.Exception.Message)
    exit 1
}
finally { if ($null -ne $lock) { $lock.Dispose() } }
