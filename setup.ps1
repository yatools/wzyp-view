param()
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$TaskVenv = Join-Path $PSScriptRoot '.venv'
$TaskPython = Join-Path $TaskVenv 'Scripts\python.exe'
$TaskRequirements = Join-Path $PSScriptRoot 'requirements.txt'
$TaskUv = Get-Command uv -ErrorAction SilentlyContinue
if (-not (Test-Path -LiteralPath $TaskPython)) {
    if ($null -ne $TaskUv) {
        & $TaskUv.Source venv $TaskVenv --python 3.13
    } else {
        & python -m venv $TaskVenv
    }
    if ($LASTEXITCODE -ne 0) { throw '创建运行环境失败，请先安装 Python 3.11 或更新版本。' }
}
if ($null -ne $TaskUv) {
    & $TaskUv.Source pip install --python $TaskPython -r $TaskRequirements
} else {
    & $TaskPython -m ensurepip
    if ($LASTEXITCODE -ne 0) { throw '安装依赖工具失败。' }
    & $TaskPython -m pip install -r $TaskRequirements
}
if ($LASTEXITCODE -ne 0) { throw '依赖安装失败，请检查网络后重试。' }
& $TaskPython -m playwright install chromium
if ($LASTEXITCODE -ne 0) { throw '浏览器组件安装失败，请检查网络后重新运行 setup.ps1。已有数据保留。' }
Write-Host '安装完成。运行 start.bat 即可启动，数据和配置均已保留。'
