[CmdletBinding()]
param([string]$InstallDirectory = "$env:ProgramFiles\ErpOnecAgent")
$ErrorActionPreference = 'Stop'
$exe = Join-Path $InstallDirectory 'ErpOnecAgent.exe'
if (-not (Test-Path -LiteralPath $exe -PathType Leaf)) { throw 'ErpOnecAgent is not installed.' }
Push-Location $InstallDirectory
try { & $exe --collect-diagnostics; exit $LASTEXITCODE } finally { Pop-Location }

