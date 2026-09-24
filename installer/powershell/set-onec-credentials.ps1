[CmdletBinding()]
param([string]$InstallDirectory = "$env:ProgramFiles\ErpOnecAgent")
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$exe = Join-Path $InstallDirectory 'ErpOnecAgent.exe'
if (-not (Test-Path -LiteralPath $exe -PathType Leaf)) { throw "ErpOnecAgent.exe was not found in $InstallDirectory" }
Push-Location $InstallDirectory
try {
    & $exe --store-onec-credential
    if ($LASTEXITCODE -ne 0) { throw 'Credential storage failed.' }
} finally { Pop-Location }
