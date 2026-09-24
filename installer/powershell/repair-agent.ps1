[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$PublishDirectory,
    [string]$InstallDirectory = "$env:ProgramFiles\ErpOnecAgent",
    [string]$ServiceName = 'ErpOnecAgent'
)
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$publish = (Resolve-Path -LiteralPath $PublishDirectory).Path
$service = Get-Service -Name $ServiceName -ErrorAction Stop
$wasRunning = $service.Status -eq 'Running'
if ($wasRunning) { Stop-Service -Name $ServiceName; $service.WaitForStatus('Stopped',[TimeSpan]::FromSeconds(30)) }
Get-ChildItem -LiteralPath $publish -File | Where-Object Name -ne 'appsettings.json' | Copy-Item -Destination $InstallDirectory -Force
if ($wasRunning) { Start-Service -Name $ServiceName; (Get-Service -Name $ServiceName).WaitForStatus('Running',[TimeSpan]::FromSeconds(30)) }
Write-Host 'Binary repair completed; configuration and ProgramData were preserved.'

