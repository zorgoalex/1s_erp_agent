[CmdletBinding()]
param(
    [string]$InstallDirectory = "$env:ProgramFiles\ErpOnecAgent",
    [string]$DataDirectory = "$env:ProgramData\ErpOnecAgent",
    [string]$ServiceName = 'ErpOnecAgent',
    [switch]$RemoveData,
    [switch]$ConfirmDataRemoval
)
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$resolvedInstall = [IO.Path]::GetFullPath($InstallDirectory)
$programFilesRoot = [IO.Path]::GetFullPath($env:ProgramFiles).TrimEnd('\') + '\'
if (-not $resolvedInstall.StartsWith($programFilesRoot,[StringComparison]::OrdinalIgnoreCase)) { throw 'Refusing to remove an installation directory outside Program Files.' }
$service = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if ($service) {
    if ($service.Status -ne 'Stopped') { Stop-Service -Name $ServiceName -Force; $service.WaitForStatus('Stopped',[TimeSpan]::FromSeconds(30)) }
    & sc.exe delete $ServiceName | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Failed to unregister the service.' }
}
if (Test-Path -LiteralPath $resolvedInstall) { Remove-Item -LiteralPath $resolvedInstall -Recurse -Force }
if ($RemoveData) {
    if (-not $ConfirmDataRemoval) { throw 'Data deletion requires both -RemoveData and -ConfirmDataRemoval.' }
    $resolved = [IO.Path]::GetFullPath($DataDirectory)
    $programDataRoot = [IO.Path]::GetFullPath($env:ProgramData).TrimEnd('\') + '\'
    if (-not $resolved.StartsWith($programDataRoot,[StringComparison]::OrdinalIgnoreCase)) { throw 'Refusing to remove a data directory outside ProgramData.' }
    if (Test-Path -LiteralPath $resolved) { Remove-Item -LiteralPath $resolved -Recurse -Force }
    Write-Warning "Deleted persistent data at $resolved. It is not recoverable unless backed up."
} else { Write-Host "Service binaries removed. Persistent data preserved at $DataDirectory" }
