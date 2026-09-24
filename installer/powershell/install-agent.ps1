[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$PublishDirectory,
    [Parameter(Mandatory)][string]$ConfigPath,
    [string]$InstallDirectory = "$env:ProgramFiles\ErpOnecAgent",
    [string]$DataDirectory = "$env:ProgramData\ErpOnecAgent",
    [string]$ServiceName = "ErpOnecAgent",
    [switch]$SkipCredentialPrompt,
    [switch]$SkipStart
)
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$identity = [Security.Principal.WindowsIdentity]::GetCurrent()
$principal = [Security.Principal.WindowsPrincipal]::new($identity)
if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) { throw 'Run this script from an elevated PowerShell session.' }
if (-not [Environment]::Is64BitOperatingSystem) { throw 'ErpOnecAgent supports Windows x64 only.' }
$publish = (Resolve-Path -LiteralPath $PublishDirectory).Path
$config = (Resolve-Path -LiteralPath $ConfigPath).Path
$sourceExe = Join-Path $publish 'ErpOnecAgent.exe'
if (-not (Test-Path -LiteralPath $sourceExe -PathType Leaf)) { throw "ErpOnecAgent.exe was not found in $publish" }

New-Item -ItemType Directory -Force -Path $InstallDirectory,$DataDirectory | Out-Null
foreach ($child in 'data','data\backups','spool\creating','spool\ready','spool\acknowledged','spool\quarantine','logs','diagnostics','config','updates','secrets') {
    New-Item -ItemType Directory -Force -Path (Join-Path $DataDirectory $child) | Out-Null
}

$existing = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if ($existing -and $existing.Status -ne 'Stopped') { Stop-Service -Name $ServiceName -Force; $existing.WaitForStatus('Stopped',[TimeSpan]::FromSeconds(30)) }
Get-ChildItem -LiteralPath $publish -Force | Copy-Item -Destination $InstallDirectory -Recurse -Force
$targetConfig = Join-Path $InstallDirectory 'appsettings.json'
# The publish output contains the safe template. The explicitly supplied,
# operator-reviewed configuration must replace it on both install and upgrade.
Copy-Item -LiteralPath $config -Destination $targetConfig -Force

$exe = Join-Path $InstallDirectory 'ErpOnecAgent.exe'
if (-not $existing) {
    & sc.exe create $ServiceName binPath= ('"' + $exe + '"') start= delayed-auto obj= "NT SERVICE\$ServiceName" | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Failed to register the Windows service.' }
}
& sc.exe config $ServiceName start= delayed-auto obj= "NT SERVICE\$ServiceName" | Out-Null
& sc.exe failure $ServiceName reset= 86400 actions= restart/60000/restart/60000/restart/60000 | Out-Null
& sc.exe failureflag $ServiceName 1 | Out-Null
& sc.exe sidtype $ServiceName restricted | Out-Null

& icacls.exe $DataDirectory /inheritance:r /grant:r "SYSTEM:(OI)(CI)F" "BUILTIN\Administrators:(OI)(CI)F" "NT SERVICE\${ServiceName}:(OI)(CI)M" | Out-Null
& icacls.exe $InstallDirectory /inheritance:r /grant:r "SYSTEM:(OI)(CI)F" "BUILTIN\Administrators:(OI)(CI)F" "NT SERVICE\${ServiceName}:(OI)(CI)RX" | Out-Null

Push-Location $InstallDirectory
try {
    & $exe --migrate
    if ($LASTEXITCODE -ne 0) { throw 'SQLite migration failed.' }
    if (-not $SkipCredentialPrompt) { & $exe --store-onec-credential; if ($LASTEXITCODE -ne 0) { throw 'Credential storage failed.' } }
    & $exe --validate-config
    if ($LASTEXITCODE -ne 0) { throw 'Configuration validation failed.' }
} finally { Pop-Location }

if (-not $SkipStart) {
    Start-Service -Name $ServiceName
    (Get-Service -Name $ServiceName).WaitForStatus('Running',[TimeSpan]::FromSeconds(30))
}
Write-Host "Installed $ServiceName. Data is stored in $DataDirectory"
