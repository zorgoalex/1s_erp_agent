[CmdletBinding()]
param([string]$SigningCertificateThumbprint)
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$root = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..')).Path
$localSdk = Join-Path $root '.dotnet\dotnet.exe'
$dotnet = if (Test-Path -LiteralPath $localSdk) { $localSdk } else { (Get-Command dotnet -ErrorAction Stop).Source }
$artifacts = Join-Path $root 'artifacts'
$publish = Join-Path $artifacts 'publish\win-x64'
$expectedPublish = [IO.Path]::GetFullPath((Join-Path $root 'artifacts\publish\win-x64'))
if ([IO.Path]::GetFullPath($publish) -ne $expectedPublish) { throw 'Unsafe publish directory.' }
if (Test-Path -LiteralPath $publish) { Remove-Item -LiteralPath $publish -Recurse -Force }
& $dotnet restore (Join-Path $root 'ErpOnecAgent.sln') --locked-mode
if ($LASTEXITCODE -ne 0) { throw 'Restore failed.' }
& $dotnet test (Join-Path $root 'ErpOnecAgent.sln') -c Release --no-restore
if ($LASTEXITCODE -ne 0) { throw 'Tests failed.' }
& $dotnet publish (Join-Path $root 'src\ErpOnecAgent.Service\ErpOnecAgent.Service.csproj') -c Release -r win-x64 --self-contained true --no-restore -o $publish
if ($LASTEXITCODE -ne 0) { throw 'Publish failed.' }
& (Join-Path $PSScriptRoot 'new-sbom.ps1') -RepositoryRoot $root -OutputPath (Join-Path $artifacts 'sbom.cdx.json')
if ($SigningCertificateThumbprint) {
    $signtool = (Get-Command signtool.exe -ErrorAction Stop).Source
    Get-ChildItem -LiteralPath $publish -File | Where-Object Extension -in '.exe','.dll' | ForEach-Object { & $signtool sign /sha1 $SigningCertificateThumbprint /fd SHA256 /td SHA256 /tr 'http://timestamp.digicert.com' $_.FullName; if ($LASTEXITCODE -ne 0) { throw "Signing failed: $($_.Name)" } }
} else { Write-Warning 'Artifacts are unsigned and are not a production release. Supply -SigningCertificateThumbprint in the owner-controlled build environment.' }
& (Join-Path $PSScriptRoot 'new-manifest.ps1') -InputDirectory $publish -OutputPath (Join-Path $artifacts 'SHA256SUMS.txt')
