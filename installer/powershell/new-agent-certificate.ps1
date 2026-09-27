[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$AgentId,
    [string]$OutputDirectory = (Get-Location).Path,
    [string]$ServiceName = 'ErpOnecAgent',
    [int]$ValidityYears = 2,
    [switch]$GrantServiceAccess
)
# Creates the agent's mTLS client certificate: self-signed, RSA 3072, client-authentication EKU,
# private key NON-exportable in LocalMachine\My. Writes only the PUBLIC certificate (PEM) and its
# SHA-1 thumbprint for ERP registration; the private key never leaves the machine.
# Requires an elevated PowerShell. Set Erp:ClientCertificateThumbprint to the printed thumbprint.
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$principal = [Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()
if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) { throw 'Run this script in an elevated PowerShell (LocalMachine certificate store).' }
if ($AgentId -notmatch '^[A-Za-z0-9._-]{1,64}$') { throw 'AgentId must be 1..64 characters: letters, digits, dot, underscore, hyphen.' }

$certificate = New-SelfSignedCertificate `
    -Subject "CN=erp-onec-agent $AgentId" `
    -CertStoreLocation 'Cert:\LocalMachine\My' `
    -KeyAlgorithm RSA -KeyLength 3072 -HashAlgorithm SHA256 `
    -KeyExportPolicy NonExportable `
    -KeyUsage DigitalSignature, KeyEncipherment `
    -TextExtension @('2.5.29.37={text}1.3.6.1.5.5.7.3.2') `
    -NotAfter (Get-Date).AddYears($ValidityYears)

if ($GrantServiceAccess) {
    # The service runs as the virtual account NT SERVICE\<ServiceName>; it needs read access to the private key.
    $key = [System.Security.Cryptography.X509Certificates.RSACertificateExtensions]::GetRSAPrivateKey($certificate)
    $keyFile = $key.Key.UniqueName
    $keyPath = Join-Path "$env:ProgramData\Microsoft\Crypto\Keys" $keyFile
    if (-not (Test-Path -LiteralPath $keyPath)) { $keyPath = Join-Path "$env:ProgramData\Microsoft\Crypto\RSA\MachineKeys" $keyFile }
    & icacls.exe $keyPath /grant "NT SERVICE\${ServiceName}:R" | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "Granting key access to NT SERVICE\$ServiceName failed." }
}

New-Item -ItemType Directory -Force -Path $OutputDirectory | Out-Null
$pemPath = Join-Path $OutputDirectory "agent-$AgentId.pem"
$base64 = [Convert]::ToBase64String($certificate.RawData, [Base64FormattingOptions]::InsertLineBreaks)
Set-Content -LiteralPath $pemPath -Value "-----BEGIN CERTIFICATE-----`n$base64`n-----END CERTIFICATE-----" -Encoding ascii

$sha256 = [BitConverter]::ToString([Security.Cryptography.SHA256]::Create().ComputeHash($certificate.RawData)).Replace('-', '')
Write-Output "Certificate created in LocalMachine\My (private key non-exportable)."
Write-Output "Subject:           $($certificate.Subject)"
Write-Output "Valid until (UTC): $($certificate.NotAfter.ToUniversalTime().ToString('o'))"
Write-Output "Thumbprint (SHA-1, for Erp:ClientCertificateThumbprint): $($certificate.Thumbprint)"
Write-Output "SHA-256 fingerprint: $sha256"
Write-Output "Public certificate (send to ERP): $pemPath"
