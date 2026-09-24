[CmdletBinding()]
param([Parameter(Mandatory)][string]$InputDirectory,[Parameter(Mandatory)][string]$OutputPath)
$root = (Resolve-Path -LiteralPath $InputDirectory).Path
$lines = Get-ChildItem -LiteralPath $root -File -Recurse | Sort-Object FullName | ForEach-Object {
    $relative = [IO.Path]::GetRelativePath($root,$_.FullName).Replace('\','/')
    '{0}  {1}' -f (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant(),$relative
}
[IO.File]::WriteAllLines([IO.Path]::GetFullPath($OutputPath),$lines,[Text.UTF8Encoding]::new($false))

