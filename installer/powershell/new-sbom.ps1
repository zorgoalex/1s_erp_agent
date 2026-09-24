[CmdletBinding()]
param([Parameter(Mandatory)][string]$RepositoryRoot,[Parameter(Mandatory)][string]$OutputPath)
$components = @{}
Get-ChildItem -LiteralPath $RepositoryRoot -Filter 'packages.lock.json' -Recurse | ForEach-Object {
    $lock = Get-Content -LiteralPath $_.FullName -Raw | ConvertFrom-Json -AsHashtable
    foreach ($framework in $lock.dependencies.Values) {
        foreach ($entry in $framework.GetEnumerator()) {
            if ($entry.Value.type -eq 'Project') { continue }
            $version = [string]$entry.Value.resolved
            $key = "$($entry.Key)/$version"
            $components[$key] = [ordered]@{ type='library'; name=$entry.Key; version=$version; purl="pkg:nuget/$([Uri]::EscapeDataString($entry.Key))@$version" }
        }
    }
}
$sbom = [ordered]@{
    bomFormat='CycloneDX'; specVersion='1.6'; serialNumber="urn:uuid:$([guid]::NewGuid())"; version=1
    metadata=[ordered]@{ timestamp=[DateTimeOffset]::UtcNow.ToString('O'); component=[ordered]@{ type='application'; name='ErpOnecAgent'; version='1.0.0' } }
    components=@($components.Values | Sort-Object name,version)
}
$json = $sbom | ConvertTo-Json -Depth 10
[IO.File]::WriteAllText([IO.Path]::GetFullPath($OutputPath),$json,[Text.UTF8Encoding]::new($false))
