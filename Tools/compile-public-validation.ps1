param(
    [string]$RepoRoot = (Split-Path -Path $PSScriptRoot -Parent),
    [string]$TargetMql5Root = '',
    [string]$MetaEditorPath = '',
    [switch]$SyncRepoToTarget,
    [string]$LogDirectory = ''
)

$ErrorActionPreference = 'Stop'

function Get-NormalizedPath {
    param([Parameter(Mandatory = $true)][string]$Path)

    return [System.IO.Path]::GetFullPath($Path)
}

function Sync-MqttTree {
    param(
        [Parameter(Mandatory = $true)][string]$SourceRoot,
        [Parameter(Mandatory = $true)][string]$DestinationRoot,
        [Parameter(Mandatory = $true)][string]$RelativePath
    )

    $sourcePath = Join-Path $SourceRoot $RelativePath
    $destinationPath = Join-Path $DestinationRoot $RelativePath

    if (-not (Test-Path -LiteralPath $sourcePath)) {
        throw "Source tree not found: $sourcePath"
    }

    New-Item -ItemType Directory -Path $destinationPath -Force | Out-Null

    $null = robocopy $sourcePath $destinationPath /E /XO /R:2 /W:1 /NFL /NDL /NJH /NJS /NP
    $exitCode = $LASTEXITCODE
    if ($exitCode -ge 8) {
        throw "robocopy failed for $RelativePath with exit code $exitCode"
    }
}

function Invoke-CompileWithRetry {
    param(
        [Parameter(Mandatory = $true)][string]$CompileScript,
        [Parameter(Mandatory = $true)][string]$SourcePath,
        [Parameter(Mandatory = $true)][string]$LogPath,
        [Parameter(Mandatory = $true)][string]$MetaEditorPath
    )

    $attempt = 1
    while ($attempt -le 2) {
        Write-Host "==> Compile attempt ${attempt}: $SourcePath"
        & $CompileScript -SourcePath $SourcePath -LogPath $LogPath -MetaEditorPath $MetaEditorPath
        $exitCode = $LASTEXITCODE

        if ($exitCode -eq 0) {
            return @{ Success = $true; Attempts = $attempt; RetriedOnFatal1031 = ($attempt -gt 1) }
        }

        $logText = ''
        if (Test-Path -LiteralPath $LogPath) {
            $logText = Get-Content -Path $LogPath -Encoding Unicode -ErrorAction SilentlyContinue | Out-String
        }

        $isFatal1031 = $logText -match 'fatal compiler error:\s*-1031'
        if ($isFatal1031 -and $attempt -eq 1) {
            Write-Warning "MetaEditor returned fatal compiler error -1031 for $SourcePath. Retrying once."
            $attempt++
            continue
        }

        return @{
            Success            = $false
            Attempts           = $attempt
            RetriedOnFatal1031 = ($attempt -gt 1)
            ExitCode           = $exitCode
            Fatal1031          = $isFatal1031
        }
    }
}

$RepoRoot = Get-NormalizedPath -Path $RepoRoot
$pathResolverScript = Join-Path $RepoRoot 'Scripts\MQTT\Tools\Resolve-MT5Paths.ps1'
if (-not (Test-Path -LiteralPath $pathResolverScript)) {
    throw "Path resolver not found: $pathResolverScript"
}
. $pathResolverScript

$MetaEditorPath = Resolve-MetaEditorPath -PreferredPath $MetaEditorPath

if ([string]::IsNullOrWhiteSpace($TargetMql5Root)) {
    $TargetMql5Root = $RepoRoot
}
$TargetMql5Root = Get-NormalizedPath -Path $TargetMql5Root

if ([string]::IsNullOrWhiteSpace($LogDirectory)) {
    $LogDirectory = Join-Path $RepoRoot 'Tools\generated\public-validation'
}
$LogDirectory = Get-NormalizedPath -Path $LogDirectory

$compileScript = Join-Path $RepoRoot 'Scripts\MQTT\Tools\compile-mql5.ps1'
if (-not (Test-Path -LiteralPath $compileScript)) {
    throw "Compile helper not found: $compileScript"
}

if (-not (Test-Path -LiteralPath $TargetMql5Root)) {
    throw "Target MQL5 root not found: $TargetMql5Root"
}

New-Item -ItemType Directory -Path $LogDirectory -Force | Out-Null

if ($SyncRepoToTarget) {
    Write-Host "Syncing MQTT trees into $TargetMql5Root"
    Sync-MqttTree -SourceRoot $RepoRoot -DestinationRoot $TargetMql5Root -RelativePath 'Include\MQTT'
    Sync-MqttTree -SourceRoot $RepoRoot -DestinationRoot $TargetMql5Root -RelativePath 'Scripts\MQTT'
    Sync-MqttTree -SourceRoot $RepoRoot -DestinationRoot $TargetMql5Root -RelativePath 'Experts\MQTT'
}

$targets = @(
    'Scripts\MQTT\Tests\Unit\Protocol\TEST_MQTT.mq5',
    'Scripts\MQTT\Tests\Unit\Session\TEST_MqttClient.mq5',
    'Scripts\MQTT\Tests\Unit\Transport\TEST_Transport.mq5',
    'Scripts\MQTT\Tests\Unit\Queue\TEST_PublishQueue.mq5',
    'Experts\MQTT\Harnesses\PublishQueueTestHarness.mq5'
)

$results = New-Object System.Collections.Generic.List[object]
$failed = $false

foreach ($relativePath in $targets) {
    $sourcePath = Join-Path $TargetMql5Root $relativePath
    if (-not (Test-Path -LiteralPath $sourcePath)) {
        throw "Compile target not found: $sourcePath"
    }

    $safeName = ($relativePath -replace '[\\/]', '-')
    $logPath = Join-Path $LogDirectory ($safeName + '.log')
    Remove-Item -Path $logPath -Force -ErrorAction SilentlyContinue

    $result = Invoke-CompileWithRetry -CompileScript $compileScript -SourcePath $sourcePath -LogPath $logPath -MetaEditorPath $MetaEditorPath
    $results.Add([pscustomobject]@{
            Target             = $relativePath
            Success            = $result.Success
            Attempts           = $result.Attempts
            RetriedOnFatal1031 = $result.RetriedOnFatal1031
            LogPath            = $logPath
        }) | Out-Null

    if (-not $result.Success) {
        $failed = $true
    }
}

Write-Host ''
Write-Host 'Public validation compile summary:'
$results | ForEach-Object {
    $status = if ($_.Success) { 'PASS' } else { 'FAIL' }
    $retrySuffix = if ($_.RetriedOnFatal1031) { ' (retried after -1031)' } else { '' }
    Write-Host (" - {0}: {1} in {2} attempt(s){3}" -f $_.Target, $status, $_.Attempts, $retrySuffix)
}

if ($failed) {
    exit 1
}

exit 0
