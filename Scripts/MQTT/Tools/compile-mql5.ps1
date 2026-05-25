param(
  [Parameter(Mandatory = $true)]
  [string]$SourcePath,

  [Parameter(Mandatory = $true)]
  [string]$LogPath,

  [string]$MetaEditorPath = '',

  [string]$WorkspaceMql5Root = '',

  [string]$TargetMql5Root = '',

  [switch]$SyncMqttTrees
)

. (Join-Path $PSScriptRoot 'Resolve-MT5Paths.ps1')

function Get-NormalizedPath {
  param([Parameter(Mandatory = $true)][string]$Path)

  return [System.IO.Path]::GetFullPath($Path)
}

function Find-Mql5RootFromPath {
  param([Parameter(Mandatory = $true)][string]$Path)

  $current = if (Test-Path -LiteralPath $Path -PathType Container) {
    Get-NormalizedPath -Path $Path
  }
  else {
    Split-Path -Path (Get-NormalizedPath -Path $Path) -Parent
  }

  while (-not [string]::IsNullOrWhiteSpace($current)) {
    $hasInclude = Test-Path -LiteralPath (Join-Path $current 'Include')
    $hasScripts = Test-Path -LiteralPath (Join-Path $current 'Scripts')
    $hasExperts = Test-Path -LiteralPath (Join-Path $current 'Experts')
    if ($hasInclude -and $hasScripts -and $hasExperts) {
      return $current
    }

    $parent = Split-Path -Path $current -Parent
    if ([string]::IsNullOrWhiteSpace($parent) -or $parent -eq $current) {
      break
    }
    $current = $parent
  }

  return ''
}

function Get-RelativePathFromRoot {
  param(
    [Parameter(Mandatory = $true)][string]$Root,
    [Parameter(Mandatory = $true)][string]$Path
  )

  Push-Location $Root
  try {
    return (Resolve-Path -LiteralPath $Path -Relative)
  }
  finally {
    Pop-Location
  }
}

function Sync-MqttTree {
  param(
    [Parameter(Mandatory = $true)][string]$SourceRoot,
    [Parameter(Mandatory = $true)][string]$DestinationRoot,
    [Parameter(Mandatory = $true)][string]$RelativePath
  )

  $sourcePath = Get-NormalizedPath -Path (Join-Path $SourceRoot $RelativePath)
  $destinationPath = Get-NormalizedPath -Path (Join-Path $DestinationRoot $RelativePath)

  if (-not (Test-Path -LiteralPath $sourcePath)) {
    throw "Source tree not found: $sourcePath"
  }

  if ([string]::Equals($sourcePath, $destinationPath, [System.StringComparison]::OrdinalIgnoreCase)) {
    return
  }

  New-Item -ItemType Directory -Path $destinationPath -Force | Out-Null

  #--- Strict sync: workspace files must win even if the target tree has newer timestamps.
  $null = robocopy $sourcePath $destinationPath /E /R:2 /W:1 /NFL /NDL /NJH /NJS /NP
  $exitCode = $LASTEXITCODE
  if ($exitCode -ge 8) {
    throw "robocopy failed for $RelativePath with exit code $exitCode"
  }
}

$MetaEditorPath = Resolve-MetaEditorPath -PreferredPath $MetaEditorPath

$SourcePath = Get-NormalizedPath -Path $SourcePath

if (-not (Test-Path -LiteralPath $SourcePath)) {
  Write-Error "Source file not found: $SourcePath"
  exit 1
}

$compileSourcePath = $SourcePath

if ($SyncMqttTrees) {
  if ([string]::IsNullOrWhiteSpace($WorkspaceMql5Root)) {
    $WorkspaceMql5Root = Find-Mql5RootFromPath -Path $SourcePath
  }

  if ([string]::IsNullOrWhiteSpace($WorkspaceMql5Root)) {
    Write-Error "Could not infer the workspace MQL5 root from source path: $SourcePath"
    exit 1
  }

  $WorkspaceMql5Root = Get-NormalizedPath -Path $WorkspaceMql5Root
  if (-not (Test-Path -LiteralPath $WorkspaceMql5Root)) {
    Write-Error "Workspace MQL5 root not found: $WorkspaceMql5Root"
    exit 1
  }

  if ([string]::IsNullOrWhiteSpace($TargetMql5Root)) {
    $TargetMql5Root = [Environment]::GetEnvironmentVariable('MQL5_ROOT')
  }
  if ([string]::IsNullOrWhiteSpace($TargetMql5Root)) {
    $TargetMql5Root = $WorkspaceMql5Root
  }

  $TargetMql5Root = Get-NormalizedPath -Path $TargetMql5Root
  if (-not (Test-Path -LiteralPath $TargetMql5Root)) {
    Write-Error "Target MQL5 root not found: $TargetMql5Root"
    exit 1
  }

  $relativeSourcePath = Get-RelativePathFromRoot -Root $WorkspaceMql5Root -Path $SourcePath
  if ([string]::IsNullOrWhiteSpace($relativeSourcePath) -or $relativeSourcePath -match '^\.\.' -or $relativeSourcePath -eq '.') {
    Write-Error "Source path '$SourcePath' is not inside workspace root '$WorkspaceMql5Root'"
    exit 1
  }

  if (-not [string]::Equals($WorkspaceMql5Root, $TargetMql5Root, [System.StringComparison]::OrdinalIgnoreCase)) {
    Write-Host "Syncing MQTT trees from $WorkspaceMql5Root to $TargetMql5Root"
    Sync-MqttTree -SourceRoot $WorkspaceMql5Root -DestinationRoot $TargetMql5Root -RelativePath 'Include\MQTT'
    Sync-MqttTree -SourceRoot $WorkspaceMql5Root -DestinationRoot $TargetMql5Root -RelativePath 'Scripts\MQTT'
    Sync-MqttTree -SourceRoot $WorkspaceMql5Root -DestinationRoot $TargetMql5Root -RelativePath 'Experts\MQTT'
  }

  $relativeSourcePath = $relativeSourcePath -replace '^[.][\\/]', ''
  $compileSourcePath = Join-Path $TargetMql5Root $relativeSourcePath

  if (-not (Test-Path -LiteralPath $compileSourcePath)) {
    Write-Error "Synced compile target not found: $compileSourcePath"
    exit 1
  }
}

Remove-Item -Path $LogPath -ErrorAction SilentlyContinue

$proc = Start-Process -FilePath $MetaEditorPath -ArgumentList @("/compile:$compileSourcePath", "/log:$LogPath") -PassThru -Wait

for ($i = 0; $i -lt 100 -and !(Test-Path -Path $LogPath); $i++) {
  Start-Sleep -Milliseconds 200
}

if (!(Test-Path -Path $LogPath)) {
  Write-Error "Compile log was not created: $LogPath"
  exit 1
}

$tail = @()
for ($i = 0; $i -lt 150; $i++) {
  $tail = Get-Content -Path $LogPath -Encoding Unicode -ErrorAction SilentlyContinue | Select-Object -Last 5
  if (($tail -join ' ') -match 'Result:') {
    break
  }
  Start-Sleep -Milliseconds 200
}

Get-Content -Path $LogPath -Encoding Unicode | Select-Object -Last 60

if (($tail -join ' ') -match 'Result:\s+0 errors') {
  exit 0
}

if ($proc.ExitCode -eq 0 -and (Test-Path -Path $LogPath)) {
  exit 0
}

exit 1
