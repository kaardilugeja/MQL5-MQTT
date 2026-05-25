function Get-Mt5ExecutableCandidates {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ExecutableName,
        [Parameter(Mandatory = $true)]
        [string[]]$FallbackPaths
    )

    $candidateList = New-Object System.Collections.Generic.List[string]
    $roots = @('C:\Program Files', 'C:\Program Files (x86)')

    foreach ($root in $roots) {
        if (-not (Test-Path -LiteralPath $root)) {
            continue
        }

        $installDirs = Get-ChildItem -LiteralPath $root -Directory -ErrorAction SilentlyContinue |
        Where-Object { $_.Name -like 'MetaTrader 5*' } |
        Sort-Object Name

        foreach ($installDir in $installDirs) {
            $candidatePath = Join-Path $installDir.FullName $ExecutableName
            if (-not $candidateList.Contains($candidatePath)) {
                $candidateList.Add($candidatePath)
            }
        }
    }

    if ($candidateList.Count -eq 0) {
        foreach ($fallbackPath in $FallbackPaths) {
            if (-not $candidateList.Contains($fallbackPath)) {
                $candidateList.Add($fallbackPath)
            }
        }
    }

    return $candidateList.ToArray()
}

function Resolve-Mt5ToolPath {
    param(
        [string]$PreferredPath,
        [Parameter(Mandatory = $true)]
        [string]$ParameterName,
        [Parameter(Mandatory = $true)]
        [string]$EnvironmentVariableName,
        [Parameter(Mandatory = $true)]
        [string]$ToolName,
        [Parameter(Mandatory = $true)]
        [string[]]$CandidatePaths
    )

    if (-not [string]::IsNullOrWhiteSpace($PreferredPath)) {
        $resolvedPreferredPath = [System.IO.Path]::GetFullPath($PreferredPath)
        if (-not (Test-Path -LiteralPath $resolvedPreferredPath)) {
            throw "$ToolName not found at explicit -$ParameterName path: $resolvedPreferredPath"
        }
        return $resolvedPreferredPath
    }

    $envValue = [Environment]::GetEnvironmentVariable($EnvironmentVariableName)
    if (-not [string]::IsNullOrWhiteSpace($envValue)) {
        $resolvedEnvPath = [System.IO.Path]::GetFullPath($envValue)
        if (-not (Test-Path -LiteralPath $resolvedEnvPath)) {
            throw "$ToolName not found at `$env:$EnvironmentVariableName path: $resolvedEnvPath"
        }
        return $resolvedEnvPath
    }

    foreach ($candidatePath in $CandidatePaths) {
        if (Test-Path -LiteralPath $candidatePath) {
            return $candidatePath
        }
    }

    $candidateList = ($CandidatePaths | ForEach-Object { " - $_" }) -join [Environment]::NewLine
    throw "$ToolName not found. Pass -$ParameterName, set `$env:$EnvironmentVariableName, or install MetaTrader 5 in one of these common locations:$([Environment]::NewLine)$candidateList"
}

function Resolve-MetaEditorPath {
    param([string]$PreferredPath = '')

    $fallbackPaths = @(
        'C:\Program Files\MetaTrader 5\MetaEditor64.exe'
        'C:\Program Files (x86)\MetaTrader 5\MetaEditor64.exe'
    )
    $candidates = Get-Mt5ExecutableCandidates -ExecutableName 'MetaEditor64.exe' -FallbackPaths $fallbackPaths

    return Resolve-Mt5ToolPath -PreferredPath $PreferredPath -ParameterName 'MetaEditorPath' `
        -EnvironmentVariableName 'METAEDITOR_PATH' -ToolName 'MetaEditor' -CandidatePaths $candidates
}

function Resolve-Mt5TerminalPath {
    param([string]$PreferredPath = '')

    $fallbackPaths = @(
        'C:\Program Files\MetaTrader 5\terminal64.exe'
        'C:\Program Files (x86)\MetaTrader 5\terminal64.exe'
    )
    $candidates = Get-Mt5ExecutableCandidates -ExecutableName 'terminal64.exe' -FallbackPaths $fallbackPaths

    return Resolve-Mt5ToolPath -PreferredPath $PreferredPath -ParameterName 'TerminalPath' `
        -EnvironmentVariableName 'MT5_TERMINAL_PATH' -ToolName 'MetaTrader terminal' -CandidatePaths $candidates
}
