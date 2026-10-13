[CmdletBinding()]
param(
    [string]$CoreRepository = "",
    [string]$PythonCommand = "python",
    [ValidateSet("portable", "managed")]
    [string]$WorkspacePlacement = "portable",
    [string]$PostgresImage = "postgres:17",
    [switch]$KeepDatabase,
    [switch]$KeepFiles
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$workspaceName = "smoke_counties"
$streamName = "county_events"
$tableName = "county_events"
$datasetId = "$workspaceName.$streamName"
$sourceUrlMarker = "SMOKE_SOURCE_URL"
$environmentVariableNames = @(
    "METRKA_ENV",
    "METRKA_WORKSPACES_CONFIG_PATH",
    "METRKA_METADATA_DSN",
    "METRKA_MIGRATION_DSN",
    "METRKA_OPERATIONS_DSN",
    "METRKA_METADATA_CONFIG_PATH",
    "METRKA_SILVER_ENGINE_POLICY",
    "METRKA_MIGRATION_OWNER_ROLE",
    "METRKA_MIGRATION_TEST_DSN",
    "PYTHONPATH"
)
$originalEnvironment = @{}

foreach ($name in $environmentVariableNames) {
    $originalEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, "Process")
}

$utf8WithoutBom = New-Object System.Text.UTF8Encoding($false)
$script:containerName = "metrka-installed-e2e-" + [Guid]::NewGuid().ToString("N").Substring(0, 8)
$databaseStarted = $false
$sourceServer = $null
$smokeRoot = $null
$succeeded = $false

function Write-Step {
    param([string]$Message)

    Write-Host ""
    Write-Host "==> $Message" -ForegroundColor Cyan
}

function Invoke-Checked {
    param(
        [string]$FilePath,
        [string[]]$ArgumentList,
        [string]$WorkingDirectory = ""
    )

    $pushedLocation = $false

    try {
        if ($WorkingDirectory) {
            Push-Location -LiteralPath $WorkingDirectory
            $pushedLocation = $true
        }

        & $FilePath @ArgumentList
        $exitCode = $LASTEXITCODE
    }
    finally {
        if ($pushedLocation) {
            Pop-Location
        }
    }

    if ($exitCode -ne 0) {
        throw "Command failed with exit code ${exitCode}: $FilePath $($ArgumentList -join ' ')"
    }
}

function Get-GitOutput {
    param(
        [string]$Repository,
        [string[]]$ArgumentList
    )

    $output = & git -C $Repository @ArgumentList

    if ($LASTEXITCODE -ne 0) {
        throw "Git command failed in ${Repository}: git $($ArgumentList -join ' ')"
    }

    return @($output)
}

function Assert-CleanMain {
    param(
        [string]$Repository,
        [string]$Label
    )

    $branch = ((Get-GitOutput -Repository $Repository -ArgumentList @("branch", "--show-current")) -join "").Trim()

    if ($branch -ne "main") {
        throw "$Label must be on main. Current branch: $branch"
    }

    $status = @(Get-GitOutput -Repository $Repository -ArgumentList @(
        "status",
        "--porcelain",
        "--untracked-files=normal"
    ))

    if ($status.Count -gt 0) {
        throw "$Label must be clean before the smoke test. Git status: $($status -join '; ')"
    }
}

function Get-FreeTcpPort {
    # Ask the operating system for a free loopback port; it skips reserved ranges.
    $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, 0)
    $listener.Start()

    try {
        return $listener.LocalEndpoint.Port
    }
    finally {
        $listener.Stop()
    }
}

function New-Secret {
    return [Guid]::NewGuid().ToString("N")
}

function Write-Utf8File {
    param(
        [string]$Path,
        [string]$Content
    )

    [IO.File]::WriteAllText($Path, $Content, $utf8WithoutBom)
}

function Invoke-Psql {
    param([string]$Sql)

    $output = @(
        & docker exec $script:containerName psql `
            --username postgres `
            --dbname metrka `
            --tuples-only `
            --no-align `
            --quiet `
            --set ON_ERROR_STOP=1 `
            --command $Sql
    )

    if ($LASTEXITCODE -ne 0) {
        throw "PostgreSQL command failed: $Sql"
    }

    return $output
}

function Invoke-PsqlScalar {
    param([string]$Sql)

    $values = @(Invoke-Psql -Sql $Sql | ForEach-Object { $_.Trim() } | Where-Object { $_ })

    if ($values.Count -ne 1) {
        throw "Expected one scalar value from PostgreSQL, received $($values.Count): $Sql"
    }

    return $values[0]
}

function Assert-DatabaseValue {
    param(
        [string]$Sql,
        [string]$Expected,
        [string]$Description
    )

    $actual = Invoke-PsqlScalar -Sql $Sql

    if ($actual -ne $Expected) {
        throw "$Description. Expected $Expected, received $actual."
    }

    Write-Host "Verified: $Description = $actual"
}

function Assert-FileExists {
    param(
        [string]$Path,
        [string]$Description
    )

    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "$Description was not created: $Path"
    }

    Write-Host "Verified: $Description"
}

function Assert-DefinitionRootClean {
    param([string]$DefinitionRoot)

    foreach ($unexpectedDirectory in @("data", "logs")) {
        $unexpectedPath = Join-Path $DefinitionRoot $unexpectedDirectory
        if (Test-Path -LiteralPath $unexpectedPath) {
            throw "Managed execution wrote runtime state below definition_root: $unexpectedPath"
        }
    }

    $status = @(Get-GitOutput -Repository $DefinitionRoot -ArgumentList @(
        "status",
        "--porcelain",
        "--untracked-files=normal"
    ))

    if ($status.Count -gt 0) {
        throw "Managed execution changed definition_root. Git status: $($status -join '; ')"
    }

    Write-Host "Verified: managed definition_root remained read-only"
}

try {
    if ([string]::IsNullOrWhiteSpace($CoreRepository)) {
        $CoreRepository = Join-Path $PSScriptRoot "..\.."
    }

    $resolvedCoreRepository = (Resolve-Path -LiteralPath $CoreRepository).Path
    $fixtureRoot = Join-Path $PSScriptRoot "fixture"
    $fixtureWorkspace = Join-Path $fixtureRoot $workspaceName
    $fixtureSource = Join-Path $fixtureRoot "source"
    $wheelAuditScript = Join-Path $PSScriptRoot "audit_wheel.py"
    $importProbeScript = Join-Path $PSScriptRoot "verify_installed_import.py"
    $versionProbeScript = Join-Path $PSScriptRoot "verify_installed_version.py"
    $roundTripProbeScript = Join-Path $PSScriptRoot "verify_workspace_roundtrip.py"

    foreach ($requiredPath in @(
        (Join-Path $fixtureWorkspace "conf\main.yaml"),
        (Join-Path $fixtureWorkspace "conf\contract.yaml"),
        (Join-Path $fixtureWorkspace "conf\quality.yaml"),
        (Join-Path $fixtureSource "$streamName.csv"),
        $wheelAuditScript,
        $importProbeScript,
        $versionProbeScript,
        $roundTripProbeScript,
        (Join-Path $resolvedCoreRepository "pyproject.toml"),
        (Join-Path $resolvedCoreRepository "src\metrka_core\build_provenance.py")
    )) {
        if (-not (Test-Path -LiteralPath $requiredPath -PathType Leaf)) {
            throw "Required smoke-test input is missing: $requiredPath"
        }
    }

    foreach ($commandName in @("git", "docker", $PythonCommand)) {
        if (-not (Get-Command $commandName -ErrorAction SilentlyContinue)) {
            throw "Required command is not available: $commandName"
        }
    }

    $bootstrapPython = (Get-Command $PythonCommand).Source

    Write-Step "Checking the repository and prerequisites"
    Assert-CleanMain -Repository $resolvedCoreRepository -Label "metrka-core"
    Invoke-Checked -FilePath $bootstrapPython -ArgumentList @(
        "-c",
        "import sys; assert sys.version_info >= (3, 12), sys.version"
    )
    Invoke-Checked -FilePath "docker" -ArgumentList @("version")

    $coreCommit = ((Get-GitOutput -Repository $resolvedCoreRepository -ArgumentList @(
        "rev-parse",
        "HEAD"
    )) -join "").Trim()
    Write-Host "Core commit: $coreCommit"

    $smokeRoot = Join-Path $env:TEMP (
        "metrka-installed-e2e-" + [Guid]::NewGuid().ToString("N")
    )
    $buildEnvironment = Join-Path $smokeRoot "build-venv"
    $installedEnvironment = Join-Path $smokeRoot "installed-venv"
    $wheelDirectory = Join-Path $smokeRoot "wheel"
    $sourceDirectory = Join-Path $smokeRoot "source"
    $definitionRoot = Join-Path $smokeRoot "definitions\$workspaceName"
    $workspacesConfigPath = Join-Path $smokeRoot "workspaces.yaml"
    $workspaceExportPath = Join-Path $smokeRoot "$workspaceName-customer-workspace.zip"
    $roundTripRoot = Join-Path $smokeRoot "customer-workspace-roundtrip"
    $roundTripWorkspaceRoot = Join-Path $roundTripRoot $workspaceName
    $roundTripConfigPath = Join-Path $smokeRoot "roundtrip-workspaces.yaml"

    New-Item -ItemType Directory -Path $wheelDirectory -Force | Out-Null

    Write-Step "Building a provenance-bearing wheel from clean main"
    Invoke-Checked -FilePath $bootstrapPython -ArgumentList @("-m", "venv", $buildEnvironment)
    $buildPython = Join-Path $buildEnvironment "Scripts\python.exe"
    Invoke-Checked -FilePath $buildPython -ArgumentList @(
        "-m",
        "pip",
        "install",
        "--disable-pip-version-check",
        "--upgrade",
        "pip",
        "build"
    )

    [Environment]::SetEnvironmentVariable(
        "PYTHONPATH",
        (Join-Path $resolvedCoreRepository "src"),
        "Process"
    )
    Invoke-Checked -FilePath $buildPython -ArgumentList @(
        "-m",
        "metrka_core.build_provenance",
        "--branch",
        "main"
    ) -WorkingDirectory $resolvedCoreRepository
    [Environment]::SetEnvironmentVariable("PYTHONPATH", $null, "Process")

    # Build the sdist and then the wheel from it, as RELEASING.md does. A wheel built
    # straight from the checkout also packs stale files left in its ignored build/ folder.
    Invoke-Checked -FilePath $buildPython -ArgumentList @(
        "-m",
        "build",
        "--outdir",
        $wheelDirectory,
        $resolvedCoreRepository
    ) -WorkingDirectory $resolvedCoreRepository

    $wheels = @(Get-ChildItem -LiteralPath $wheelDirectory -Filter "*.whl" -File)

    if ($wheels.Count -ne 1) {
        throw "Expected exactly one wheel, found $($wheels.Count) in $wheelDirectory"
    }

    $wheel = $wheels[0]
    Write-Host "Built wheel: $($wheel.FullName)"

    Invoke-Checked -FilePath $buildPython -ArgumentList @(
        $wheelAuditScript,
        $wheel.FullName
    )

    Write-Step "Installing the wheel into a clean virtual environment"
    Invoke-Checked -FilePath $bootstrapPython -ArgumentList @("-m", "venv", $installedEnvironment)
    $smokePython = Join-Path $installedEnvironment "Scripts\python.exe"
    $smokeMetrka = Join-Path $installedEnvironment "Scripts\metrka.exe"
    Invoke-Checked -FilePath $smokePython -ArgumentList @(
        "-m",
        "pip",
        "install",
        "--disable-pip-version-check",
        "--upgrade",
        "pip"
    )
    Invoke-Checked -FilePath $smokePython -ArgumentList @(
        "-m",
        "pip",
        "install",
        "--disable-pip-version-check",
        $wheel.FullName
    )
    Invoke-Checked -FilePath $smokePython -ArgumentList @("-m", "pip", "check")

    if (-not (Test-Path -LiteralPath $smokeMetrka -PathType Leaf)) {
        throw "Installed metrka command was not created: $smokeMetrka"
    }

    Invoke-Checked -FilePath $smokePython -ArgumentList @($importProbeScript) `
        -WorkingDirectory $smokeRoot
    Invoke-Checked -FilePath $smokePython -ArgumentList @(
        $versionProbeScript,
        $smokeMetrka,
        $smokeRoot
    ) -WorkingDirectory $smokeRoot
    Invoke-Checked -FilePath $smokeMetrka -ArgumentList @("--help") `
        -WorkingDirectory $smokeRoot

    Write-Step "Preparing the smoke workspace and its local source"
    $sourcePort = Get-FreeTcpPort
    $sourceUrl = "http://127.0.0.1:$sourcePort/$streamName.csv"
    Copy-Item -LiteralPath $fixtureSource -Destination $sourceDirectory -Recurse
    New-Item -ItemType Directory -Path (Split-Path -Parent $definitionRoot) -Force | Out-Null
    Copy-Item -LiteralPath $fixtureWorkspace -Destination $definitionRoot -Recurse

    # The fixture's main.yaml names the source by a marker; the address is known only now.
    $mainPath = Join-Path $definitionRoot "conf\main.yaml"
    $mainText = [IO.File]::ReadAllText($mainPath)

    if (-not $mainText.Contains($sourceUrlMarker)) {
        throw "The fixture main.yaml has no $sourceUrlMarker marker: $mainPath"
    }

    Write-Utf8File -Path $mainPath -Content $mainText.Replace($sourceUrlMarker, $sourceUrl)

    # Every run records the Git revision of its workspace, so the copy becomes a repository.
    Invoke-Checked -FilePath "git" -ArgumentList @("-C", $definitionRoot, "init", "--quiet")
    Invoke-Checked -FilePath "git" -ArgumentList @("-C", $definitionRoot, "add", "--all")
    Invoke-Checked -FilePath "git" -ArgumentList @(
        "-C",
        $definitionRoot,
        "-c",
        "user.name=Metrka installed smoke",
        "-c",
        "user.email=installed-smoke@metrka.invalid",
        "commit",
        "--quiet",
        "--message",
        "Smoke workspace"
    )

    $definitionRootYaml = $definitionRoot.Replace("\", "/")

    if ($WorkspacePlacement -eq "managed") {
        $dataRoot = Join-Path $smokeRoot "managed-data\$workspaceName"
        New-Item -ItemType Directory -Path $dataRoot -Force | Out-Null
        $dataRootYaml = $dataRoot.Replace("\", "/")
        Write-Utf8File -Path $workspacesConfigPath -Content @"
schema_version: 1
workspaces:
  ${workspaceName}:
    placement: managed
    definition_root: "$definitionRootYaml"
    data_root: "$dataRootYaml"
"@
    }
    else {
        $dataRoot = Join-Path $definitionRoot "data"
        Write-Utf8File -Path $workspacesConfigPath -Content @"
schema_version: 1
workspaces:
  ${workspaceName}:
    placement: portable
    workspace_root: "$definitionRootYaml"
"@
    }

    $sourceServer = Start-Process -FilePath $smokePython `
        -ArgumentList "-m http.server $sourcePort --bind 127.0.0.1" `
        -WorkingDirectory $sourceDirectory `
        -WindowStyle Hidden `
        -RedirectStandardOutput (Join-Path $smokeRoot "source-server.out.log") `
        -RedirectStandardError (Join-Path $smokeRoot "source-server.err.log") `
        -PassThru

    $sourceReady = $false

    for ($attempt = 0; $attempt -lt 30 -and -not $sourceReady; $attempt++) {
        try {
            Invoke-WebRequest -Uri $sourceUrl -UseBasicParsing -TimeoutSec 2 | Out-Null
            $sourceReady = $true
        }
        catch {
            Start-Sleep -Milliseconds 500
        }
    }

    if (-not $sourceReady) {
        throw "The local source server did not answer at $sourceUrl"
    }

    Write-Host "Source:          $sourceUrl"
    Write-Host "Definition root: $definitionRoot"
    Write-Host "Data root:       $dataRoot"

    Write-Step "Starting a disposable PostgreSQL ($PostgresImage)"
    $databasePort = Get-FreeTcpPort
    Invoke-Checked -FilePath "docker" -ArgumentList @(
        "run",
        "--detach",
        "--name",
        $script:containerName,
        "--env",
        "POSTGRES_PASSWORD=$(New-Secret)",
        "--env",
        "POSTGRES_DB=metrka",
        "--publish",
        "127.0.0.1:${databasePort}:5432",
        $PostgresImage
    )
    $databaseStarted = $true

    # During initialization the server listens on its socket only; TCP answers once it is final.
    $databaseReady = $false

    for ($attempt = 0; $attempt -lt 60 -and -not $databaseReady; $attempt++) {
        & docker exec $script:containerName pg_isready --host 127.0.0.1 --username postgres --dbname metrka | Out-Null
        if ($LASTEXITCODE -eq 0) {
            $databaseReady = $true
        }
        else {
            Start-Sleep -Seconds 1
        }
    }

    if (-not $databaseReady) {
        throw "PostgreSQL did not become ready in container $($script:containerName)"
    }

    Write-Host "Database: container $($script:containerName) on 127.0.0.1:$databasePort"

    Write-Step "Provisioning roles with the script shipped in the installed wheel"
    $rolesScript = (& $smokePython -c (
        "import pathlib, metrka_core; " +
        "path = pathlib.Path(metrka_core.__file__).parent / 'metadata' / 'migrations' / " +
        "'bootstrap' / 'provision_metadata_roles.sql'; " +
        "assert path.is_file(), path; print(path)"
    ))

    if ($LASTEXITCODE -ne 0) {
        throw "The installed wheel does not ship provision_metadata_roles.sql"
    }

    $rolesScript = ($rolesScript -join "").Trim()
    Invoke-Checked -FilePath "docker" -ArgumentList @(
        "cp",
        $rolesScript,
        "$($script:containerName):/tmp/provision_metadata_roles.sql"
    )
    Invoke-Checked -FilePath "docker" -ArgumentList @(
        "exec",
        $script:containerName,
        "psql",
        "--username",
        "postgres",
        "--dbname",
        "metrka",
        "--quiet",
        "--file",
        "/tmp/provision_metadata_roles.sql"
    )

    $passwords = @{}

    foreach ($role in @("metrka_migrator", "metrka_etl", "metrka_operator", "metrka_web")) {
        $passwords[$role] = New-Secret
        Invoke-Psql -Sql "ALTER ROLE $role PASSWORD '$($passwords[$role])';" | Out-Null
    }

    $databaseAddress = "127.0.0.1:$databasePort/metrka"
    $env:METRKA_ENV = "development"
    $env:METRKA_WORKSPACES_CONFIG_PATH = $workspacesConfigPath
    $env:METRKA_MIGRATION_DSN = "postgresql://metrka_migrator:$($passwords['metrka_migrator'])@$databaseAddress"
    $env:METRKA_METADATA_DSN = "postgresql://metrka_etl:$($passwords['metrka_etl'])@$databaseAddress"
    $env:METRKA_OPERATIONS_DSN = "postgresql://metrka_operator:$($passwords['metrka_operator'])@$databaseAddress"
    $env:METRKA_SILVER_ENGINE_POLICY = "allow_candidate"
    $env:METRKA_MIGRATION_OWNER_ROLE = "metrka_owner"
    Remove-Item Env:METRKA_METADATA_CONFIG_PATH -ErrorAction SilentlyContinue
    Remove-Item Env:METRKA_MIGRATION_TEST_DSN -ErrorAction SilentlyContinue
    Remove-Item Env:PYTHONPATH -ErrorAction SilentlyContinue

    Write-Host "Environment:         $env:METRKA_ENV"
    Write-Host "Workspace placement: $WorkspacePlacement"
    Write-Host "Workspaces config:   $env:METRKA_WORKSPACES_CONFIG_PATH"
    Write-Host "Engine policy:       $env:METRKA_SILVER_ENGINE_POLICY"

    Write-Step "Applying and checking PostgreSQL migrations from the installed wheel"
    Invoke-Checked -FilePath $smokePython -ArgumentList @(
        "-m",
        "metrka_core.metadata.migrations",
        "upgrade",
        "head"
    ) -WorkingDirectory $smokeRoot
    Invoke-Checked -FilePath $smokePython -ArgumentList @(
        "-m",
        "metrka_core.metadata.migrations",
        "check"
    ) -WorkingDirectory $smokeRoot

    Write-Step "Validating the smoke workspace without PostgreSQL execution"
    Invoke-Checked -FilePath $smokeMetrka -ArgumentList @(
        "workspace",
        "validate",
        $workspaceName,
        "--workspaces-config-path",
        $workspacesConfigPath
    ) -WorkingDirectory $smokeRoot

    Write-Step "Running the smoke workspace for the first time"
    Invoke-Checked -FilePath $smokeMetrka -ArgumentList @(
        "run",
        $workspaceName,
        "--workspaces-config-path",
        $workspacesConfigPath
    ) -WorkingDirectory $smokeRoot

    Assert-DatabaseValue `
        -Sql "SELECT count(*) FROM logs.pipeline_runs WHERE workspace_name = '$workspaceName' AND status = 'success';" `
        -Expected "1" `
        -Description "successful pipeline runs after first execution"
    Assert-DatabaseValue `
        -Sql "SELECT count(*) FROM meta.marshaled_files WHERE dataset_id = '$datasetId';" `
        -Expected "1" `
        -Description "registered Bronze source files after first execution"
    Assert-DatabaseValue `
        -Sql "SELECT count(*) FROM logs.silver_build_attempts WHERE dataset_id = '$datasetId' AND status = 'succeeded';" `
        -Expected "1" `
        -Description "successful Silver builds after first execution"
    Assert-DatabaseValue `
        -Sql "SELECT count(*) FROM catalog.dataset_publication_candidates WHERE dataset_id = '$datasetId' AND status = 'awaiting_approval';" `
        -Expected "1" `
        -Description "publication candidates awaiting approval"
    Assert-DatabaseValue `
        -Sql "SELECT count(*) FROM catalog.dataset_publications WHERE dataset_id = '$datasetId';" `
        -Expected "0" `
        -Description "publications before operator approval"

    $bronzeFiles = @(
        Get-ChildItem -LiteralPath (Join-Path $dataRoot "files\bronze\runs") `
            -Recurse -File -ErrorAction Stop
    )
    $silverCsvFiles = @(
        Get-ChildItem -LiteralPath (Join-Path $dataRoot "files\silver\tables") `
            -Recurse -Filter "*.csv" -File -ErrorAction Stop
    )
    $silverParquetFiles = @(
        Get-ChildItem -LiteralPath (Join-Path $dataRoot "files\silver\tables") `
            -Recurse -Filter "*.parquet" -File -ErrorAction Stop
    )
    $manifestFiles = @(
        Get-ChildItem -LiteralPath (Join-Path $dataRoot "files\silver\manifests") `
            -Recurse -Filter "*.json" -File -ErrorAction Stop
    )

    if ($bronzeFiles.Count -lt 1) {
        throw "The first run produced no Bronze files."
    }

    if ($silverCsvFiles.Count -ne 1 -or $silverParquetFiles.Count -ne 1) {
        throw "The first run did not produce one CSV and one Parquet Silver file."
    }

    if ($manifestFiles.Count -lt 1) {
        throw "The first run produced no Silver manifest."
    }

    Write-Host "Verified: Bronze files = $($bronzeFiles.Count)"
    Write-Host "Verified: Silver CSV files = $($silverCsvFiles.Count)"
    Write-Host "Verified: Silver Parquet files = $($silverParquetFiles.Count)"
    Write-Host "Verified: Silver manifests = $($manifestFiles.Count)"

    # The table profile renames Count and reconciles the Statewide row: 2024 leaves
    # 25 - 22 = 3 unallocated, 2025 matches exactly.
    $silverLines = @(
        [IO.File]::ReadAllLines($silverCsvFiles[0].FullName) | Where-Object { $_ }
    )
    $expectedHeader = "geography_name,event_count,reporting_year"

    if ($silverLines[0].Trim([char]0xFEFF) -ne $expectedHeader) {
        throw "Silver CSV header is '$($silverLines[0])', expected '$expectedHeader'."
    }

    if (($silverLines.Count - 1) -ne 7) {
        throw "Silver CSV has $($silverLines.Count - 1) rows, expected 6 counties and 1 Unallocated row."
    }

    if (@($silverLines | Where-Object { $_ -eq "Unallocated,3,2024" }).Count -ne 1) {
        throw "Silver CSV lacks the reconciled row 'Unallocated,3,2024'."
    }

    if (@($silverLines | Where-Object { $_.StartsWith("Statewide,") }).Count -ne 0) {
        throw "Silver CSV still contains the Statewide total row."
    }

    Write-Host "Verified: Silver table renamed by its profile and reconciled"

    $snapshots = @(
        Get-ChildItem -LiteralPath (Join-Path $dataRoot "contracts\$datasetId") `
            -Recurse -Filter "contract.yaml" -File -ErrorAction Stop
    )

    if ($snapshots.Count -ne 1) {
        throw "Expected one contract snapshot, found $($snapshots.Count)."
    }

    $snapshotText = [IO.File]::ReadAllText($snapshots[0].FullName)

    foreach ($expected in @("profile: county_events", "profile: smoke_source", "glossary:", "event:")) {
        if (-not $snapshotText.Contains($expected)) {
            throw "Contract snapshot lacks '$expected': $($snapshots[0].FullName)"
        }
    }

    Write-Host "Verified: contract snapshot keeps the expanded profiles and the glossary"

    Write-Step "Approving and publishing through the installed operations CLI"
    $candidateId = Invoke-PsqlScalar -Sql (
        "SELECT candidate_id FROM catalog.dataset_publication_candidates " +
        "WHERE dataset_id = '$datasetId' AND status = 'awaiting_approval' " +
        "ORDER BY requested_at DESC LIMIT 1;"
    )
    Invoke-Checked -FilePath $smokeMetrka -ArgumentList @(
        "operations",
        "publication-candidates",
        "approve",
        $candidateId,
        "--approved-by",
        "installed-e2e-smoke"
    ) -WorkingDirectory $smokeRoot
    Invoke-Checked -FilePath $smokeMetrka -ArgumentList @(
        "operations",
        "publication-candidates",
        "publish",
        $candidateId,
        "--workspaces-config-path",
        $workspacesConfigPath
    ) -WorkingDirectory $smokeRoot

    Assert-DatabaseValue `
        -Sql "SELECT count(*) FROM catalog.dataset_publication_candidates WHERE candidate_id = '$candidateId' AND status = 'published';" `
        -Expected "1" `
        -Description "published candidate rows"
    Assert-DatabaseValue `
        -Sql "SELECT count(*) FROM catalog.dataset_publications WHERE dataset_id = '$datasetId' AND is_current IS TRUE;" `
        -Expected "1" `
        -Description "current smoke publications"

    $publicationId = Invoke-PsqlScalar -Sql (
        "SELECT publication_id FROM catalog.dataset_publications " +
        "WHERE dataset_id = '$datasetId' AND is_current IS TRUE;"
    )
    $pointerPath = Join-Path $dataRoot (
        "current\latest\silver\dataset--$datasetId.json"
    )
    $latestViewPath = Join-Path $dataRoot (
        "files\silver\views\$tableName\publication=$publicationId\latest.sql"
    )
    $historyViewPath = Join-Path $dataRoot (
        "files\silver\views\$tableName\history.sql"
    )
    Assert-FileExists -Path $pointerPath -Description "current Silver pointer"
    Assert-FileExists -Path $latestViewPath -Description "publication-versioned latest view"
    Assert-FileExists -Path $historyViewPath -Description "Silver history view"

    $pointer = Get-Content -LiteralPath $pointerPath -Raw | ConvertFrom-Json

    if ($pointer.publication_id -ne $publicationId) {
        throw (
            "Current pointer publication mismatch. Expected $publicationId, " +
            "received $($pointer.publication_id)."
        )
    }

    Invoke-Checked -FilePath $smokeMetrka -ArgumentList @(
        "operations",
        "reconcile-publications",
        "--dataset-id",
        $datasetId,
        "--workspaces-config-path",
        $workspacesConfigPath
    ) -WorkingDirectory $smokeRoot

    Write-Step "Running the smoke workspace a second time to prove idempotency"
    Invoke-Checked -FilePath $smokeMetrka -ArgumentList @(
        "run",
        $workspaceName,
        "--workspaces-config-path",
        $workspacesConfigPath
    ) -WorkingDirectory $smokeRoot

    Assert-DatabaseValue `
        -Sql "SELECT count(*) FROM logs.pipeline_runs WHERE workspace_name = '$workspaceName' AND status = 'success';" `
        -Expected "2" `
        -Description "successful pipeline runs after repeated execution"
    Assert-DatabaseValue `
        -Sql "SELECT count(*) FROM logs.pipeline_runs WHERE workspace_name = '$workspaceName' AND status <> 'success';" `
        -Expected "0" `
        -Description "non-successful pipeline runs"
    Assert-DatabaseValue `
        -Sql "SELECT count(*) FROM meta.marshaled_files WHERE dataset_id = '$datasetId';" `
        -Expected "1" `
        -Description "Bronze source records after repeated execution"
    Assert-DatabaseValue `
        -Sql "SELECT count(*) FROM logs.silver_build_attempts WHERE dataset_id = '$datasetId';" `
        -Expected "1" `
        -Description "Silver build attempts after repeated execution"
    Assert-DatabaseValue `
        -Sql "SELECT count(*) FROM catalog.dataset_publication_candidates WHERE dataset_id = '$datasetId';" `
        -Expected "1" `
        -Description "publication candidates after repeated execution"
    Assert-DatabaseValue `
        -Sql "SELECT count(*) FROM catalog.dataset_publications WHERE dataset_id = '$datasetId';" `
        -Expected "1" `
        -Description "publications after repeated execution"

    if ($WorkspacePlacement -eq "managed") {
        Assert-DefinitionRootClean -DefinitionRoot $definitionRoot
    }

    Write-Step "Exporting and verifying a customer workspace package"
    Invoke-Checked -FilePath $smokeMetrka -ArgumentList @(
        "workspace",
        "export",
        $workspaceName,
        "--output",
        $workspaceExportPath,
        "--workspaces-config-path",
        $workspacesConfigPath
    ) -WorkingDirectory $smokeRoot
    Assert-FileExists -Path $workspaceExportPath -Description "customer workspace export"
    Invoke-Checked -FilePath $smokeMetrka -ArgumentList @(
        "workspace",
        "verify-export",
        $workspaceExportPath
    ) -WorkingDirectory $smokeRoot

    Write-Step "Reopening the customer export as an independent portable workspace"
    Invoke-Checked -FilePath $smokeMetrka -ArgumentList @(
        "workspace",
        "import",
        $workspaceExportPath,
        "--destination-directory",
        $roundTripRoot,
        "--workspaces-config-path",
        $roundTripConfigPath
    ) -WorkingDirectory $smokeRoot
    Assert-FileExists `
        -Path (Join-Path $roundTripWorkspaceRoot "metrka-workspace-manifest.json") `
        -Description "extracted customer workspace manifest"

    # The reopened workspace validates only if the export carried its template,
    # profiles and glossary along with conf.
    Invoke-Checked -FilePath $smokeMetrka -ArgumentList @(
        "workspace",
        "validate",
        $workspaceName,
        "--workspaces-config-path",
        $roundTripConfigPath
    ) -WorkingDirectory $smokeRoot
    $roundTripProbeArguments = @(
        $roundTripProbeScript,
        $roundTripWorkspaceRoot,
        "--workspace-name",
        $workspaceName,
        "--dataset-id",
        $datasetId
    )
    foreach ($sourceRoot in @($definitionRoot, $dataRoot)) {
        $roundTripProbeArguments += @("--forbidden-source-root", $sourceRoot)
    }
    Invoke-Checked -FilePath $smokePython -ArgumentList $roundTripProbeArguments `
        -WorkingDirectory $smokeRoot

    if ($WorkspacePlacement -eq "managed") {
        Assert-DefinitionRootClean -DefinitionRoot $definitionRoot
    }

    Write-Step "Installed end-to-end smoke passed (placement=$WorkspacePlacement)"
    Write-Host "Wheel:           $($wheel.FullName)"
    Write-Host "Publication:     $publicationId"
    Write-Host "Placement:       $WorkspacePlacement"
    Write-Host "Core commit:     $coreCommit"
    $succeeded = $true
}
finally {
    if ($null -ne $sourceServer -and -not $sourceServer.HasExited) {
        Stop-Process -Id $sourceServer.Id -Force -ErrorAction SilentlyContinue
    }

    if ($databaseStarted) {
        if ($KeepDatabase) {
            Write-Host "Database kept for inspection. Remove it with:"
            Write-Host "  docker rm -f $($script:containerName)"
        }
        else {
            & docker rm --force $script:containerName | Out-Null
        }
    }

    foreach ($name in $environmentVariableNames) {
        [Environment]::SetEnvironmentVariable($name, $originalEnvironment[$name], "Process")
    }

    if ($null -ne $smokeRoot -and (Test-Path -LiteralPath $smokeRoot)) {
        if ($succeeded -and -not $KeepFiles) {
            Remove-Item -LiteralPath $smokeRoot -Recurse -Force -ErrorAction SilentlyContinue
        }
        else {
            Write-Host "Smoke files kept for inspection: $smokeRoot"
        }
    }
}
