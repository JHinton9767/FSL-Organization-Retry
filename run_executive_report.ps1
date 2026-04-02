param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ArgsList
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$runner = Join-Path $repoRoot "run_executive_report.py"

if (-not (Test-Path $runner)) {
    throw "Runner not found at $runner"
}

$commands = @(
    @{ Name = "py"; Args = @($runner) + $ArgsList },
    @{ Name = "python"; Args = @($runner) + $ArgsList }
)

foreach ($command in $commands) {
    $available = Get-Command $command.Name -ErrorAction SilentlyContinue
    if ($available) {
        & $command.Name @($command.Args)
        exit $LASTEXITCODE
    }
}

throw "Neither 'py' nor 'python' is available on PATH. Install Python or run the script with a full python.exe path."
