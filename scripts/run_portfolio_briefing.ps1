$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectDir = Split-Path -Parent $scriptDir

python "$scriptDir\futu_read_positions.py" --project-dir "$projectDir"
if ($LASTEXITCODE -ne 0) {
  throw "futu_read_positions failed with exit code $LASTEXITCODE"
}

python "$scriptDir\generate_portfolio_report.py" --project-dir "$projectDir"
if ($LASTEXITCODE -ne 0) {
  throw "generate_portfolio_report failed with exit code $LASTEXITCODE"
}

Write-Output "[OK] Portfolio briefing completed."

