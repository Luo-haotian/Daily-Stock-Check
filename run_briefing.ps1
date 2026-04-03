$ErrorActionPreference = "Stop"
$projectDir = Split-Path -Parent $MyInvocation.MyCommand.Path
& "$projectDir\scripts\run_portfolio_briefing.ps1"

