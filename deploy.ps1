# JobsBot AWS deployment script
# Prerequisites: AWS CLI + SAM CLI
# Install SAM CLI: https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (-not (Get-Command aws -ErrorAction SilentlyContinue)) {
    Write-Error "AWS CLI not found. Install from: https://aws.amazon.com/cli/"
    exit 1
}

if (-not (Get-Command sam -ErrorAction SilentlyContinue)) {
    Write-Error "SAM CLI not found. Install from: https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html"
    exit 1
}

Write-Host "Building Lambda package..." -ForegroundColor Cyan
sam build
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "Deploying to AWS..." -ForegroundColor Cyan
Write-Host "(First run uses --guided to create samconfig.toml. Subsequent runs just use: sam deploy)" -ForegroundColor Yellow

if (Test-Path "samconfig.toml") {
    sam deploy
} else {
    sam deploy --guided
}
