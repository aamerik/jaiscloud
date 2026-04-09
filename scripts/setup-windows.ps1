<#
Setup script for Windows (PowerShell)
Run as Administrator:

    Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser -Force
    .\setup-windows.ps1

#>
Write-Host "LocalCloud Windows setup — beginning..."

function Has-Command($name) {
    $null -ne (Get-Command $name -ErrorAction SilentlyContinue)
}

if (Has-Command winget) {
    Write-Host "Using winget to install packages..."
    winget install --id Git.Git -e --source winget || Write-Host "git install skipped"
    winget install --id OpenJS.NodeJS -e --source winget || Write-Host "node install skipped"
    winget install --id Golang.Go -e --source winget || Write-Host "go install skipped"
    winget install --id Docker.DockerDesktop -e --source winget || Write-Host "docker install skipped"
    winget install --id Amazon.AWSCLIV2 -e --source winget || Write-Host "awscli install skipped"
    winget install --id jq -e --source winget || Write-Host "jq install skipped"
    winget install --id Microsoft.VisualStudioCode -e --source winget || Write-Host "vscode install skipped"
} else {
    Write-Host "winget not found — please install Git/Go/Node/Docker/AWS CLI manually or install winget from Microsoft Store." -ForegroundColor Yellow
}

# Configure AWS local profile
Write-Host "Configuring AWS localcloud profile..."
aws configure set aws_access_key_id test --profile localcloud
aws configure set aws_secret_access_key test --profile localcloud
aws configure set region us-east-1 --profile localcloud

# Add awslocal function to PowerShell profile
$profilePath = $PROFILE
if (-not (Test-Path -Path $profilePath)) {
    New-Item -ItemType File -Path $profilePath -Force | Out-Null
}

$functionDef = @'
function awslocal { aws --endpoint-url=http://localhost:4566 --profile localcloud @args }
'@

if (-not (Select-String -Path $profilePath -Pattern "function awslocal" -Quiet)) {
    Add-Content -Path $profilePath -Value $functionDef
    Write-Host "Added awslocal helper to $profilePath"
} else {
    Write-Host "awslocal helper already present in $profilePath"
}

Write-Host "Windows setup complete. Restart PowerShell to load profile changes."
