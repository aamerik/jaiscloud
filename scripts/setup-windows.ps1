<#
Setup script for Windows (PowerShell)
Run as Administrator:

    Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser -Force
    .\setup-windows.ps1
#>

Write-Host "LocalCloud Windows setup - beginning..."

function Has-Command {
    param($name)
    $null -ne (Get-Command $name -ErrorAction SilentlyContinue)
}

function Install-WingetPackage {
    param($id, $label)
    winget install --id $id -e --source winget
    if ($LASTEXITCODE -ne 0) { Write-Host "$label install skipped" }
}

if (Has-Command winget) {
    Write-Host "Using winget to install packages..."
    Install-WingetPackage "Git.Git" "git"
    Install-WingetPackage "OpenJS.NodeJS" "node"
    Install-WingetPackage "Golang.Go" "go"
    Install-WingetPackage "Docker.DockerDesktop" "docker"
    Install-WingetPackage "Amazon.AWSCLIV2" "awscli"
    Install-WingetPackage "jqlang.jq" "jq"
    Install-WingetPackage "Microsoft.VisualStudioCode" "vscode"
} else {
    Write-Host "winget not found - please install packages manually." -ForegroundColor Yellow
}

# Refresh PATH so newly installed tools (go, aws, node, etc.) are available in this session
Write-Host "Refreshing PATH..."
$env:Path = [System.Environment]::GetEnvironmentVariable("Path", "Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path", "User")

# Fallback: common install locations in case they are not yet in PATH
$extraPaths = @(
    "C:\Program Files\Go\bin",
    "C:\Program Files\nodejs",
    "C:\Program Files\Git\cmd",
    "C:\Program Files\Amazon\AWSCLIV2"
)
foreach ($p in $extraPaths) {
    if ((Test-Path $p) -and ($env:Path -notlike "*$p*")) {
        $env:Path += ";$p"
    }
}

if (Has-Command aws) {
    Write-Host "Configuring AWS localcloud profile..."
    aws configure set aws_access_key_id test --profile localcloud
    aws configure set aws_secret_access_key test --profile localcloud
    aws configure set region us-east-1 --profile localcloud
} 
else {
    Write-Host "AWS CLI not found in PATH - skipping profile config. Re-run after restarting PowerShell." -ForegroundColor Yellow
}

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