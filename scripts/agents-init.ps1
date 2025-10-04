Param(
  [string]$RemoteName = "agent-instructions",
  [string]$RemoteUrl = "git@github.com:tristanbrown/agent-instructions.git",
  [string]$PrefixDir = ".agents",
  [string]$Branch = "main"
)

$ErrorActionPreference = 'Stop'

git remote add $RemoteName $RemoteUrl
git fetch $RemoteName
git subtree add --prefix=$PrefixDir $RemoteName $Branch --squash

Write-Host "Agents subtree initialized at $PrefixDir from $RemoteName/$Branch."
