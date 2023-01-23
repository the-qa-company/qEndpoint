[CmdletBinding()]
param (
)

$prevPwd = $PWD

try {
    $base = (Get-Item $PSScriptRoot).parent
    Set-Location "$($base.Fullname)"

    mvn help:evaluate "-Dexpression=project.version" -q -DforceStdout
} finally {
  $prevPwd | Set-Location
}
