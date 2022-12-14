param(
    $Expression = "project.version"
)


$prevPwd = $PWD

try {
    $base = (Get-Item $PSScriptRoot).parent
    Set-Location "$($base.Fullname)/hdt-qs-backend"

    mvn help:evaluate "-Dexpression=$Expression" "-q" "-DforceStdout"
} finally {
  $prevPwd | Set-Location
}
