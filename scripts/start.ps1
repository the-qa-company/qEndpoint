param(
    [switch]
    $Recompile,
    [switch]
    $Client
)

$JavaLoad = @("-Xmx6G")

$prevPwd = $PWD

try {
    $base = (Get-Item $PSScriptRoot).parent
    Set-Location ($base.Fullname)

    if ($Recompile) {
        mvn "clean" install "-DskipTests"
    }

    Write-Host "Fetching name/version" -ForegroundColor Blue
    $version = scripts/get_version.ps1 -Expression "project.version"
    $name = scripts/get_version.ps1 -Expression "project.artifactId"

    $jarFile = "hdt-qs-backend/target/$name-$version-exec.jar"


    Write-Host "Starting " -ForegroundColor Blue -NoNewline
    Write-Host "$name v$version" -ForegroundColor Cyan -NoNewline
    Write-Host " '$jarFile'..." -ForegroundColor Blue

    if ($Client) {
        java @JavaLoad -jar $jarFile --client
    }
    else {
        java @JavaLoad -jar $jarFile
    }
}
finally {
    $prevPwd | Set-Location
}

