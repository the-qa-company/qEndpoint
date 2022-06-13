param(
    $PackageFile,
    $Version,
    $EndpointJar
)

Write-Host $PSScriptRoot

$buildDir = "build"
$inputDir = "$buildDir/input"
if (Test-Path "$PSScriptRoot/$buildDir") {
    Remove-Item -Recurse -Force "$PSScriptRoot/$buildDir"
}
New-Item -ItemType Directory "$PSScriptRoot/$inputDir" > $null
$packageFileFinal = "build/jpackage_final.cfg"
$LicenseFile = "../LICENSE.md"

Write-Host "Read config file"
$ConfigData = Get-Content $PackageFile
Write-Host "Copy endpoint"
$execJarName = "target-$Version.jar"
Copy-Item $EndpointJar "$PSScriptRoot/$inputDir/$execJarName" > $null

Push-Location
Set-Location $PSScriptRoot

Write-Host "Create jpackage config file"

$LicenseFileBld = "$inputDir/LICENSE"

Copy-Item $LicenseFile $LicenseFileBld > $null

"
$(Get-Content jpackage.cfg)
$ConfigData
--main-jar $execJarName
--app-version $VERSION
" > "$packageFileFinal"

Write-Host "JPackage creation"

jpackage "@$packageFileFinal"


Pop-Location