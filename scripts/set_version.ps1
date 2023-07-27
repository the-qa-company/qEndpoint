[CmdletBinding()]
param(
    $Version,
    [switch]
    $Edit,
    [switch]
    $NoReleaseUpdate
)

$prevPwd = $PWD

try {
    $base = (Get-Item $PSScriptRoot).Parent
    Set-Location "$($base.Fullname)"

    if (!$Edit -and $null -eq $Version) {
        Write-Host "set_version.ps1 -version `"x.y.z`""
        Write-Host "set_version.ps1 -edit"
        Write-Host "-version x.y.z    Set the version to x.y.z, if this is already"
        Write-Host "                  the current version, it will edit the file."
        Write-Host "-edit             Edit the release file"
        return
    }

    if ($Edit) {
        Write-Host "edit RELEASE file"
        vim "release/RELEASE.md"
        return
    }

    $OldVersion=$(scripts/get_version.ps1)
    Write-Host "old version: $OldVersion"

    if ($OldVersion -eq $Version) {
        Write-Host "the new version is the same as the old version, edit RELEASE file"
        vim release/RELEASE.md
        return
    }

    
    Remove-Item pom.xml_backupsv -ErrorAction Ignore > $null
    Copy-Item pom.xml pom.xml_backupsv

    Write-Host "set new version..."

    mvn versions:set versions:commit -DnewVersion="$VERSION" -q

    $NewVersion=$(scripts/get_version.ps1)

    Write-Host "new version: $NewVersion"

    if (!$NoReleaseUpdate) {
        New-Item -Type File release/RELEASE.md_old -ErrorAction Ignore > $null
        Remove-Item release/RELEASE.md_old_backupsv -ErrorAction Ignore > $null
        Move-Item release/RELEASE.md_old release/RELEASE.md_old_backupsv

        # Write new lines

        "## Version $OldVersion
        " > release/RELEASE.md_old

        gc release/RELEASE.md >> release/RELEASE.md_old

        # Write old lines

        gc release/RELEASE.md_old_backupsv >> release/RELEASE.md_old

        Remove-Item release/RELEASE.md_backupsv -ErrorAction Ignore > $null
        Move-Item release/RELEASE.md release/RELEASE.md_backupsv

        Remove-Item -Force release/RELEASE.md -ErrorAction Ignore > $null

        Write-Host "Open release file"

        vim release/RELEASE.md

        if (!(Test-Path "release/RELEASE.md")) {
            Write-Error "no release file created, abort"
            Remove-Item release/RELEASE.md -ErrorAction Ignore > $null
            Remove-Item release/RELEASE.md_old -ErrorAction Ignore > $null
            Move-Item release/RELEASE.md_backupsv release/RELEASE.md
            Move-Item release/RELEASE.md_old_backupsv release/RELEASE.md_old
            mvn versions:set versions:commit -DnewVersion="$OldVersion" -q
            exit -1
        }

        Write-Host "Remove backup files"

        Remove-Item -Force release/RELEASE.md_backupsv -ErrorAction Ignore > $null
        Remove-Item -Force release/RELEASE.md_old_backupsv -ErrorAction Ignore > $null
    }
    Remove-Item -Force pom.xml_backupsv -ErrorAction Ignore > $null
} finally {
  $prevPwd | Set-Location
}