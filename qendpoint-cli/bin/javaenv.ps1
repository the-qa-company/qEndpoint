param(
    # Java start class
    [Parameter(Mandatory = $true)]
    [string]
    $JavaStartClass,
    $RequiredParameters
)

$javaenv = @{
    "JAVAOPTIONS"  = "-XX:NewRatio=1", "-XX:SurvivorRatio=9", "-Xmx1G"
    "JAVACMD"      = "java"
    "RDFHDT_COLOR" = "false"
}


# set the env variable for the color
$env:RDFHDT_COLOR = $javaenv.RDFHDT_COLOR

# Find libraries
$libDir = [System.IO.Path]::Combine(((Get-Item $PSScriptRoot).Parent.FullName), "lib")
$libs = ((Get-ChildItem $libDir).FullName) -join ([System.IO.Path]::PathSeparator)

return & $javaenv.JAVACMD @($javaenv.JAVAOPTIONS) "-classpath" $libs $JavaStartClass @(
    $RequiredParameters.Keys | ForEach-Object {
        $it = $RequiredParameters.Item($_)
        if ("$_" -eq "OtherParams") {
            $it
        }
        else {
            "-$_"
            if ($it.GetType() -ne [System.Management.Automation.SwitchParameter]) { 
                $it
            }
        }
    }
)
