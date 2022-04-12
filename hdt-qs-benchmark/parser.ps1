param(
    $Result = "results",
    $OutputFile = "results.md",
    $Fields = @("qps", "executecount"),
    $GlobalFields = @("qmph", "totalruntime"),
    $ResultCSV = "results.csv"
)

# map to get the size of a file
$script:SizeMap = @{
    "benchmark_result_10000.xml"   = 3500000
    "benchmark_result_50000.xml"   = 17500000
    "benchmark_result_100000.xml"  = 34872000
    "benchmark_result_200000.xml"  = 69494000
    "benchmark_result_500000.xml"  = 173526000
    "benchmark_result_1000000.xml" = 346000000
    "benchmark_result_5000000.xml" = 1700000000
}
# unit for PettryNB
$script:SizeUnit = "", "K", "M", "B", "T"

# Get a pretty version of $n, ex: PrettyNB 100000 = 100K
function PrettyNB ($n) {
    $c = $n
    $i = 0
    while (($c -ge 1000) -and ($i -lt ($script:SizeUnit.Count - 1))) {
        $i++
        $c /= 1000
    }
    return "$([int]($c * 10) / 10)$($script:SizeUnit[$i])";
}


$resultCSVData = cat $ResultCSV | ConvertFrom-Csv -Header @("store", "mode", "uid", "triples", "runSize", "hdtSize", "nativeSize")

$XMLFiles = (Get-ChildItem -File -Recurse $Result) | ForEach-Object {
    $xmlfile = [XML](Get-Content ($_.FullName))
    $size = $script:SizeMap[$_.Name]
    if ($null -eq $size) {
        $size = 0
    }
    return @{
        xml  = $xmlfile.bsbm
        file = $_.FullName
        size = $size
    }
} | Sort-Object -Property { $_.size }

if ($XMLFiles.Count -eq 0) {
    Write-Error "Not enough xml files in $Result"
    exit -1
}


$QueryCount = $XMLFiles[0].xml.queries.query.Count

Write-Output "# Fields"
Write-Output ""

Write-Output "- [Global field](#global-field)"
foreach ($Field in $Fields) {
    Write-Output "- [$Field](#$Field)"
}

Write-Output "# Global field"
Write-Output ""
Write-Output "| File | Size (triples) | $($GlobalFields -join ' | ') |"
Write-Output "| ---- | ---- | $(($GlobalFields | ForEach-Object {" -- "}) -join ' | ') |"
foreach ($XMLFile in $XMLFiles) {
    Write-Output "| $($XMLFile.file) | $(PrettyNB $XMLFile.size)t | $(($GlobalFields | ForEach-Object {
        return $XMLFile.xml.querymix.$_
    }) -join ' | ') |"
}

Write-Output ""

foreach ($Field in $Fields) {
    Write-Output "# $Field"
    Write-Output ""
    Write-Output "| File | $(1..($QueryCount) -join ' | ') |"
    Write-Output "| --- | $((1..($QueryCount) | ForEach-Object {" -- "}) -join ' | ') |"
    foreach ($XMLFile in $XMLFiles) {
        Write-Output "| $($XMLFile.file) | $((0..($QueryCount - 1) | ForEach-Object {
            $data = $XMLFile.xml.queries.query[$_]
            return $data.$Field
        }) -join ' | ') |"
    }
    Write-Output ""
}