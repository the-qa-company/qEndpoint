param(
    [String]
    $options,
    [String]
    $config,
    [String]
    $searchCfg,
    [String]
    $csv,
    [Switch]
    $color,
    [Parameter()]
    [Switch]
    $version,
    [ArgumentCompleter({
        return @("guess", "delta", "hdt", "qendpoint", "reader", "profiler") | ForEach-Object { $_ }
    })]
    $type,
    [switch]
    $rdf4jfixdump,
    [Parameter()]
    [Switch]
    $binindex,
    [Parameter()]
    [Switch]
    $memory,
    [Parameter(ValueFromRemainingArguments, Position = 0)]
    [string[]]
    $OtherParams
)

& "$(Get-Item $PSScriptRoot)/javaenv.ps1" "com.the_qa_company.qendpoint.tools.QEPSearch" -RequiredParameters $PSBoundParameters
