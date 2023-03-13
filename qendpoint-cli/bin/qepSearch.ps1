param(
    [String]
    $options,
    [String]
    $config,
    [String]
    $searchCfg,
    [Switch]
    $color,
    [Parameter()]
    [Switch]
    $version,
    [ArgumentCompleter({
        return @("guess", "delta", "hdt", "qendpoint") | ForEach-Object { $_ }
    })]
    $rdftype,
    [Parameter()]
    [Switch]
    $memory,
    [Parameter(ValueFromRemainingArguments, Position = 0)]
    [string[]]
    $OtherParams
)

& "$(Get-Item $PSScriptRoot)/javaenv.ps1" "com.the_qa_company.qendpoint.tools.QEPSearch" -RequiredParameters $PSBoundParameters
