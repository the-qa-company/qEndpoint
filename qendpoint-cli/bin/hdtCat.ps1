param(
    [Parameter()]
    [String]
    $options,
    [Parameter()]
    [String]
    $config,
    [Parameter()]
    [Switch]
    $kcat,
    [Parameter()]
    [Switch]
    $index,
    [Parameter()]
    [Switch]
    $version,
    [Parameter()]
    [Switch]
    $quiet,
    [Parameter()]
    [Switch]
    $color,
    [Parameter(ValueFromRemainingArguments, Position = 0)]
    [string[]]
    $OtherParams
)

& "$(Get-Item $PSScriptRoot)/javaenv.ps1" "com.the_qa_company.qendpoint.core.tools.HDTCat" -RequiredParameters $PSBoundParameters
