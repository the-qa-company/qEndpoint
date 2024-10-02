param(
    [Parameter()]
    [String]
    $options,
    [Parameter()]
    [String]
    $config,
    [Parameter()]
    [Switch]
    $color,
    [Parameter()]
    [Switch]
    $quiet,
    [string]
    $base,
    [Parameter(ValueFromRemainingArguments, Position = 0)]
    [string[]]
    $OtherParams
)

& "$(Get-Item $PSScriptRoot)/javaenv.ps1" "com.the_qa_company.qendpoint.core.tools.HDTAutoIndexer" -RequiredParameters $PSBoundParameters
