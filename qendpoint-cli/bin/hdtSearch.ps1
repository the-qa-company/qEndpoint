param(
    [String]
    $options,
    [String]
    $config,
    [Switch]
    $color,
    [Parameter()]
    [Switch]
    $version,
    [Parameter()]
    [Switch]
    $memory,
    [Parameter(ValueFromRemainingArguments, Position = 0)]
    [string[]]
    $OtherParams
)

& "$(Get-Item $PSScriptRoot)/javaenv.ps1" "com.the_qa_company.qendpoint.core.tools.HdtSearch" -RequiredParameters $PSBoundParameters
