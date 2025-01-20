param(
    [Parameter()]
    [Switch]
    $unicode,
    [Parameter()]
    [Switch]
    $progress,
    [Parameter()]
    [Switch]
    $color,
    [Parameter()]
    [Switch]
    $binary,
    [Parameter()]
    [Switch]
    $quiet,
    [Parameter()]
    [Switch]
    $load,
    [Parameter()]
    [Switch]
    $shared,
    [Parameter()]
    [Switch]
    $equals,
    [Parameter(ValueFromRemainingArguments, Position = 0)]
    [string[]]
    $OtherParams
)

& "$(Get-Item $PSScriptRoot)/javaenv.ps1" "com.the_qa_company.qendpoint.core.tools.HDTVerify" -RequiredParameters $PSBoundParameters
