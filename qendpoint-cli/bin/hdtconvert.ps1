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
    $version,
    [Parameter()]
    [Switch]
    $dict,
    [Parameter()]
    [Switch]
    $quiet,
    [Parameter()]
    [Switch]
    $load,
    [Parameter(ValueFromRemainingArguments, Position = 0)]
    [string[]]
    $OtherParams
)

& "$(Get-Item $PSScriptRoot)/javaenv.ps1" "com.the_qa_company.qendpoint.core.tools.HDTConvertTool" -RequiredParameters $PSBoundParameters
