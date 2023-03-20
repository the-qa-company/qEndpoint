param(
    [Parameter()]
    [Switch]
    $color,
    [Parameter()]
    [Switch]
    $quiet,
    [Parameter(ValueFromRemainingArguments, Position = 0)]
    [string[]]
    $OtherParams
)

& "$(Get-Item $PSScriptRoot)/javaenv.ps1" "com.the_qa_company.qendpoint.tools.QueryFormatterTool" -RequiredParameters $PSBoundParameters
