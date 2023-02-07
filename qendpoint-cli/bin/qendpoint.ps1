param(
    [Parameter()]
    [Switch]
    $client,
    [Parameter(ValueFromRemainingArguments, Position = 0)]
    [string[]]
    $OtherParams
)

& "$(Get-Item $PSScriptRoot)/javaenv.ps1" -UseDoubleMinus "com.the_qa_company.qendpoint.Application" -RequiredParameters $PSBoundParameters