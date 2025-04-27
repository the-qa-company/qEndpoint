param(
    [String]
    $options,
    [String]
    $config,
    [string]
    $base,
    [Switch]
    $color,
    [Switch]
    $multithread,
    [Parameter(ValueFromRemainingArguments, Position = 0)]
    [string[]]
    $OtherParams
)

& "$(Get-Item $PSScriptRoot)/javaenv.ps1" "com.the_qa_company.qendpoint.core.tools.RDF2HDTMult" -RequiredParameters $PSBoundParameters
