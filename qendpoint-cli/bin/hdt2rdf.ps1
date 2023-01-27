param(
    [Parameter()]
    [Switch]
    $version,
    [Parameter(ValueFromRemainingArguments, Position = 0)]
    [string[]]
    $OtherParams
)

& "$(Get-Item $PSScriptRoot)/javaenv.ps1" "com.the_qa_company.qendpoint.core.tools.HDT2RDF" -RequiredParameters $PSBoundParameters
