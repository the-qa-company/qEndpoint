param(
    [String]
    $options,
    [String]
    $config,
    [ArgumentCompleter({
        return @("ntriples", "nt", "n3", "nq", "nquad", "rdfxml", "rdf-xml", "owl", "turtle", "rar", "tar", "tgz", "tbz", "tbz2", "zip", "list", "hdt") | ForEach-Object { $_ }
    })]
    $rdftype,
    [Switch]
    $version,
    [string]
    $base,
    [Switch]
    $index,
    [Switch]
    $quiet,
    [Switch]
    $color,
    [Switch]
    $canonicalntfile,
    [Switch]
    $multithread,
    [Switch]
    $printoptions,
    [Parameter(ValueFromRemainingArguments, Position = 0)]
    [string[]]
    $OtherParams
)

& "$(Get-Item $PSScriptRoot)/javaenv.ps1" "com.the_qa_company.qendpoint.tools.RioRDF2HDT" -RequiredParameters $PSBoundParameters
