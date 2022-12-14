param(
    $Server = "http://localhost:1234",
    $URL = "https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.nt.gz"
)

$LoadURI = "$Server/api/endpoint/loadURL"
$QueryURI = "$($LoadURI)?url=$([System.Web.HttpUtility]::UrlEncode($URL))"

Write-Host "Send $QueryURI"
Invoke-RestMethod -Uri $QueryURI -Method Get