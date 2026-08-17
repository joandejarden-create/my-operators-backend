# Append a pre-fetched website extract (from WebFetch or manual capture) to the library + source_log
param(
    [Parameter(Mandatory)] [string]$Company,
    [Parameter(Mandatory)] [string]$ParentCompany,
    [Parameter(Mandatory)] [string]$Slug,
    [Parameter(Mandatory)] [string]$Title,
    [Parameter(Mandatory)] [string]$Url,
    [Parameter(Mandatory)] [string]$Region,
    [Parameter(Mandatory)] [string]$RegionRelevance,
    [Parameter(Mandatory)] [string]$SourceType,
    [Parameter(Mandatory)] [string]$BodyFile
)

$AccessedDate = "2026-07-05"
$Base = (Get-Item "G:\My Drive\Dealality*\Platform Design & Build\Brand Reference Material").FullName
$csvPath = Join-Path $Base "00_source_log\source_log.csv"

function Get-SafeFolderName([string]$Name) {
    return ($Name -replace '[\\/:*?"<>|&]', ' - ' -replace '\s+', ' ').Trim(' ', '-')
}

function Get-RegionFolder([string]$RegionTag, [string]$CompanyName, [string]$DefaultRelevance) {
    $safeCompany = Get-SafeFolderName $CompanyName
    switch -Regex ($RegionTag) {
        'CALA|Mexico|Caribbean|Brazil|Dominican|Costa Rica|LatAm' { return "01_priority_regions\CALA_LatAm_Caribbean_Mexico\$safeCompany" }
        'Spain|Portugal' { return "01_priority_regions\Spain_Portugal\$safeCompany" }
        'Americas' { return "01_priority_regions\Americas\$safeCompany" }
        default {
            if ($DefaultRelevance -eq 'High') { return "01_priority_regions\CALA_LatAm_Caribbean_Mexico\$safeCompany" }
            return "02_global_reference\Brand_Parent_Portfolios\$safeCompany"
        }
    }
}

$body = Get-Content $BodyFile -Raw -Encoding UTF8
$safeTitle = ($Title -replace '[^a-zA-Z0-9]+', '_').Trim('_')
if ($safeTitle.Length -gt 60) { $safeTitle = $safeTitle.Substring(0, 60) }
$regionSlug = ($Region -replace '[^a-zA-Z0-9]+', '_')
$fileName = "${AccessedDate}__${Slug}__${safeTitle}__Official_Development_Web__${regionSlug}.md"
$relFolder = Get-RegionFolder -RegionTag $Region -CompanyName $Company -DefaultRelevance $RegionRelevance
$destDir = Join-Path $Base $relFolder
[System.IO.Directory]::CreateDirectory($destDir) | Out-Null
$destPath = Join-Path $destDir $fileName
$relPath = "$relFolder\$fileName"

$md = @"
## Source metadata

- company: $Company
- parent_company: $ParentCompany
- source_title: $Title
- source_url: $Url
- accessed_date: $AccessedDate
- region_covered: $Region
- source_type: $SourceType
- official_source: true
- extraction_method: manual_webfetch_capture

# $Title

**Official source:** $Url
**Accessed:** $AccessedDate

> Text captured from official page via WebFetch. Confirm against live URL before Airtable writes.

## Extracted content (official page text)

$body

## Explorer mapping hints

| Explorer theme | Look in extract for |
|----------------|---------------------|
| brand_positioning | Brand names, segment labels, guest promise |
| affiliation_model | franchise, management, owner |
| pipeline / footprint | hotels, rooms, countries, regions |
| fees / economics | investment, fees, returns |

## Fields not found on this page

Use PDFs or other sources if not stated above.
"@

Set-Content -LiteralPath $destPath -Value $md -Encoding UTF8

$rows = @(Import-Csv $csvPath)
$key = "$Url|$relPath"
if (-not ($rows | Where-Object { "$($_.Source_URL)|$($_.Saved_File_Path)" -eq $key })) {
    $rows += [pscustomobject]@{
        Company = $Company
        Brand_or_Operator = "$Company (parent)"
        Parent_Company = $ParentCompany
        Source_Title = "$Title - website extract"
        Source_URL = $Url
        Saved_File_Path = $relPath
        PDF_Year = 'n/a'
        Accessed_Date = $AccessedDate
        Region_Relevance = $RegionRelevance
        Region_Covered = $Region
        Source_Type = $SourceType
        Official_Source = 'Yes'
        Explorer_Usefulness = 'High'
        Freshness_Score = 'Unknown'
        Region_Score = $(if ($RegionRelevance -eq 'High') { 'High' } else { 'Medium' })
        Overall_Relevance = 'High'
        Use_Status = 'Use First'
        Reason = 'Manual WebFetch capture of official development/brand page text.'
        Explorer_Fields_Supported = 'brand_positioning;affiliation_model;geographic_footprint;chain_scale;target_owner_profile'
        Notes = "Chars: $($body.Length)"
    }
    $rows | Export-Csv -Path $csvPath -NoTypeInformation -Encoding UTF8
}

Write-Host "Saved: $relPath"
