# Brand Reference Material library builder
# PDFs + structured website text extracts from official development/brand pages
param(
    [string[]]$CompanyIds = @(),
    [switch]$WebsiteOnly,
    [switch]$SkipExistingWebsite
)

$ErrorActionPreference = "Continue"
$repoRoot = Split-Path -Parent $PSScriptRoot
$manifestPath = Join-Path $PSScriptRoot "brand-reference-material-companies.json"
$manifest = Get-Content $manifestPath -Raw -Encoding UTF8 | ConvertFrom-Json
$AccessedDate = $manifest.accessed_date
$Base = (Get-Item "G:\My Drive\Dealality*\Platform Design & Build\Brand Reference Material").FullName
$LogDir = Join-Path $Base "00_source_log"
$WebExtractRoot = Join-Path $LogDir "website_text_extracts"
$csvPath = Join-Path $LogDir "source_log.csv"
$ua = "Dealality-Reference-Library/1.0 (research; contact@dealality.com)"
$headers = @{ "User-Agent" = $ua; "Accept-Language" = "en-US,en;q=0.9" }

New-Item -ItemType Directory -Force -Path $WebExtractRoot | Out-Null

function Get-SafeFolderName {
    param([string]$Name)
    return ($Name -replace '[\\/:*?"<>|&]', ' - ' -replace '\s+', ' ').Trim(' ', '-')
}

function Get-RegionFolder {
    param([string]$RegionTag, [string]$CompanyName, [string]$DefaultRelevance)
    $safeCompany = Get-SafeFolderName $CompanyName
    switch -Regex ($RegionTag) {
        'CALA|Mexico|Caribbean|Brazil|Dominican|Costa Rica|LatAm' {
            return "01_priority_regions\CALA_LatAm_Caribbean_Mexico\$safeCompany"
        }
        'Spain|Portugal' {
            return "01_priority_regions\Spain_Portugal\$safeCompany"
        }
        'Americas' {
            return "01_priority_regions\Americas\$safeCompany"
        }
        default {
            if ($DefaultRelevance -eq 'High') {
                return "01_priority_regions\CALA_LatAm_Caribbean_Mexico\$safeCompany"
            }
            return "02_global_reference\Brand_Parent_Portfolios\$safeCompany"
        }
    }
}

function Convert-HtmlToStructuredText {
    param([string]$Html, [string]$PageUrl)
    if (-not $Html) { return @{ Title = ''; Body = ''; Sections = @() } }

    $title = ''
    if ($Html -match '(?is)<title[^>]*>(.*?)</title>') {
        $title = [System.Net.WebUtility]::HtmlDecode(($Matches[1] -replace '\s+', ' ').Trim())
    }

    $work = $Html
    $work = $work -replace '(?is)<script[^>]*>.*?</script>', ' '
    $work = $work -replace '(?is)<style[^>]*>.*?</style>', ' '
    $work = $work -replace '(?is)<nav[^>]*>.*?</nav>', ' '
    $work = $work -replace '(?is)<footer[^>]*>.*?</footer>', ' '
    $work = $work -replace '(?is)<header[^>]*>.*?</header>', ' '

    $metaBits = [System.Collections.Generic.List[string]]::new()
    if ($Html -match '(?is)<meta[^>]+property=["'']og:description["''][^>]+content=["'']([^"'']+)["'']') {
        $metaBits.Add('Page summary: ' + [System.Net.WebUtility]::HtmlDecode($Matches[1].Trim()))
    }
    if ($Html -match '(?is)<meta[^>]+name=["'']description["''][^>]+content=["'']([^"'']+)["'']') {
        $metaBits.Add('Meta description: ' + [System.Net.WebUtility]::HtmlDecode($Matches[1].Trim()))
    }

    $sections = [System.Collections.Generic.List[string]]::new()
    $pattern = '(?is)<h([1-3])[^>]*>(.*?)</h\1>(.*?)(?=<h[1-3]|$)'
    $matches = [regex]::Matches($work, $pattern)
    foreach ($m in $matches) {
        $level = $m.Groups[1].Value
        $heading = [System.Net.WebUtility]::HtmlDecode(($m.Groups[2].Value -replace '<[^>]+>', ' ' -replace '\s+', ' ').Trim())
        $block = $m.Groups[3].Value
        $block = $block -replace '(?is)<li[^>]*>', "`n- "
        $block = $block -replace '<[^>]+>', ' '
        $block = [System.Net.WebUtility]::HtmlDecode($block)
        $block = ($block -split '\s+' | Where-Object { $_ }) -join ' '
        if ($heading.Length -gt 2 -and $block.Length -gt 40) {
            $prefix = ('#' * [int]$level)
            $sections.Add("$prefix $heading`n`n$block`n")
        }
    }

    if ($sections.Count -eq 0) {
        $plain = $work -replace '(?is)<br\s*/?>', "`n"
        $plain = $plain -replace '<[^>]+>', ' '
        $plain = [System.Net.WebUtility]::HtmlDecode($plain)
        $plain = ($plain -split '\s+' | Where-Object { $_ }) -join ' '
        if ($plain.Length -gt 500) {
            $sections.Add($plain.Substring(0, [Math]::Min(10000, $plain.Length)))
        }
    }

    $body = ($sections -join "`n---`n").Trim()
    if ($metaBits.Count -gt 0) {
        $body = (($metaBits -join "`n`n") + "`n`n---`n`n" + $body).Trim()
    }
    if ($body.Length -gt 14000) { $body = $body.Substring(0, 14000) + "`n`n[Truncated at 14,000 characters - see Source_URL for full page.]" }

    return @{ Title = $title; Body = $body; Sections = $sections }
}

function Save-WebsiteExtract {
    param($Company, $Page, $ParentCompany, $Slug, $RegionRelevance)

    $companyName = $Company.company
    $safeTitle = ($Page.title -replace '[^a-zA-Z0-9]+', '_').Trim('_')
    if ($safeTitle.Length -gt 60) { $safeTitle = $safeTitle.Substring(0, 60) }
    $regionSlug = ($Page.region -replace '[^a-zA-Z0-9]+', '_')
    $fileName = "${AccessedDate}__${Slug}__${safeTitle}__Official_Development_Web__${regionSlug}.md"
    $relFolder = Get-RegionFolder -RegionTag $Page.region -CompanyName $companyName -DefaultRelevance $RegionRelevance
    $destDir = Join-Path $Base $relFolder
    New-Item -ItemType Directory -Force -Path $destDir | Out-Null
    $destPath = Join-Path $destDir $fileName
    $relPath = "$relFolder\$fileName"

    if ($SkipExistingWebsite -and (Test-Path $destPath)) {
        return @{ Ok = $true; Skipped = $true; Path = $relPath; Error = '' }
    }

    try {
        $resp = Invoke-WebRequest -Uri $Page.url -UseBasicParsing -TimeoutSec 45 -Headers $headers -MaximumRedirection 5
        $parsed = Convert-HtmlToStructuredText -Html $resp.Content -PageUrl $Page.url
        $pageTitle = if ($parsed.Title) { $parsed.Title } else { $Page.title }
        if ($parsed.Body.Length -lt 80) { throw "Insufficient extractable text: $($parsed.Body.Length)" }

        $md = @"
## Source metadata

- company: $($companyName)
- parent_company: $($ParentCompany)
- source_title: $($Page.title)
- source_url: $($Page.url)
- accessed_date: $($AccessedDate)
- region_covered: $($Page.region)
- source_type: $($Page.type)
- official_source: true
- extraction_method: automated_heading_and_paragraph_extract

# $($Page.title)

**Official source:** $($Page.url)
**Accessed:** $AccessedDate
**HTML page title:** $pageTitle

> Text below is extracted from the official page for Explorer reference. Confirm against source URL before writing to Airtable.

## Extracted content (official page text)

$($parsed.Body)

## Explorer mapping hints (review against extract above)

| Explorer theme | Look in extract for |
|----------------|---------------------|
| brand_positioning | Brand names, segment labels, guest promise language |
| affiliation_model | franchise, management, affiliation, owner |
| pipeline / footprint | hotels, rooms, countries, regions, openings |
| CALA / Spain-Portugal | Latin America, Caribbean, Mexico, Spain, Portugal mentions |
| fees / economics | investment, fees, returns (often not on marketing pages) |

## Fields not found on this page

Use PDFs, FDDs, or other sources if not stated above.

"@

        Set-Content -Path $destPath -Value $md -Encoding UTF8
        return @{ Ok = $true; Skipped = $false; Path = $relPath; Error = ''; Chars = $parsed.Body.Length; Title = $pageTitle }
    } catch {
        return @{ Ok = $false; Skipped = $false; Path = ''; Error = $_.Exception.Message }
    }
}

function Save-PdfFile {
    param($Company, $Pdf, $Slug, $RegionRelevance)
    $companyName = $Company.company
    $fileName = "${AccessedDate}__${Slug}__$($Pdf.file_suffix).pdf"
    $relFolder = $Pdf.rel_folder
    if ($relFolder) {
        $parts = $relFolder -split '\\'
        $parts[-1] = Get-SafeFolderName $parts[-1]
        $relFolder = $parts -join '\'
    }
    $destPath = Join-Path $Base ($relFolder + "\" + $fileName)
    $relPath = "$relFolder\$fileName"
    $dir = Split-Path $destPath -Parent
    New-Item -ItemType Directory -Force -Path $dir | Out-Null

    $reqHeaders = $headers.Clone()
    if ($Pdf.url -match 'sec\.gov') {
        $reqHeaders['User-Agent'] = 'Dealality Reference Library research@dealality.com'
    }

    try {
        Invoke-WebRequest -Uri $Pdf.url -OutFile $destPath -UseBasicParsing -TimeoutSec 120 -Headers $reqHeaders
        $bytes = (Get-Item $destPath).Length
        if ($bytes -lt 5000) { throw "File too small ($bytes bytes)" }
        return @{ Ok = $true; Path = $relPath; Bytes = $bytes; Error = '' }
    } catch {
        if ($Pdf.optional) { return @{ Ok = $false; Optional = $true; Path = ''; Error = $_.Exception.Message } }
        return @{ Ok = $false; Optional = $false; Path = ''; Error = $_.Exception.Message }
    }
}

function New-LogRow {
    param($Fields)
    [pscustomobject]$Fields
}

$existingRows = @()
if (Test-Path $csvPath) { $existingRows = @(Import-Csv $csvPath) }
$newRows = [System.Collections.Generic.List[object]]::new()
$runLog = [System.Collections.Generic.List[object]]::new()
$missing = [System.Collections.Generic.List[string]]::new()

$companies = @($manifest.companies)
if ($CompanyIds.Count -gt 0) {
    $companies = @($companies | Where-Object { $CompanyIds -contains $_.id })
}

foreach ($co in $companies) {
    $regionRel = $co.region_relevance
    Write-Host "`n=== $($co.company) ===" -ForegroundColor Cyan

    foreach ($page in @($co.website_pages)) {
        $r = Save-WebsiteExtract -Company $co -Page $page -ParentCompany $co.parent -Slug $co.slug -RegionRelevance $regionRel
        $status = if ($r.Skipped) { 'skipped' } elseif ($r.Ok) { 'saved' } else { 'failed' }
        Write-Host "  WEB $status : $($page.title)"
        $runLog.Add([pscustomobject]@{ Company = $co.company; Kind = 'website'; Name = $page.title; Status = $status; Detail = $(if ($r.Error) { $r.Error } else { "$($r.Chars) chars" }) })

        if ($r.Ok -and -not $r.Skipped) {
            $freshness = 'Unknown'
            $newRows.Add((New-LogRow @{
                Company = $co.company
                Brand_or_Operator = "$($co.company) (parent)"
                Parent_Company = $co.parent
                Source_Title = "$($page.title) - website extract"
                Source_URL = $page.url
                Saved_File_Path = $r.Path
                PDF_Year = 'n/a'
                Accessed_Date = $AccessedDate
                Region_Relevance = $regionRel
                Region_Covered = $page.region
                Source_Type = $page.type
                Official_Source = 'Yes'
                Explorer_Usefulness = 'High'
                Freshness_Score = $freshness
                Region_Score = $(if ($regionRel -eq 'High') { 'High' } else { 'Medium' })
                Overall_Relevance = 'High'
                Use_Status = 'Use First'
                Reason = 'Structured extract from official development/brand page for Explorer field grounding.'
                Explorer_Fields_Supported = 'brand_positioning;affiliation_model;geographic_footprint;chain_scale;target_owner_profile;unknown_fields'
                Notes = "Website text extract ($($r.Chars) chars). HTML title: $($r.Title)"
            }))
        } elseif (-not $r.Ok) {
            $missing.Add("## $($co.company) - $($page.title)`n`n- URL: $($page.url)`n- Error: $($r.Error)`n- Action: verify URL or capture manually`n")
        }
    }

    if (-not $WebsiteOnly) {
        foreach ($pdf in @($co.pdfs)) {
            $pr = Save-PdfFile -Company $co -Pdf $pdf -Slug $co.slug -RegionRelevance $regionRel
            $pstatus = if ($pr.Ok) { 'saved' } else { 'failed' }
            Write-Host "  PDF $pstatus : $($pdf.title)"
            $runLog.Add([pscustomobject]@{ Company = $co.company; Kind = 'pdf'; Name = $pdf.title; Status = $pstatus; Detail = $(if ($pr.Error) { $pr.Error } else { "$([math]::Round($pr.Bytes/1MB,2)) MB" }) })

            if ($pr.Ok) {
                $newRows.Add((New-LogRow @{
                    Company = $co.company
                    Brand_or_Operator = "$($co.company) (parent)"
                    Parent_Company = $co.parent
                    Source_Title = $pdf.title
                    Source_URL = $pdf.url
                    Saved_File_Path = $pr.Path
                    PDF_Year = $pdf.pdf_year
                    Accessed_Date = $AccessedDate
                    Region_Relevance = $regionRel
                    Region_Covered = 'Global'
                    Source_Type = $pdf.source_type
                    Official_Source = 'Yes'
                    Explorer_Usefulness = 'High'
                    Freshness_Score = $(if ($pdf.pdf_year -match '202[4-6]') { 'High' } elseif ($pdf.pdf_year -match '202[1-3]') { 'Medium' } else { 'Unknown' })
                    Region_Score = $(if ($regionRel -eq 'High') { 'High' } else { 'Medium' })
                    Overall_Relevance = 'High'
                    Use_Status = $pdf.use_status
                    Reason = 'Official PDF from company investor or development site.'
                    Explorer_Fields_Supported = $pdf.fields
                    Notes = ''
                }))
            } elseif (-not $pdf.optional) {
                $missing.Add("## $($co.company) - PDF: $($pdf.title)`n`n- URL: $($pdf.url)`n- Error: $($pr.Error)`n")
            }
        }
    }
}

# Merge CSV (append new rows; dedupe by Source_URL + Saved_File_Path)
$allRows = @($existingRows) + @($newRows)
$deduped = $allRows | Group-Object { "$($_.Source_URL)|$($_.Saved_File_Path)" } | ForEach-Object { $_.Group | Select-Object -First 1 }
$deduped | Export-Csv -Path $csvPath -NoTypeInformation -Encoding UTF8

# Append missing
$missingPath = Join-Path $LogDir "missing_or_no_pdf_found.md"
$missingHeader = Get-Content $missingPath -Raw -ErrorAction SilentlyContinue
if (-not $missingHeader) { $missingHeader = "# Missing or No PDF / Website Extract`n`nGenerated: $AccessedDate`n`n" }
if ($missing.Count -gt 0) {
    $append = "`n`n# Continuation run additions ($AccessedDate)`n`n" + ($missing -join "`n")
    Add-Content -Path $missingPath -Value $append -Encoding UTF8
}

# Summary stats
$savedWeb = @($runLog | Where-Object { $_.Kind -eq 'website' -and $_.Status -eq 'saved' }).Count
$failedWeb = @($runLog | Where-Object { $_.Kind -eq 'website' -and $_.Status -eq 'failed' }).Count
$savedPdf = @($runLog | Where-Object { $_.Kind -eq 'pdf' -and $_.Status -eq 'saved' }).Count
$failedPdf = @($runLog | Where-Object { $_.Kind -eq 'pdf' -and $_.Status -eq 'failed' }).Count
$companiesAttempted = @($companies | Select-Object -ExpandProperty company -Unique).Count

$summary = @"
# Brand Reference Material - Download Summary

Generated: $AccessedDate  
Base path: $Base

## Run scope

Companies processed this run: **$companiesAttempted**  
Website extracts saved: **$savedWeb** (failed: $failedWeb)  
PDFs saved this run: **$savedPdf** (failed: $failedPdf)  
Total source_log rows: **$($deduped.Count)**

## Website text extracts

Structured markdown extracts (official page text + Explorer mapping table) are saved alongside PDFs under company folders, and indexed in ``source_log.csv`` with Source_Type ``Official Development Website`` / ``Official Brand Website`` / ``Official Operator Website``.

Mirror folder: ``00_source_log/website_text_extracts/`` (optional; primary copies live in regional taxonomy folders).

## Companies with Use First documents (all runs)

$(($deduped | Where-Object { $_.Use_Status -eq 'Use First' -and $_.Saved_File_Path } | Select-Object -ExpandProperty Company -Unique | Sort-Object | ForEach-Object { "- $_" }) -join "`n")

## Per-company this run

$(($runLog | Group-Object Company | ForEach-Object {
    $c = $_.Name
    $items = $_.Group | ForEach-Object { "  - [$($_.Kind)] $($_.Name): $($_.Status) ($($_.Detail))" }
    "**$c**`n$($items -join "`n")"
}) -join "`n`n")

## Recommended next batch

- Add verified PDFs for Wyndham, Choice, BWH, Minor/NH, Barcelo, Posadas where investor/development PDFs exist
- IHG Americas brochure via development contact
- Regional FDDs: Hilton Mexico, Marriott LatAm where public
- Operator third-party: Aimbridge capability decks if published

## Source quality notes

- Website extracts are **automated** from HTML; always confirm facts against live page before Airtable writes.
- Do not infer fees, room counts, or pipeline numbers not present in extract text.
- Cloudflare or bot protection may block some pages - logged in missing file.

"@

Set-Content -Path (Join-Path $LogDir "download_summary.md") -Value $summary -Encoding UTF8

Write-Host "`nDone. Website: $savedWeb saved, $failedWeb failed. PDF: $savedPdf saved. CSV rows: $($deduped.Count)" -ForegroundColor Green
