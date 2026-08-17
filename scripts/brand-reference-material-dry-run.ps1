# Brand Reference Material - dry run (first 5 companies)
# Saves to Google Drive Brand Reference Material library

$ErrorActionPreference = "Stop"
$AccessedDate = "2026-07-05"
$Base = (Get-Item "G:\My Drive\Dealality*\Platform Design & Build\Brand Reference Material").FullName
$LogDir = Join-Path $Base "00_source_log"
$ua = "Dealality-Reference-Library/1.0 (research; contact via dealality.com)"

function Save-Pdf {
    param(
        [string]$Url,
        [string]$DestPath
    )
    $dir = Split-Path $DestPath -Parent
    if (-not (Test-Path $dir)) { New-Item -ItemType Directory -Force -Path $dir | Out-Null }
    Invoke-WebRequest -Uri $Url -OutFile $DestPath -UseBasicParsing -TimeoutSec 120 -Headers @{ "User-Agent" = $ua }
    if (-not (Test-Path $DestPath)) { throw "Download failed: $Url" }
    $bytes = (Get-Item $DestPath).Length
    if ($bytes -lt 5000) { throw "Download too small ($bytes bytes): $Url" }
    return $bytes
}

$downloads = @(
    @{
        Company = "Marriott International"
        Brand_or_Operator = "Marriott International (parent)"
        Parent_Company = "Marriott International"
        Source_Title = "2024 Annual Report (Form ARS / 10-K companion)"
        Source_URL = "https://www.sec.gov/Archives/edgar/data/1048286/000114036125010603/ny20039208x3_ars.pdf"
        RelFolder = "02_global_reference\Investor_Presentations\Marriott International"
        FileName = "${AccessedDate}__Marriott__2024_Annual_Report__Investor_Annual_Report__Global__2024.pdf"
        PDF_Year = "2024"
        Region_Relevance = "Medium"
        Region_Covered = "Global"
        Source_Type = "Investor Annual Report"
        Official_Source = "Yes"
        Explorer_Usefulness = "High"
        Freshness_Score = "High"
        Region_Score = "Medium"
        Overall_Relevance = "High"
        Use_Status = "Use First"
        Reason = "Official SEC-filed annual report with brand portfolio, pipeline, franchise/management model, and geographic footprint."
        Explorer_Fields_Supported = "brand_positioning;chain_scale;affiliation_model;franchise_model;management_model;pipeline;geographic_footprint;loyalty_distribution;owner_economics"
        Notes = "No single Marriott development brochure PDF found; annual report is best official parent-level source."
    },
    @{
        Company = "Marriott International"
        Brand_or_Operator = "City Express by Marriott"
        Parent_Company = "Marriott International"
        Source_Title = "2025 City Express Domestic Franchise Disclosure Document (Items 1-23)"
        Source_URL = "https://www.hotel-development.marriott.com/ResourceFiles/fdd-document/2025-city-express-fdd-3-31-2025.pdf"
        RelFolder = "01_priority_regions\CALA_LatAm_Caribbean_Mexico\Marriott International"
        FileName = "${AccessedDate}__Marriott__City_Express_FDD__Official_FDD__Americas__2025.pdf"
        PDF_Year = "2025"
        Region_Relevance = "High"
        Region_Covered = "Americas (US/Canada FDD; brand launched from Mexico/LatAm City Express acquisition)"
        Source_Type = "Official FDD"
        Official_Source = "Yes"
        Explorer_Usefulness = "High"
        Freshness_Score = "High"
        Region_Score = "High"
        Overall_Relevance = "High"
        Use_Status = "Use First"
        Reason = "Official 2025 FDD for Marriott's midscale LatAm-origin brand entering Americas; strong fees, standards, and franchise model detail."
        Explorer_Fields_Supported = "brand_positioning;chain_scale;franchise_model;development_requirements;fees_or_key_money;brand_standards;conversion_fit;target_owner_profile"
        Notes = "FDD is US/Canada scoped but documents City Express brand origin and Marriott portfolio context."
    },
    @{
        Company = "Hilton Worldwide"
        Brand_or_Operator = "Hilton Worldwide (parent)"
        Parent_Company = "Hilton Worldwide Holdings Inc."
        Source_Title = "2025 Annual Report (SEC Form 10-K)"
        Source_URL = "https://stories-editor.hilton.com/wp-content/uploads/2026/03/Hilton-2025-Annual-Report-SEC-Form-10-K.pdf"
        RelFolder = "02_global_reference\Investor_Presentations\Hilton Worldwide"
        FileName = "${AccessedDate}__Hilton__2025_Annual_Report_10-K__Investor_Annual_Report__Global__2025.pdf"
        PDF_Year = "2025"
        Region_Relevance = "Medium"
        Region_Covered = "Global"
        Source_Type = "Investor Annual Report"
        Official_Source = "Yes"
        Explorer_Usefulness = "High"
        Freshness_Score = "High"
        Region_Score = "Medium"
        Overall_Relevance = "High"
        Use_Status = "Use First"
        Reason = "Official Hilton IR-hosted 10-K with brand portfolio tables, pipeline, franchise terms, and segment detail."
        Explorer_Fields_Supported = "brand_positioning;chain_scale;affiliation_model;franchise_model;management_model;pipeline;geographic_footprint;owner_economics;loyalty_distribution"
        Notes = "Hosted on official Hilton stories-editor domain."
    },
    @{
        Company = "Hilton Worldwide"
        Brand_or_Operator = "Hilton Worldwide (parent)"
        Parent_Company = "Hilton Worldwide Holdings Inc."
        Source_Title = "Q4 2025 Fact Sheet - At a Glance (brand portfolio counts)"
        Source_URL = "https://stories.hilton.com/uploads/2026/02/HLT.Q4-2025-FactSheet-FEB2026-L03.FINAL_.pdf"
        RelFolder = "02_global_reference\Brand_Parent_Portfolios\Hilton Worldwide"
        FileName = "${AccessedDate}__Hilton__Q4_2025_Fact_Sheet_Brand_Portfolio__Official_Brand_Portfolio__Global__2025.pdf"
        PDF_Year = "2025"
        Region_Relevance = "Medium"
        Region_Covered = "Global"
        Source_Type = "Official Brand Portfolio"
        Official_Source = "Yes"
        Explorer_Usefulness = "High"
        Freshness_Score = "High"
        Region_Score = "Medium"
        Overall_Relevance = "High"
        Use_Status = "Use First"
        Reason = "Official quarterly fact sheet with brand-tier property/country counts as of Dec 31, 2025."
        Explorer_Fields_Supported = "brand_positioning;chain_scale;geographic_footprint;pipeline;loyalty_distribution"
        Notes = "Complements 10-K; easier brand-count reference than full filing."
    },
    @{
        Company = "Hyatt Corporation"
        Brand_or_Operator = "Hyatt Hotels Corporation (parent)"
        Parent_Company = "Hyatt Hotels Corporation"
        Source_Title = "2026 Investor Day Presentation"
        Source_URL = "https://s203.q4cdn.com/249399152/files/doc_presentations/2026/2026-Hyatt-Investor-Day-Presentation_vFinal-Combined.pdf"
        RelFolder = "02_global_reference\Brand_Parent_Portfolios\Hyatt Corporation"
        FileName = "${AccessedDate}__Hyatt__2026_Investor_Day_Presentation__Investor_Presentation__Global__2026.pdf"
        PDF_Year = "2026"
        Region_Relevance = "Medium"
        Region_Covered = "Global"
        Source_Type = "Investor Presentation"
        Official_Source = "Yes"
        Explorer_Usefulness = "High"
        Freshness_Score = "High"
        Region_Score = "Medium"
        Overall_Relevance = "High"
        Use_Status = "Use First"
        Reason = "Official Hyatt IR presentation (May 2026) with five portfolio architecture, pipeline, and brand strategy."
        Explorer_Fields_Supported = "brand_positioning;chain_scale;affiliation_model;pipeline;geographic_footprint;loyalty_distribution;investment_thesis;CALA_presence"
        Notes = "No public standalone development brochure PDF found on hyatt.com/development."
    },
    @{
        Company = "Hyatt Corporation"
        Brand_or_Operator = "Hyatt Hotels Corporation (parent)"
        Parent_Company = "Hyatt Hotels Corporation"
        Source_Title = "2025 Annual Report on Form 10-K"
        Source_URL = "https://s203.q4cdn.com/249399152/files/doc_financials/2025/ar/e87cdaed-c0b9-4479-8bc7-8d95694aec52.pdf"
        RelFolder = "02_global_reference\Investor_Presentations\Hyatt Corporation"
        FileName = "${AccessedDate}__Hyatt__2025_Annual_Report_10-K__Investor_Annual_Report__Global__2025.pdf"
        PDF_Year = "2025"
        Region_Relevance = "Medium"
        Region_Covered = "Global"
        Source_Type = "Investor Annual Report"
        Official_Source = "Yes"
        Explorer_Usefulness = "High"
        Freshness_Score = "High"
        Region_Score = "Medium"
        Overall_Relevance = "High"
        Use_Status = "Use First"
        Reason = "Official Hyatt IR/SEC 10-K with brand portfolio, inclusive collection, franchise/management detail."
        Explorer_Fields_Supported = "brand_positioning;chain_scale;affiliation_model;franchise_model;management_model;pipeline;geographic_footprint;owner_economics;loyalty_distribution;CALA_presence"
        Notes = "Includes Inclusive Collection brands highly relevant to CALA."
    },
    @{
        Company = "IHG Hotels & Resorts"
        Brand_or_Operator = "IHG Hotels & Resorts (parent)"
        Parent_Company = "InterContinental Hotels Group PLC"
        Source_Title = "IHG Investor Deck - May 2026"
        Source_URL = "https://www.ihgplc.com/~/media/Files/I/Ihg-Plc/investors/ihg-investor-deck-may-2026.pdf"
        RelFolder = "01_priority_regions\Americas\IHG Hotels & Resorts"
        FileName = "${AccessedDate}__IHG__Investor_Deck_May_2026__Investor_Presentation__Global__2026.pdf"
        PDF_Year = "2026"
        Region_Relevance = "High"
        Region_Covered = "Global with Americas/EMEAA/Greater China splits"
        Source_Type = "Investor Presentation"
        Official_Source = "Yes"
        Explorer_Usefulness = "High"
        Freshness_Score = "High"
        Region_Score = "High"
        Overall_Relevance = "High"
        Use_Status = "Use First"
        Reason = "Official IHG PLC investor deck with Americas pipeline, brand growth, and system-size metrics."
        Explorer_Fields_Supported = "brand_positioning;chain_scale;pipeline;geographic_footprint;CALA_presence;loyalty_distribution;affiliation_model"
        Notes = "Substitute for Americas Full Development Brochure (not retrievable via public direct URL)."
    },
    @{
        Company = "IHG Hotels & Resorts"
        Brand_or_Operator = "IHG Hotels & Resorts (parent)"
        Parent_Company = "InterContinental Hotels Group PLC"
        Source_Title = "Full Year 2025 Results - Supplementary Information"
        Source_URL = "https://www.ihgplc.com/~/media/Files/I/Ihg-Plc/results/2026/full-year-2025/full-year-results-2025-supplementary-information.pdf"
        RelFolder = "01_priority_regions\Americas\IHG Hotels & Resorts"
        FileName = "${AccessedDate}__IHG__Full_Year_2025_Supplementary_Information__Investor_Presentation__Global__2025.pdf"
        PDF_Year = "2025"
        Region_Relevance = "High"
        Region_Covered = "Global with Americas brand-level pipeline tables"
        Source_Type = "Investor Presentation"
        Official_Source = "Yes"
        Explorer_Usefulness = "High"
        Freshness_Score = "High"
        Region_Score = "High"
        Overall_Relevance = "High"
        Use_Status = "Use First"
        Reason = "Official supplementary deck with Americas system size and pipeline by brand (Kimpton, Iberostar, Holiday Inn, etc.)."
        Explorer_Fields_Supported = "pipeline;geographic_footprint;brand_positioning;chain_scale;CALA_presence;conversion_fit"
        Notes = "Strong brand-level Americas pipeline breakdown."
    },
    @{
        Company = "Accor"
        Brand_or_Operator = "Accor SA (parent)"
        Parent_Company = "Accor SA"
        Source_Title = "Accor Brandbook (English)"
        Source_URL = "https://assets.group.accor.com/yrj0orc8tx24/5LtY0I2Mr4izMLJgFhDtZd/61e3a105a3e44cb3f7967de3cf98968f/Accor_Brandbook_in_English.pdf"
        RelFolder = "02_global_reference\Brand_Parent_Portfolios\Accor"
        FileName = "${AccessedDate}__Accor__Brandbook_English__Official_Brand_Portfolio__Global__2026.pdf"
        PDF_Year = "2026"
        Region_Relevance = "Medium"
        Region_Covered = "Global"
        Source_Type = "Official Brand Portfolio"
        Official_Source = "Yes"
        Explorer_Usefulness = "High"
        Freshness_Score = "High"
        Region_Score = "Medium"
        Overall_Relevance = "High"
        Use_Status = "Use First"
        Reason = "Official Accor Group brandbook covering 45+ brands across Luxury & Lifestyle and PM&E divisions."
        Explorer_Fields_Supported = "brand_positioning;chain_scale;geographic_footprint;affiliation_model;brand_standards"
        Notes = "Published March 2026 on group.accor.com documents page."
    },
    @{
        Company = "Accor"
        Brand_or_Operator = "Accor SA (parent)"
        Parent_Company = "Accor SA"
        Source_Title = "Hotel Portfolio - June 2025 (by brand and area)"
        Source_URL = "https://assets.group.accor.com/yrj0orc8tx24/5pU3QcTcdGyCNwE70JbygK/023ae88aac3ed8f7c662f2a113df00fd/Hotel-Portfolio-ENG-062025.pdf"
        RelFolder = "02_global_reference\Brand_Parent_Portfolios\Accor"
        FileName = "${AccessedDate}__Accor__Hotel_Portfolio_June_2025__Official_Brand_Portfolio__Global__2025.pdf"
        PDF_Year = "2025"
        Region_Relevance = "Medium"
        Region_Covered = "Global (Europe & North Africa, APAC, MEA, Americas)"
        Source_Type = "Official Brand Portfolio"
        Official_Source = "Yes"
        Explorer_Usefulness = "High"
        Freshness_Score = "High"
        Region_Score = "Medium"
        Overall_Relevance = "High"
        Use_Status = "Use First"
        Reason = "Official portfolio statistics by brand, segment, operating type, and region - strong footprint evidence."
        Explorer_Fields_Supported = "geographic_footprint;chain_scale;brand_positioning;pipeline;CALA_presence;Spain_Portugal_presence"
        Notes = "Americas = 12% of rooms; ENA includes Spain/Portugal-relevant footprint."
    }
)

$rows = @()
$results = @()

foreach ($d in $downloads) {
    $dest = Join-Path $Base ($d.RelFolder + "\" + $d.FileName)
    $savedRel = $d.RelFolder + "\" + $d.FileName
    try {
        $size = Save-Pdf -Url $d.Source_URL -DestPath $dest
        $status = "saved"
        $err = ""
    } catch {
        $status = "failed"
        $err = $_.Exception.Message
        $size = 0
    }
    $results += [pscustomobject]@{ Company = $d.Company; File = $d.FileName; Status = $status; Bytes = $size; Error = $err }
    $rows += [pscustomobject]@{
        Company = $d.Company
        Brand_or_Operator = $d.Brand_or_Operator
        Parent_Company = $d.Parent_Company
        Source_Title = $d.Source_Title
        Source_URL = $d.Source_URL
        Saved_File_Path = if ($status -eq "saved") { $savedRel } else { "" }
        PDF_Year = $d.PDF_Year
        Accessed_Date = $AccessedDate
        Region_Relevance = $d.Region_Relevance
        Region_Covered = $d.Region_Covered
        Source_Type = $d.Source_Type
        Official_Source = $d.Official_Source
        Explorer_Usefulness = $d.Explorer_Usefulness
        Freshness_Score = $d.Freshness_Score
        Region_Score = $d.Region_Score
        Overall_Relevance = $d.Overall_Relevance
        Use_Status = $d.Use_Status
        Reason = $d.Reason
        Explorer_Fields_Supported = $d.Explorer_Fields_Supported
        Notes = if ($status -eq "failed") { "$($d.Notes) DOWNLOAD FAILED: $err" } else { $d.Notes }
    }
}

$csvPath = Join-Path $LogDir "source_log.csv"
$rows | Export-Csv -Path $csvPath -NoTypeInformation -Encoding UTF8

$missingMd = @"
# Missing or No PDF Found - Dry Run (first 5 companies)

Generated: $AccessedDate

## IHG Hotels & Resorts - Americas IHG Full Development Brochure (English)

- **Company:** IHG Hotels & Resorts
- **Search terms used:** `"IHG Full Development Brochure"`, `"Americas IHG Full Development Brochure"`, `site:development.ihg.com brochure pdf`, `site:ihgplc.com Americas development PDF`
- **Best official non-PDF source found:** https://development.ihg.com/resources (brochure listed but no stable public direct PDF URL retrieved); https://development.ihg.com/regions/americas
- **Reason no PDF was saved:** Brochure appears gated/JS-loaded on IHG Development site; direct URL probing returned 404. Saved IHG PLC investor deck and FY2025 supplementary information instead.
- **Recommended next action:** Request Americas development brochure via IHG development contact form, or capture from authenticated development portal session.

## Hyatt Corporation - standalone hotel development owner brochure

- **Company:** Hyatt Corporation
- **Search terms used:** `"Hyatt hotel development brochure PDF"`, `site:hyatt.com development PDF`, `site:investors.hyatt.com presentation 2025`
- **Best official non-PDF source found:** https://www.hyatt.com/development/ ; https://newsroom.hyatt.com (brand portfolio realignment release Jan 2025)
- **Reason no PDF was saved:** No public standalone development brochure PDF identified; development content is web-based. Saved 2026 Investor Day presentation and 2025 10-K instead.
- **Recommended next action:** Monitor Hyatt IR events page for owner-focused decks; check regional development contacts for localized CALA materials.

## Marriott International - single global development portfolio brochure

- **Company:** Marriott International
- **Search terms used:** `"Marriott hotel development brand portfolio PDF"`, `site:hotel-development.marriott.com ResourceFiles portfolio`
- **Best official non-PDF source found:** https://www.hotel-development.marriott.com/brands (portfolio matrix web page)
- **Reason no PDF was saved:** Marriott publishes brand-specific FDDs and web portfolio matrix rather than one global development brochure PDF.
- **Recommended next action:** For CALA, prioritize regional FDDs and Latin America/Caribbean development contacts; consider Courtyard/Four Points FDDs if needed for Explorer fee fields.

"@

Set-Content -Path (Join-Path $LogDir "missing_or_no_pdf_found.md") -Value $missingMd -Encoding UTF8

$saved = ($results | Where-Object Status -eq "saved").Count
$failed = ($results | Where-Object Status -eq "failed").Count
$useFirstCos = ($rows | Where-Object { $_.Use_Status -eq "Use First" -and $_.Saved_File_Path } | Select-Object -ExpandProperty Company -Unique)

$summary = @"
# Brand Reference Material - Dry Run Summary

Generated: $AccessedDate  
Base path: $Base

## Scope

Dry run for first 5 priority brand parents: **Marriott, Hilton, Hyatt, IHG, Accor**.

## Results

| Metric | Count |
|--------|------:|
| Companies attempted | 5 |
| PDFs attempted | $($downloads.Count) |
| PDFs saved successfully | $saved |
| Download failures | $failed |

## Companies with Use First documents saved

$(($useFirstCos | ForEach-Object { "- $_" }) -join "`n")

## Per-file download status

$(($results | ForEach-Object {
    $size = if ($_.Bytes) { " ({0:N2} MB)" -f ($_.Bytes/1MB) } else { "" }
    "- **$($_.Company)** - ``$($_.File)`` - $($_.Status)$size"
}) -join "`n")

## Companies with only backup/archive documents

None in this dry run - all saved PDFs marked **Use First**.

## Companies with no PDF found

Partial gaps only (see ``missing_or_no_pdf_found.md``):

- IHG - Americas Full Development Brochure (English) not retrievable via public URL
- Hyatt - no standalone development brochure PDF
- Marriott - no single global development brochure PDF (FDDs + annual report used)

## Recommended next research batch

1. **Wyndham, Choice/Radisson Americas, BWH/WorldHotels** - major US-parent portfolios with CALA exposure
2. **Iberostar, Palladium, BarcelÃ³, RIU, Karisma** - resort/all-inclusive CALA operators
3. **Grupo Posadas, Princess, Atlantica, Faranda** - regional CALA owner-operators
4. **Per-company follow-ups:** IHG Iberostar Americas brochure (JS portal), Hilton Mexico FDDs (disclosure-documents page), Accor PM&E Global Development Presentation (hotel-development page download)

## Source quality notes / red flags

- **IHG development brochures** on development.ihg.com may require contact/form or session - do not scrape behind gates.
- **Hyatt investors.hyatt.com** blocked automated fetch (Cloudflare); PDFs on q4cdn.com worked and are official IR CDN.
- **Marriott/Hilton FDDs** are US/Canada legal disclosures - useful for fees/standards but not CALA-specific unless regional FDD exists.
- **SEC annual reports** are authoritative but US-centric in legal framing; pair with regional development materials when available.
- Existing legacy folders at library root (e.g., Choice Hotels International, Marriott International) were **not modified** in this dry run; new taxonomy lives under ``01_priority_regions`` etc.

## Files updated

- ``00_source_log/source_log.csv`` - $($rows.Count) rows
- ``00_source_log/missing_or_no_pdf_found.md``
- ``00_source_log/dry_run_summary.md`` (this file)

"@

Set-Content -Path (Join-Path $LogDir "dry_run_summary.md") -Value $summary -Encoding UTF8

Write-Host "Done. Saved $saved / $($downloads.Count) PDFs to $Base"
$results | Format-Table -AutoSize

