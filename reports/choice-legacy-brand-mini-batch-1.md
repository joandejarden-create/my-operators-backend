# Choice Legacy Mini-Batch 1 — Source Package

Generated: 2026-07-06T22:54:10.065Z
Batch: **mini-batch-1**
Mode: **dry_run**
Airtable modified: **no**

## Executive summary

| Metric | Count |
|--------|------:|
| Brands in batch | 3 |
| Ready for PDF registration | 0 |
| Already registered (PDF duplicate) | 3 |
| Needing URL capture | 3 |
| Press kit uncertain/missing | 0 |
| JS-shell / dev-page risk (medium+) | 3 |
| Split-out recommended | 0 |
| Blocked | 0 |

## Recommended order of operations

- 1. Dry-run review this report
- 2. Apply local PDF registration per brand (explicit approval flag)
- 3. Capture consumer + press kit URLs with --apply --register (one brand at a time)
- 4. Optional: capture development URL as provenance only (not primary extract)
- 5. npm run steward-partner-intelligence -- --entity-type brand --target-rec-id … --dry-run
- 6. Do NOT extract or publish governance until sources stewarded

## Brands

### Comfort Inn & Suites

- Record ID: `recOzH5iAE1xEjyD0` (confirmed)
- Profile status: **Platform Ready**
- Governance: **Company Published**
- Explorer active: **yes**
- Profile completeness: **Strong Existing Profile**
- PI sources: **3/3** approved
- Local PDF: `Choice Hotels International/Comfort Inn/brochure--comfort-inn.pdf`
- PDF readable: **yes** · text length **1858**
- Proposed title: **Comfort Inn & Suites — Choice development brochure (local)**
- PDF registration: **skip_already_registered**
- Duplicate: `recZFPfGRo5C9FF2Q` (local_file_path)
- Consumer URL: https://www.choicehotels.com/comfort-hotels (verified)
- Press kit URL: https://media.choicehotels.com/comfort-press-kit (verified)
- Development URL: https://www.choicehotelsdevelopment.com/our-brands/upper-midscale/comfort
- Dev JS-shell risk: **high** · recommendation: **provenance_only_prefer_local_pdf**
- Ready for PDF registration: **no**
- Ready for URL capture: **yes**
- Split out: **no**
- Fixture extractable text: **3756** chars (`fixtures/choice-dev-site-text/our-brands__upper-midscale__comfort.txt`)
- Live probe: HTTP 200; stripped text **29**; likely JS shell: **yes**

#### Recommended P0 package

- **consumer_brand_page** (Brand Page): https://www.choicehotels.com/comfort-hotels → capture_after_dry_run_review
- **development_pdf_local** (Development Brochure): Choice Hotels International/Comfort Inn/brochure--comfort-inn.pdf → skip_already_registered
- **press_kit** (Press Release): https://media.choicehotels.com/comfort-press-kit → capture_after_dry_run_review
- **development_page** (Development Page): https://www.choicehotelsdevelopment.com/our-brands/upper-midscale/comfort → provenance_only_prefer_local_pdf

- Missing: consumer page URL capture; press kit URL capture

#### Exact next commands

```bash
npm run partner-reference:download -- --url "https://www.choicehotels.com/comfort-hotels" --company "Choice Hotels International" --brand "Comfort Inn & Suites" --type website-capture --title "Choice consumer brand page" --brand-id recOzH5iAE1xEjyD0 --dry-run
npm run partner-reference:download -- --url "https://media.choicehotels.com/comfort-press-kit" --company "Choice Hotels International" --brand "Comfort Inn & Suites" --type media-kit --title "Choice press kit / media center" --brand-id recOzH5iAE1xEjyD0 --dry-run
npm run partner-reference:download -- --url "https://www.choicehotelsdevelopment.com/our-brands/upper-midscale/comfort" --company "Choice Hotels International" --brand "Comfort Inn & Suites" --type website-capture --title "Choice development brand page (provenance)" --brand-id recOzH5iAE1xEjyD0 --dry-run  # provenance only; do not rely on extract
```

### Everhome Suites

- Record ID: `recqkkrsevi4r9ibj` (confirmed)
- Profile status: **Platform Ready**
- Governance: **Company Published**
- Explorer active: **yes**
- Profile completeness: **Strong Existing Profile**
- PI sources: **3/3** approved
- Local PDF: `Choice Hotels International/Everhome Suites/Everhome Suites_Franchise Development Presentation.pdf`
- PDF readable: **yes** · text length **25577**
- Proposed title: **Everhome Suites — franchise development presentation (local)**
- PDF registration: **skip_already_registered**
- Duplicate: `rechRqlbx7DF4YCCV` (local_file_path)
- Consumer URL: https://www.choicehotels.com/everhome-suites (verified)
- Press kit URL: https://media.choicehotels.com/everhome-suites (verified)
- Development URL: https://www.choicehotelsdevelopment.com/our-brands/extended-stay/everhome-suites
- Dev JS-shell risk: **high** · recommendation: **provenance_only_prefer_local_pdf**
- Ready for PDF registration: **no**
- Ready for URL capture: **yes**
- Split out: **no**
- Fixture extractable text: **3378** chars (`fixtures/choice-dev-site-text/our-brands__extended-stay__everhome-suites.txt`)
- Live probe: HTTP 200; stripped text **29**; likely JS shell: **yes**

#### Recommended P0 package

- **consumer_brand_page** (Brand Page): https://www.choicehotels.com/everhome-suites → capture_after_dry_run_review
- **development_pdf_local** (Development Brochure): Choice Hotels International/Everhome Suites/Everhome Suites_Franchise Development Presentation.pdf → skip_already_registered
- **press_kit** (Press Release): https://media.choicehotels.com/everhome-suites → capture_after_dry_run_review
- **development_page** (Development Page): https://www.choicehotelsdevelopment.com/our-brands/extended-stay/everhome-suites → provenance_only_prefer_local_pdf

- Missing: consumer page URL capture; press kit URL capture

#### Exact next commands

```bash
npm run partner-reference:download -- --url "https://www.choicehotels.com/everhome-suites" --company "Choice Hotels International" --brand "Everhome Suites" --type website-capture --title "Choice consumer brand page" --brand-id recqkkrsevi4r9ibj --dry-run
npm run partner-reference:download -- --url "https://media.choicehotels.com/everhome-suites" --company "Choice Hotels International" --brand "Everhome Suites" --type media-kit --title "Choice press kit / media center" --brand-id recqkkrsevi4r9ibj --dry-run
npm run partner-reference:download -- --url "https://www.choicehotelsdevelopment.com/our-brands/extended-stay/everhome-suites" --company "Choice Hotels International" --brand "Everhome Suites" --type website-capture --title "Choice development brand page (provenance)" --brand-id recqkkrsevi4r9ibj --dry-run  # provenance only; do not rely on extract
```

### Quality Inn

- Record ID: `recd8o4k1JddhkRWW` (confirmed)
- Profile status: **Platform Ready**
- Governance: **Company Published**
- Explorer active: **yes**
- Profile completeness: **Strong Existing Profile**
- PI sources: **3/3** approved
- Local PDF: `Choice Hotels International/Quality Inn/brochure--quality-inn.pdf`
- PDF readable: **yes** · text length **1923**
- Proposed title: **Quality Inn — Choice development brochure (local)**
- PDF registration: **skip_already_registered**
- Duplicate: `recmEnl9wcLfSA4Mk` (local_file_path)
- Consumer URL: https://www.choicehotels.com/quality-inn (verified)
- Press kit URL: https://media.choicehotels.com/quality-press-kit (verified)
- Development URL: https://www.choicehotelsdevelopment.com/our-brands/midscale/quality-inn
- Dev JS-shell risk: **high** · recommendation: **provenance_only_prefer_local_pdf**
- Ready for PDF registration: **no**
- Ready for URL capture: **yes**
- Split out: **no**
- Fixture extractable text: **3155** chars (`fixtures/choice-dev-site-text/our-brands__midscale__quality-inn.txt`)
- Live probe: HTTP 200; stripped text **29**; likely JS shell: **yes**

#### Recommended P0 package

- **consumer_brand_page** (Brand Page): https://www.choicehotels.com/quality-inn → capture_after_dry_run_review
- **development_pdf_local** (Development Brochure): Choice Hotels International/Quality Inn/brochure--quality-inn.pdf → skip_already_registered
- **press_kit** (Press Release): https://media.choicehotels.com/quality-press-kit → capture_after_dry_run_review
- **development_page** (Development Page): https://www.choicehotelsdevelopment.com/our-brands/midscale/quality-inn → provenance_only_prefer_local_pdf

- Missing: consumer page URL capture; press kit URL capture

#### Exact next commands

```bash
npm run partner-reference:download -- --url "https://www.choicehotels.com/quality-inn" --company "Choice Hotels International" --brand "Quality Inn" --type website-capture --title "Choice consumer brand page" --brand-id recd8o4k1JddhkRWW --dry-run
npm run partner-reference:download -- --url "https://media.choicehotels.com/quality-press-kit" --company "Choice Hotels International" --brand "Quality Inn" --type media-kit --title "Choice press kit / media center" --brand-id recd8o4k1JddhkRWW --dry-run
npm run partner-reference:download -- --url "https://www.choicehotelsdevelopment.com/our-brands/midscale/quality-inn" --company "Choice Hotels International" --brand "Quality Inn" --type website-capture --title "Choice development brand page (provenance)" --brand-id recd8o4k1JddhkRWW --dry-run  # provenance only; do not rely on extract
```

## Does not do

- Rebuild Brand Explorer content
- Overwrite Brand Setup fields
- Extract facts
- Approve sources or facts
- Publish governance
- Set Company Validated
- Register uncertain URLs without capture
- Auto-approve registered sources
