# Choice Legacy Mini-Batch 2 — Source Package

Generated: 2026-07-06T23:04:34.156Z
Batch: **mini-batch-2**
Mode: **apply**
Airtable modified: **yes**

## Executive summary

| Metric | Count |
|--------|------:|
| Brands in batch | 4 |
| Ready for PDF registration | 4 |
| Already registered (PDF duplicate) | 0 |
| Needing URL capture | 4 |
| Press kit uncertain/missing | 1 |
| JS-shell / dev-page risk (medium+) | 4 |
| Split-out recommended | 1 |
| Blocked | 0 |

## Recommended order of operations

- 1. Dry-run review this report
- 2. Apply local PDF registration per brand (explicit approval flag)
- 3. Capture consumer + press kit URLs with --apply --register (one brand at a time)
- 4. Optional: capture development URL as provenance only (not primary extract)
- 5. npm run steward-partner-intelligence -- --entity-type brand --target-rec-id … --dry-run
- 6. Do NOT extract or publish governance until sources stewarded

## Brands

### Country Inn & Suites by Choice

- Record ID: `recaayt9u7YYg8h7Y` (confirmed)
- Profile status: **Active — Evidence Package Needed**
- Governance: **—**
- Explorer active: **yes**
- Profile completeness: **Strong Existing Profile**
- PI sources: **0/0** approved
- Local PDF: `Choice Hotels International/Country Inn & Suites/Country Inn & Suites Prototype Brochure.pdf`
- PDF readable: **yes** · text length **12178**
- Proposed title: **Country Inn & Suites by Choice — prototype brochure (local)**
- PDF registration: **ready_to_register_local**
- Duplicate: none
- Consumer URL: https://www.choicehotels.com/country-inn-suites (verified)
- Press kit URL: — (uncertain)
- Development URL: https://www.choicehotelsdevelopment.com/our-brands/upper-midscale/country-inn-and-suites
- Dev JS-shell risk: **high** · recommendation: **provenance_only_prefer_local_pdf**
- Ready for PDF registration: **yes**
- Ready for URL capture: **yes**
- Split out: **yes** (press_kit_uncertain_or_missing)
- Regional caveats:
  - Americas brand owned by Choice; global RHG Country Inn materials are separate reference only
- Fixture extractable text: **3864** chars (`fixtures/choice-dev-site-text/our-brands__upper-midscale__country-inn-and-suites.txt`)
- Live probe: HTTP 200; stripped text **29**; likely JS shell: **yes**

#### Recommended P0 package

- **consumer_brand_page** (Brand Page): https://www.choicehotels.com/country-inn-suites → capture_after_dry_run_review
- **development_pdf_local** (Development Brochure): Choice Hotels International/Country Inn & Suites/Country Inn & Suites Prototype Brochure.pdf → apply_local_pdf
- **press_kit** (Press Release): undefined → capture_after_dry_run_review
- **development_page** (Development Page): https://www.choicehotelsdevelopment.com/our-brands/upper-midscale/country-inn-and-suites → provenance_only_prefer_local_pdf

- Missing: consumer page URL capture; press kit URL capture

#### Exact next commands

```bash
npm run choice-legacy-brand-source-package-batch -- --batch mini-batch-2 --apply --approve-choice-legacy-batch-source-register --brand country-inn-suites-choice
npm run partner-reference:download -- --url "https://www.choicehotels.com/country-inn-suites" --company "Choice Hotels International" --brand "Country Inn & Suites by Choice" --type website-capture --title "Choice consumer brand page" --brand-id recaayt9u7YYg8h7Y --dry-run
npm run partner-reference:download -- --url "https://www.choicehotelsdevelopment.com/our-brands/upper-midscale/country-inn-and-suites" --company "Choice Hotels International" --brand "Country Inn & Suites by Choice" --type website-capture --title "Choice development brand page (provenance)" --brand-id recaayt9u7YYg8h7Y --dry-run  # provenance only; do not rely on extract
```

### Radisson by Choice

- Record ID: `recywbx1YQSTCPqW1` (confirmed)
- Profile status: **Active — Evidence Package Needed**
- Governance: **—**
- Explorer active: **yes**
- Profile completeness: **Strong Existing Profile**
- PI sources: **0/0** approved
- Local PDF: `Choice Hotels International/Radisson/1. Brand Book - RD.pdf`
- PDF readable: **yes** · text length **12456**
- Proposed title: **Radisson by Choice — brand book (local)**
- PDF registration: **ready_to_register_local**
- Duplicate: none
- Consumer URL: https://www.choicehotels.com/radisson (verified)
- Press kit URL: https://media.choicehotels.com/Radisson-press-kit (verified)
- Development URL: https://www.choicehotelsdevelopment.com/our-brands/upscale/radisson
- Dev JS-shell risk: **high** · recommendation: **provenance_only_prefer_local_pdf**
- Ready for PDF registration: **yes**
- Ready for URL capture: **yes**
- Split out: **no**
- Regional caveats:
  - Americas Radisson owned by Choice; do not register RHG global radissonhotels.com facts on this Brand Basics row
  - Press kit includes explicit Americas vs RHG Belgium ownership split
- Fixture extractable text: **3114** chars (`fixtures/choice-dev-site-text/our-brands__upscale__radisson.txt`)
- Live probe: HTTP 200; stripped text **29**; likely JS shell: **yes**

#### Recommended P0 package

- **consumer_brand_page** (Brand Page): https://www.choicehotels.com/radisson → capture_after_dry_run_review
- **development_pdf_local** (Development Brochure): Choice Hotels International/Radisson/1. Brand Book - RD.pdf → apply_local_pdf
- **press_kit** (Press Release): https://media.choicehotels.com/Radisson-press-kit → capture_after_dry_run_review
- **development_page** (Development Page): https://www.choicehotelsdevelopment.com/our-brands/upscale/radisson → provenance_only_prefer_local_pdf

- Missing: consumer page URL capture; press kit URL capture

#### Exact next commands

```bash
npm run choice-legacy-brand-source-package-batch -- --batch mini-batch-2 --apply --approve-choice-legacy-batch-source-register --brand radisson-choice
npm run partner-reference:download -- --url "https://www.choicehotels.com/radisson" --company "Choice Hotels International" --brand "Radisson by Choice" --type website-capture --title "Choice consumer brand page" --brand-id recywbx1YQSTCPqW1 --dry-run
npm run partner-reference:download -- --url "https://media.choicehotels.com/Radisson-press-kit" --company "Choice Hotels International" --brand "Radisson by Choice" --type media-kit --title "Choice press kit / media center" --brand-id recywbx1YQSTCPqW1 --dry-run
npm run partner-reference:download -- --url "https://www.choicehotelsdevelopment.com/our-brands/upscale/radisson" --company "Choice Hotels International" --brand "Radisson by Choice" --type website-capture --title "Choice development brand page (provenance)" --brand-id recywbx1YQSTCPqW1 --dry-run  # provenance only; do not rely on extract
```

### Radisson Individuals by Choice

- Record ID: `recRyvM8OmLlDj9G7` (confirmed)
- Profile status: **Active — Evidence Package Needed**
- Governance: **—**
- Explorer active: **yes**
- Profile completeness: **Strong Existing Profile**
- PI sources: **0/0** approved
- Local PDF: `Choice Hotels International/Radisson Individuals/RADIN_PitchDeck_PPT_New_Final.pdf`
- PDF readable: **yes** · text length **10662**
- Proposed title: **Radisson Individuals by Choice — pitch deck (local)**
- PDF registration: **ready_to_register_local**
- Duplicate: none
- Consumer URL: https://www.choicehotels.com/radisson-individuals (verified)
- Press kit URL: https://media.choicehotels.com/Radisson-Individuals-press-kit (verified)
- Development URL: https://www.choicehotelsdevelopment.com/our-brands/upper-upscale/radisson-individuals
- Dev JS-shell risk: **high** · recommendation: **provenance_only_prefer_local_pdf**
- Ready for PDF registration: **yes**
- Ready for URL capture: **yes**
- Split out: **no**
- Regional caveats:
  - Americas Radisson Individuals owned by Choice; exclude RHG global portfolio facts
- Fixture extractable text: **1683** chars (`fixtures/choice-dev-site-text/our-brands__upper-upscale__radisson-individuals.txt`)
- Live probe: HTTP 200; stripped text **29**; likely JS shell: **yes**

#### Recommended P0 package

- **consumer_brand_page** (Brand Page): https://www.choicehotels.com/radisson-individuals → capture_after_dry_run_review
- **development_pdf_local** (Development Brochure): Choice Hotels International/Radisson Individuals/RADIN_PitchDeck_PPT_New_Final.pdf → apply_local_pdf
- **press_kit** (Press Release): https://media.choicehotels.com/Radisson-Individuals-press-kit → capture_after_dry_run_review
- **development_page** (Development Page): https://www.choicehotelsdevelopment.com/our-brands/upper-upscale/radisson-individuals → provenance_only_prefer_local_pdf

- Missing: consumer page URL capture; press kit URL capture

#### Exact next commands

```bash
npm run choice-legacy-brand-source-package-batch -- --batch mini-batch-2 --apply --approve-choice-legacy-batch-source-register --brand radisson-individuals-choice
npm run partner-reference:download -- --url "https://www.choicehotels.com/radisson-individuals" --company "Choice Hotels International" --brand "Radisson Individuals by Choice" --type website-capture --title "Choice consumer brand page" --brand-id recRyvM8OmLlDj9G7 --dry-run
npm run partner-reference:download -- --url "https://media.choicehotels.com/Radisson-Individuals-press-kit" --company "Choice Hotels International" --brand "Radisson Individuals by Choice" --type media-kit --title "Choice press kit / media center" --brand-id recRyvM8OmLlDj9G7 --dry-run
npm run partner-reference:download -- --url "https://www.choicehotelsdevelopment.com/our-brands/upper-upscale/radisson-individuals" --company "Choice Hotels International" --brand "Radisson Individuals by Choice" --type website-capture --title "Choice development brand page (provenance)" --brand-id recRyvM8OmLlDj9G7 --dry-run  # provenance only; do not rely on extract
```

### Radisson RED by Choice

- Record ID: `recmKqo7M7mLZgRqQ` (confirmed)
- Profile status: **Active — Evidence Package Needed**
- Governance: **—**
- Explorer active: **yes**
- Profile completeness: **Strong Existing Profile**
- PI sources: **0/0** approved
- Local PDF: `Choice Hotels International/Radisson RED/Upscale by Choice brand overview guide.pdf`
- PDF readable: **yes** · text length **10002**
- Proposed title: **Radisson RED by Choice — Upscale by Choice brand overview (local)**
- PDF registration: **ready_to_register_local**
- Duplicate: none
- Consumer URL: https://www.choicehotels.com/radisson-red (verified)
- Press kit URL: https://media.choicehotels.com/Radisson-Red-press-kit (verified)
- Development URL: https://www.choicehotelsdevelopment.com/our-brands/upscale/radisson-red
- Dev JS-shell risk: **high** · recommendation: **provenance_only_prefer_local_pdf**
- Ready for PDF registration: **yes**
- Ready for URL capture: **yes**
- Split out: **no**
- Regional caveats:
  - Americas Radisson RED owned by Choice; RHG Enjoy It brochure is separate global reference (see save-radisson-red-choice-development-pdfs.mjs)
- RHG/global exclusions:
  - `Choice Hotels International/Radisson RED/Radisson RED - Enjoy It development brochure (RHG 2022).pdf` — excluded_rhg_global_primary_candidate
- Fixture extractable text: **1003** chars (`fixtures/choice-dev-site-text/our-brands__upscale__radisson-red.txt`)
- Live probe: HTTP 200; stripped text **29**; likely JS shell: **yes**

#### Recommended P0 package

- **consumer_brand_page** (Brand Page): https://www.choicehotels.com/radisson-red → capture_after_dry_run_review
- **development_pdf_local** (Development Brochure): Choice Hotels International/Radisson RED/Upscale by Choice brand overview guide.pdf → apply_local_pdf
- **press_kit** (Press Release): https://media.choicehotels.com/Radisson-Red-press-kit → capture_after_dry_run_review
- **development_page** (Development Page): https://www.choicehotelsdevelopment.com/our-brands/upscale/radisson-red → provenance_only_prefer_local_pdf

- Missing: consumer page URL capture; press kit URL capture

#### Exact next commands

```bash
npm run choice-legacy-brand-source-package-batch -- --batch mini-batch-2 --apply --approve-choice-legacy-batch-source-register --brand radisson-red-choice
npm run partner-reference:download -- --url "https://www.choicehotels.com/radisson-red" --company "Choice Hotels International" --brand "Radisson RED by Choice" --type website-capture --title "Choice consumer brand page" --brand-id recmKqo7M7mLZgRqQ --dry-run
npm run partner-reference:download -- --url "https://media.choicehotels.com/Radisson-Red-press-kit" --company "Choice Hotels International" --brand "Radisson RED by Choice" --type media-kit --title "Choice press kit / media center" --brand-id recmKqo7M7mLZgRqQ --dry-run
npm run partner-reference:download -- --url "https://www.choicehotelsdevelopment.com/our-brands/upscale/radisson-red" --company "Choice Hotels International" --brand "Radisson RED by Choice" --type website-capture --title "Choice development brand page (provenance)" --brand-id recmKqo7M7mLZgRqQ --dry-run  # provenance only; do not rely on extract
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
